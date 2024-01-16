package lsm

import (
	"bytes"
	"corekv/file"
	"corekv/iterator"
	"corekv/pb"
	"corekv/utils"
	"corekv/utils/codec"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
)

// lsm中单个table(操作单元)
type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32
}

func openTable(lm *levelManager, tableName string, b *builder) *table {
	sstSize := lm.opt.SSTableMaxSize
	if b != nil {
		sstSize = b.sstSize
	}
	var (
		t   *table
		err error
	)
	fid := file.GetFID(tableName)
	if b != nil {
		t, err = b.Flush(lm, tableName)
		if err != nil {
			utils.Err(err)
		}
		return nil
	} else {
		t = &table{lm: lm, fid: fid}
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize),
		})
	}
	t.IncrRef()
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	return t
}

func (t *table) Search(key []byte, maxVs *uint64) (*codec.Entry, error) {
	t.IncrRef()
	defer t.DecrRef()
	idx := t.ss.GetIndex()
	bloomFilter := utils.Filter(idx.BloomFilter) // TODO
	if t.ss.GetBloomLable() && bloomFilter != nil {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(nil, false)
	defer iter.Close()
	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}
	k, version := utils.ParseKey(iter.Item().Entry().Key)
	if bytes.Equal(k, key) && *maxVs < version {
		*maxVs = version
		return iter.Item().Entry(), nil
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) getEntry(key, block []byte, idx int) (entry *codec.Entry, err error) {
	if len(block) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	datastr := string(block)
	blocks := strings.Split(datastr, ",")
	if idx >= 0 && idx < len(blocks) {
		return codec.NewEntry(key, []byte(blocks[idx])), nil
	}
	return nil, utils.ErrKeyNotFound
}

// get block from table with idx
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx < len(t.ss.GetIndex().Offsets) {
		return nil, utils.ErrBlockOutIndex
	}
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}
	var blockIndex pb.BlockOffset
	utils.CondPanic(!t.offsets(&blockIndex, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		pointer: int(blockIndex.GetOffset()),
	}
	var err error
	if b.data, err = t.read(b.pointer, int(blockIndex.GetLen())); err != nil {
		return nil, errors.Wrapf(err, "failed to read from sstable: %d at offset: %d,len: %d", t.ss.GetFID(), b.pointer, blockIndex.GetLen())
	}
	readPos := len(b.data) - 4
	b.checksumLen = utils.BytesToU32(b.data[readPos : readPos+4])
	if int(b.checksumLen) > len(b.data) {
		return nil, utils.ErrChecksumLen
	}
	readPos -= int(b.checksumLen)
	b.checksum = b.data[readPos : readPos+int(b.checksumLen)]
	b.data = b.data[:readPos]
	if err = utils.VerifyCheckSum(b.data, b.checksum); err != nil {
		return nil, err
	}
	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4
	b.offsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	t.lm.cache.blocks.Set(key, b)
	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

// return fid+idx
func (t *table) blockCacheKey(idx int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

// get block (key|offset|len) from sstIndex
func (t *table) offsets(blockIndex *pb.BlockOffset, i int) bool {
	index := t.ss.GetIndex()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*blockIndex = *index.GetOffsets()[i]
	return true
}

func (t *table) Delete() error {
	return t.ss.Delete() //delete mmapfile（sstable）
}

// 按block迭代，block有自己的迭代器，封装
type tableIterator struct {
	entry    iterator.Item
	blockPos int // block index
	preFix   []byte
	isASC    bool
	boite    *blockIterator
	tb       *table
	err      error
}

func (t *table) NewIterator(prefix []byte, isAsc bool) iterator.Iterator {
	t.IncrRef()
	return &tableIterator{
		preFix: prefix,
		isASC:  isAsc,
		tb:     t,
		boite:  &blockIterator{},
	}
}

func (it *tableIterator) Next() {
	it.err = nil
	if it.blockPos >= len(it.tb.ss.GetIndex().GetOffsets()) {
		it.err = io.EOF
		return
	}
	if len(it.boite.data) == 0 {
		block, err := it.tb.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.boite.tableID = it.tb.fid
		it.boite.blockID = it.blockPos
		it.boite.SetBlock(block)
		it.boite.Rewind()
		it.err = it.boite.err
		return
	}
	it.boite.Next()
	if !it.boite.Valid() {
		it.blockPos++
		it.boite.data = nil
		it.Next()
		return
	}
	it.entry = it.boite.Item()
}

func (it *tableIterator) Valid() bool {
	return it.err != io.EOF
}

func (it *tableIterator) Rewind() {
	if it.isASC {
		it.seekFirst()
	} else {
		it.seekLast()
	}
}

func (it *tableIterator) seekFirst() {
	numBlocks := len(it.tb.ss.GetIndex().GetOffsets())
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.tb.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.boite.tableID = it.tb.fid
	it.boite.blockID = it.blockPos
	it.boite.SetBlock(block)
	it.boite.seekFirst()
	it.entry = it.boite.Item()
	it.err = it.boite.err
}

func (it *tableIterator) seekLast() {
	numBlocks := len(it.tb.ss.GetIndex().GetOffsets())
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.tb.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.boite.tableID = it.tb.fid
	it.boite.blockID = it.blockPos
	it.boite.SetBlock(block)
	it.boite.seekLast()
	it.entry = it.boite.Item()
	it.err = it.boite.err
}

func (it *tableIterator) Item() iterator.Item {
	return it.entry
}

func (it *tableIterator) Close() error {
	it.boite.Close()
	return it.tb.DecrRef()
}

func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	// 直接二分查找
	idx := sort.Search(len(it.tb.ss.GetIndex().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.tb.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.tb.ss.GetIndex().GetOffsets()) {
			return true
		}
		return bytes.Compare(ko.GetKey(), key) > 0
	})
	if idx-1 < 0 {
		idx++
	}
	it.blockPos = idx - 1
	block, err := it.tb.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.boite.tableID = it.tb.fid
	it.boite.blockID = it.blockPos
	it.boite.SetBlock(block)
	it.boite.Seek(key)
	it.err = it.boite.err
	it.entry = it.boite.Item()
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		for i := 0; i < len(t.ss.GetIndex().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}
