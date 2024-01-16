package lsm

import (
	"bytes"
	"corekv/file"
	"corekv/utils"
	"corekv/utils/codec"
	"fmt"

	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
)

// sst的封装，lsm的操作单元，底层为skiplist，持有的skiplist用于flush到sst中
type memTable struct {
	wal        *file.WalFile
	sl         *utils.SkipList
	lsm        *LSM
	buf        *bytes.Buffer
	maxVersion uint64
}

func (lm *LSM) NewMemTable() *memTable {
	newFid := atomic.AddUint64(&(lm.levels.maxFID), 1)
	fileopt := &file.Options{
		Dir:      lm.option.WorkDir,
		FileName: file.CreateWALPath(lm.option.WorkDir, newFid),
		FID:      newFid,
		MaxSz:    int(lm.option.MemTableSize),
		Flag:     os.O_CREATE | os.O_RDWR,
	}
	return &memTable{wal: file.OpenWalFile(fileopt), sl: utils.NewSkipList(), lsm: lm}
}

// TODO
func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		FID:      fid,
		MaxSz:    int(lsm.option.MemTableSize),
		Flag:     os.O_CREATE | os.O_RDWR,
		FileName: file.CreateWALPath(lsm.option.WorkDir, fid),
	}
	sl := utils.NewSkipList()
	mt := &memTable{
		sl:  sl,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = file.OpenWalFile(fileOpt)
	err := mt.UpdateSkipList() // 从wal文件中还原memtable
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}

func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	endOff, err := m.wal.Iterate(true, 0, m.callBackFunction(m.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}
	return m.wal.Truncate(int64(endOff))
}

func (m *memTable) callBackFunction(opt *Options) func(*codec.Entry, *utils.ValuePtr) error {
	return func(e *codec.Entry, _ *utils.ValuePtr) error {
		if _, ts := utils.ParseKey(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.sl.Add(e)
		return nil
	}
}

func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	if err := m.sl.Close(); err != nil {
		return err
	}
	return nil
}

// 将entry写入wal和skiplist
func (m *memTable) Set(entry *codec.Entry) error {
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	if err := m.sl.Add(entry); err != nil {
		return err
	}
	return nil
}

// memtable直接在跳表中查询
func (m *memTable) Get(Key []byte) (*codec.Entry, error) {
	return m.sl.GetEntry(Key), nil
}

// 针对
func (lsm *LSM) recovery(opt *Options) (*memTable, []*memTable) {
	dir, err := os.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}
	var fids []uint64
	maxFid := lsm.levels.maxFID
	// 读取后缀为wal的文件进行还原
	for _, file := range dir {
		if !strings.HasSuffix(file.Name(), utils.WalSuffix) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-len(utils.WalSuffix)], 10, 64)
		if maxFid < fid {
			maxFid = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	imms := []*memTable{}
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		imms = append(imms, mt)
	}
	lsm.levels.maxFID = maxFid
	return lsm.NewMemTable(), imms
}
