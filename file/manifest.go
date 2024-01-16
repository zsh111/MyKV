package file

import (
	"bufio"
	"bytes"
	"corekv/pb"
	"corekv/utils"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	errs "github.com/pkg/errors"
)

// 用于保存lsm的层级关系，实现快速恢复,manifest应用于对sst的相关操作，用于记录
// sst相关操作，并且用于快速还原，类似于AOF重写机制，对象是sst

// Manifest 结构：
// fid,level,option(add/del),checksum|

type (
	Manifest struct {
		Levels    []LevelManifest
		Tables    map[uint64]TableManifest // fid-level，全fid
		Creations int                      // 创建次数
		Deletions int                      // 删除次数
	}
	ManifestFile struct {
		opt      *Options
		f        *os.File
		lock     sync.Mutex
		manifest *Manifest
	}
	// 表示当前level的映射关系
	LevelManifest struct {
		Tables map[uint64]struct{} // 每层的sstable,fid-sst
	}
	TableManifest struct {
		Level    uint8
		Checksum []byte
	}
	bufReader struct {
		reader *bufio.Reader
		count  int64
	}
)

func (br *bufReader) Read(p []byte) (n int, err error) {
	n, err = br.reader.Read(p)
	br.count += int64(n)
	return
}

func CreateManifest() *Manifest {
	levels := make([]LevelManifest, 0)
	return &Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

// 表示一层内因为compact变动的table关系
func applyChangeSet(build *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, mc := range changeSet.Changes {
		if err := applyManifestChange(build, mc); err != nil {
			return nil
		}
	}
	return nil
}

func applyManifestChange(build *Manifest, tc *pb.ManifestChange) error {
	switch tc.Op {
	case pb.ManifestChange_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.Id)
		}
		//
		build.Tables[tc.Id] = TableManifest{
			Level:    uint8(tc.Level),
			Checksum: append([]byte{}, tc.Checksum...),
		}
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, LevelManifest{make(map[uint64]struct{})})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case pb.ManifestChange_DELETE:
		tm, ok := build.Tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		// 从这一层中删除对应id
		delete(build.Levels[tm.Level].Tables, tc.Id)
		delete(build.Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

func (m *Manifest) Close() error {

	return nil
}

func (m *Manifest) asChange() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for k, tm := range m.Tables {
		changes = append(changes, newCreateChange(k, int(tm.Level), tm.Checksum))
	}
	return changes
}

// 将level对应的sst fid转换为manifestchange
func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

// 打开manifest，
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{lock: sync.Mutex{}, opt: opt}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return mf, err
		}
		m := CreateManifest()
		fp, netCreations, err := helpRewrite(opt.Dir, m)
		utils.CondPanic(netCreations == 0, errs.Wrapf(err, utils.ErrReWriteFailure.Error()))
		if err != nil {
			return mf, err
		}
		mf.f = fp
		f = fp
		mf.manifest = m
		return mf, nil
	}

	manifest, truncOffset, err := ReplayManifestFile(f)
	if err != nil {
		_ = f.Close()
		return mf, err
	}
	if err := f.Truncate(truncOffset); err != nil {
		_ = f.Close()
		return mf, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return mf, err
	}
	mf.f = f
	mf.manifest = manifest
	return mf, nil
}

func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	for id := range mf.manifest.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}
	for id := range idMap {
		if _, ok := mf.manifest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("Table file %d  not referenced in MANIFEST", id))
			fileName := CreateSSTFilePath(mf.opt.Dir, id)
			if err := os.Remove(fileName); err != nil {
				return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

// 对已存在的manifest文件重新应用所有状态变更
func ReplayManifestFile(fp *os.File) (ret *Manifest, truncOffset int64, err error) {
	r := &bufReader{reader: bufio.NewReader(fp)}
	var Buf [8]byte
	if _, err := io.ReadFull(r, Buf[:]); err != nil {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(Buf[:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	version := binary.BigEndian.Uint32(Buf[4:8])
	if version != uint32(utils.MagicVersion) {
		return &Manifest{}, 0, fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, utils.MagicVersion)
	}
	build := CreateManifest()
	var offset int64
	for {
		offset = r.count
		var lenCrcBuf [8]byte
		_, err := io.ReadFull(r, lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCrcBuf[:4])
		var buf = make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}
		var changeSet pb.ManifestChangeSet
		if err := applyChangeSet(build, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	}
	return build, offset, err

}

func (mf *ManifestFile) rewrite() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	fp, nextCreations, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.manifest.Creations = nextCreations
	mf.manifest.Deletions = 0
	mf.f = fp
	return nil
}

// 当manifest中逻辑较复杂时，触发重写，类似AOF的重写
func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	fp, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, nil
	}
	buf := make([]byte, 8)
	l := len(utils.MagicText)
	copy(buf[:l], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[l:l+4], utils.MagicVersion)
	newCreations := len(m.Tables)
	changes := m.asChange()
	set := pb.ManifestChangeSet{Changes: changes}

	changeBuf, err := set.Marshal()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Close(); err != nil {
		return nil, 0, err
	}
	// 重命名
	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := SyncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}
	return fp, newCreations, nil
}

func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

func (mf *ManifestFile) AddChanges(changesParam []*pb.ManifestChange) error {
	return mf.addChanges(changesParam)
}

func (mf *ManifestFile) addChanges(changesParam []*pb.ManifestChange) error {
	changes := pb.ManifestChangeSet{Changes: changesParam}
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}
	mf.lock.Lock()
	defer mf.lock.Unlock()
	if err := applyChangeSet(mf.manifest, &changes); err != nil {
		return err
	}
	if mf.manifest.Deletions > utils.ManifestDelThreshold &&
		mf.manifest.Deletions > utils.ManifestDelRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}
	err = mf.f.Sync()
	return err
}

func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}
