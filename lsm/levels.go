package lsm

import (
	"bytes"
	"corekv/file"
	"corekv/iterator"
	"corekv/utils"
	"corekv/utils/codec"
	"sort"
	"sync"
	"sync/atomic"
)

type (
	// 包含配置，cache，manifest
	levelManager struct {
		maxFID   uint64 //已分配的最大fid
		opt      *Options
		cache    *cache
		manifest *file.ManifestFile
		levels   []*levelHandler
		lsm      *LSM
	}

	// 处理一个level
	levelHandler struct {
		sync.RWMutex
		levelNum       int // 文件层级
		tables         []*table
		totalSize      int64
		totalStaleSize int64 // 过期kv的数据大小
	}
)

/*-------------------------------------------------------*/

func (lm *levelManager) close() error {
	// 逐个关闭
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifest.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSM) NewLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm}
	lm.opt = opt
	lm.loadManifest()
	lm.build()
	return lm
}

func (lm *levelManager) loadCache() {}

func (lm *levelManager) loadManifest() (err error) {
	lm.manifest, err = file.OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir})
	return err
}

func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
		})
	}
	manifest := lm.manifest.GetManifest()

	if err := lm.manifest.RevertToManifest(file.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}

	lm.cache = newCache(lm.opt)
	var maxFID uint64
	for Fid, tableinfo := range manifest.Tables {
		fileName := file.CreateSSTFilePath(lm.opt.WorkDir, Fid)
		if Fid > maxFID {
			maxFID = Fid
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableinfo.Level].Add(t)
	}
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

func (lm *levelManager) flush(immutable *memTable) error {

	bl := NewTableBuilder(lm.opt)
	iter := utils.NewSkipListIterator(immutable.sl)
	iter.Rewind()
	iter.Next()
	for ; iter.Valid(); iter.Next() {
		bl.Add(iter.Item().Entry(), false)
	}
	ssTableName := file.CreateSSTFilePath(lm.opt.WorkDir, 1)
	table := openTable(lm, ssTableName, bl) // flush builder

	lm.levels[0].Add(table)
	return nil
}

func (lm *levelManager) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	for level := 1; level < utils.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err := ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, nil
}

/*------------------------------------------------------*/

func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].ss.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (lh *levelHandler) Add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) addBatch(ts []*table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, ts...)
}

func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	lh.RUnlock()
	return lh.totalSize
}

func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.ss.Size()
	lh.totalStaleSize += t.ss.Size() // TODO
}

func (lh *levelHandler) subtractSize(t *table) {
	lh.totalSize -= t.ss.Size()
}

func (lh *levelHandler) Get(key []byte) (*codec.Entry, error) {
	if lh.levelNum == 0 {
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		sort.Slice(lh.tables, func(i, j int) bool {
			return bytes.Compare(lh.tables[i].ss.GetMinKey(), lh.tables[j].ss.GetMinKey()) < 0
		})
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*codec.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Search(key, &version); err != nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) searchLNSST(key []byte) (*codec.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if entry, err := table.Search(key, &version); err != nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) getTable(key []byte) *table {
	tableSize := len(lh.tables)
	label := bytes.Compare(key, lh.tables[0].ss.GetMinKey()) < 0 || bytes.Compare(key, lh.tables[tableSize-1].ss.GetMaxKey()) > 0
	if tableSize > 0 && label {
		return nil
	} else {
		// 二分sst查找
		for i := tableSize - 1; i >= 0; i-- {
			if bytes.Compare(key, lh.tables[i].ss.GetMinKey()) > -1 && bytes.Compare(key, lh.tables[i].ss.GetMaxKey()) < 1 {
				return lh.tables[i]
			}
		}
	}
	return nil
}

type levelHandlerRLocked struct{}

// get tables.id range from left to right
func (lh *levelHandler) overlappingTables(_ levelHandlerRLocked,kr keyRange)(int,int){
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0,0
	}
	left := sort.Search(len(lh.tables),func (i int)bool {
		return bytes.Compare(kr.left,lh.tables[i].ss.GetMaxKey()) <= 0
	})
	right := sort.Search(len(lh.tables),func (i int)bool  {
		return bytes.Compare(kr.right,lh.tables[i].ss.GetMaxKey()) < 0
	})
	return left,right
}

func (lh *levelHandler)replaceTables(toDel,toAdd []*table)error{
	lh.Lock()
	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}
	var newTables []*table
	for _, t := range lh.tables {
		_,found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}
	for _, t := range toAdd {
		lh.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}

	lh.tables = newTables
	sort.Slice(lh.tables,func (i,j int)bool  {
		return bytes.Compare(lh.tables[i].ss.GetMinKey(),lh.tables[j].ss.GetMinKey()) < 0
	})
	lh.Unlock()
	return nil // TODO
}

func (lh *levelHandler)deleteTables(toDel []*table)error{
	lh.Lock()
	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}
	var newTables []*table
	for _, t := range lh.tables {
		_,found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}
	lh.tables = newTables
	lh.Unlock()
	return nil // TODO
}

func (lh *levelHandler)iterators()[]iterator.Iterator{
	lh.RLock()
	defer lh.RUnlock()
	if lh.levelNum == 0 {
		return iteratorsReversed(lh.tables,[]byte{},true)
	}
	if len(lh.tables) == 0 {
		return nil
	}
	return []iterator.Iterator{NewConcatIterator(lh.tables,[]byte{},true)}
}



