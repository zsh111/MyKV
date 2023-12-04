package corekv

import (
	"corekv/iterator"
	"corekv/lsm"
	"corekv/utils"
	"corekv/utils/codec"
	"corekv/vlog"
)

type (
	CoreAPI interface {
		Set(data *codec.Entry) error
		Get(data []byte) (*codec.Entry, error)
		Del(key []byte) error
		NewIterator(options iterator.Options) iterator.Iterator
		Info() *utils.Stats
		Close() error
	}

	DB struct {
		opt   *utils.Options
		lsm   *lsm.LSM
		vlog  *vlog.Vlog
		stats *utils.Stats
	}
)

func Open(options *utils.Options) *DB {
	db := &DB{opt: options}
	// 初始化LSM结构
	db.lsm = lsm.NewLSM(&lsm.Options{})

	db.vlog = vlog.NewVlog(&vlog.Options{})

	db.stats = utils.NewStats(options)

	// 启动sstable merge过程
	go db.lsm.StartMerge()
	// 启动vlog gc过程
	go db.vlog.StartGC()
	// 启动info统计
	go db.stats.StartStats()
	return db
}

func (db *DB) Close() error {
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.Close(); err != nil {
		return err
	}
	if err := db.stats.Close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Del(key []byte) error {
	// 写入一个值为nil的entry
	return db.Set(&codec.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}

func (db *DB) Set(data *codec.Entry) error {
	var vs *codec.ValueStruct
	if utils.ValueSize(data.Value) > db.opt.ValueThreshold {
		vs = codec.NewValueStruct(data)
		// 先写入vlog不会有事务问题，因为如果lsm写入失败，vlog会在GC阶段清理无效的key
		if err := db.vlog.Set(data); err != nil {
			return err
		}
	}
	// 写入LSM，写值指针不空就替换entry.value值
	if vs != nil {
		data.Value = codec.ValueStructCodec(vs)
	}
	return db.lsm.Set(data)
}

func (db *DB) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	// 优先从内存表中读取数据
	if entry, err = db.lsm.Get(key); err != nil {
		return entry, err
	}
	// 检查lsm拿到的value是否为value ptr，是则从vlog中拿值
	if entry != nil && codec.IsValuePtr(entry) {
		if entry, err = db.vlog.Get(entry); err != nil {
			return entry, err
		}
	}
	return nil, nil
}

func (db *DB) Info() *utils.Stats {
	return db.stats
}
