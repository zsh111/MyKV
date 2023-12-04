package lsm

import (
	"corekv/utils"
	"corekv/utils/codec"
)

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
}

func (lsm *LSM) Close() error {
	if err := lsm.memTable.close(); err != nil {
		return err
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}

	if err := lsm.levels.close(); err != nil {
		return err
	}
	// 等待合并
	lsm.closer.Close()
	return nil
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	// 启动db时加载wal，没有恢复就创建新的内存表
	lsm.memTable, lsm.immutables = recovery(opt)

	// 初始化levelmanager
	lsm.levels = newLevelManager(opt)

	// 初始化closer用于资源回收的信号控制
	lsm.closer = utils.NewCloser(1)

	return lsm
}

func (lsm *LSM) StartMerge() {
	defer lsm.closer.Done()
	for {
		select {
		case <-lsm.closer.Wait():
		}
		// 处理并发的合并过程
	}
}

func (lsm *LSM) Set(entry *codec.Entry) error {
	// 检测当前memtable是否写满，如果已满：创建新的memtable并将内存表写入immutable
	// 否则直接写入
	if err := lsm.memTable.set(entry); err != nil {
		return err
	}

	// 检查当前immutable是否需要持久化
	for _, immutable := range lsm.immutables {
		if err := lsm.levels.flush(immutable); err != nil {
			return nil
		}
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	// 优先查找mmtable，后查找immtable
	if entry, err = lsm.memTable.Get(key); entry != nil {
		return entry, err
	}
	for _, imm := range lsm.immutables {
		if entry, err = imm.Get(key); entry != nil {
			return entry, err
		}
	}
	// 从已经固化的文件中查找(多层的table中查找)
	return lsm.levels.Get(key)
}
