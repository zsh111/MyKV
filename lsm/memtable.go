package lsm

import (
	"corekv/file"
	"corekv/utils"
	"corekv/utils/codec"
)

// walfile+skiplist
type memTable struct {
	wal *file.WalFile
	sl  *utils.SkipList
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
func (m *memTable) set(entry *codec.Entry) error {
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
	return m.sl.Search(Key), nil
}

func recovery(opt *Options) (*memTable, []*memTable) {
	fileOpt := &file.Options{}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList()}, []*memTable{}
}
