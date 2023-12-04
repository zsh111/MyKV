package lsm

import (
	"corekv/file"
)

type table struct {
	ss *file.SSTable
}

func openTable(opt *Options) *table {
	return &table{ss: file.OpenSSTable(&file.Options{})}
}
