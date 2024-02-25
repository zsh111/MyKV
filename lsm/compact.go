package lsm

import "corekv/iterator"

// 用于压缩每层的sst文件
// 主要思路为：对于第0层的sst，进行区间合并，归并到L1层
// 对于非L0层的sst，每一层存在自压缩(根据过期时间压缩)
// 每次获取一串SST执行压缩计划（更好并行）

type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64
}

func iteratorsReversed(tb []*table, prefix []byte, isAsc bool) []iterator.Iterator {
	out := make([]iterator.Iterator, 0, len(tb))
	for i := len(tb) - 1; i >= 0; i-- {
		out = append(out, tb[i].NewIterator(prefix, isAsc))
	}
	return out
}
