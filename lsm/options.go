package lsm

// 这是一个配置sst文件参数的struct

type Options struct {
	WorkDir            string
	SSTableMaxSize     int64
	MemTableSize       int64
	BlockSize          int
	BloomFalsePositive float64

	numCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定level之间期望的size大小
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int
	DiscardStatsCh      *chan map[uint32]int64
}
