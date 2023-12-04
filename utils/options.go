package utils

const (
	MaxLevelNum           = 7
	DefaultValueThreshold = 1024
)

// 主要用于kv分离，设定相关参数，如level层数和maxvaluesize
type Options struct {
	ValueThreshold int64
}

func NewDefaultOPtions() *Options {
	opt := &Options{}
	opt.ValueThreshold = DefaultValueThreshold
	return opt
}
