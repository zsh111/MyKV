package file

import "os"

type (
	// 只是一个简单的文件
	LogFile struct {
		f *os.File
	}
	Options struct {
		name string
	}
)

// 判断文件是否关闭
func (lf *LogFile) Close() error {
	if err := lf.f.Close(); err != nil {
		return err
	}
	return nil
}

func (lf *LogFile) Write(bytes []byte) error {
	return nil
}

// 创建文件
func OpenLogFile(opt *Options) *LogFile {
	lf := &LogFile{}
	lf.f, _ = os.Create(opt.name)
	return lf
}
