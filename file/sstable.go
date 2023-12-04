package file

type SSTable struct {
	f *LogFile
}

func OpenSSTable(opt *Options) *SSTable {
	return &SSTable{
		f: OpenLogFile(opt),
	}
}
