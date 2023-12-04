package file

import "corekv/utils/codec"

type WalFile struct {
	f *LogFile
}

func (wf *WalFile) Close() error {
	if err := wf.f.Close(); err != nil {
		return err
	}
	return nil
}

func OpenWalFile(opt *Options) *WalFile {
	return &WalFile{f: OpenLogFile(opt)}
}

func (wf *WalFile) Write(entry *codec.Entry) error {
	walData := codec.WalCodec(entry)
	return wf.f.Write(walData)
}
