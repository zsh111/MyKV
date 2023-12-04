package file

type Manifest struct {
	f *LogFile
}

func (mf *Manifest) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

func OpenManifest(opt *Options) *Manifest {
	return &Manifest{f: OpenLogFile(opt)}
}
