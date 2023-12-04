package utils

type Stats struct {
	closer   *Closer //用于回收资源
	EntryNum uint64
}

func (s *Stats) Close() error {
	return nil
}

func (s *Stats) StartStats() {
	defer s.closer.Done()
	for {
		select {
		case <-s.closer.Wait():
		}
	}
}

func NewStats(opt *Options) *Stats {
	s := &Stats{}
	s.closer = NewCloser(1)
	s.EntryNum = 1
	return s
}
