package vlog

import (
	"corekv/utils"
	"corekv/utils/codec"
)

type Vlog struct {
	closer *utils.Closer
}

func (v *Vlog) Close() error {
	return nil
}

func NewVlog(opt *Options) *Vlog {
	v := &Vlog{}
	v.closer = utils.NewCloser(1)
	return v
}

func (v *Vlog) StartGC() {
	defer v.closer.Done()
	for {
		select {
		case <-v.closer.Wait():
		}
	}
}

func (v *Vlog) Set(entry *codec.Entry) error {
	return nil
}

func (v *Vlog) Get(entry *codec.Entry) (*codec.Entry, error) {
	return nil, nil
}
