package utils

import "sync"

type Closer struct {
	waiting     sync.WaitGroup
	closeSignal chan struct{}
}

func NewCloser(i int) *Closer {
	closer := &Closer{waiting: sync.WaitGroup{}}
	closer.waiting.Add(i)                    //表示等待i组goroutine完成
	closer.closeSignal = make(chan struct{}) // 无缓冲信号通道
	return closer
}

func (c *Closer) Close() {
	close(c.closeSignal)
	c.waiting.Wait()
}

// 表示协程完成资源回收
func (c *Closer) Done() {
	c.waiting.Done()
}

// 表示协程返回关闭信号
func (c *Closer) Wait() chan struct{} {
	return c.closeSignal
}
