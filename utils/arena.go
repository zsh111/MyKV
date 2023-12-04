package utils



const(
	maxArenaSize = 1<<30
)

type Arena struct {
	n int64
	grow bool
	buf []byte
}


func NewArena(n int64)*Arena{
	if n < 1000 {
		n = 1000
	}else if n > maxArenaSize{
		n = maxArenaSize
	}
	return &Arena{
		n: n,
		grow: false,
		buf: make([]byte, n),
	}
}


