package relay

import "time"

type idGen struct {
	baseTime uint64
	counter  uint32
}

func newIDGen() *idGen {
	return &idGen{
		baseTime: uint64(time.Now().UnixMilli()) << 32,
	}
}

func (g *idGen) Next() uint64 {
	g.counter++
	return g.baseTime | uint64(g.counter)
}
