package flow

import (
	"sync"
)

type Board struct {
	name       string
	wg         sync.WaitGroup
	bricks     map[string]IBrick
	errHandler func(*ErrMessage)
}

func NewBoard(name string) *Board {
	return &Board{
		name:   name,
		bricks: make(map[string]IBrick),
	}
}

func (b *Board) SetErrHandler(errHandler func(*ErrMessage)) {
	b.errHandler = errHandler
}

func (b *Board) AddBricks(bricks ...IBrick) {
	for _, brick := range bricks {
		_, ok := b.bricks[brick.Name()]
		if false == ok {
			b.bricks[brick.Name()] = brick

			if _, ok := brick.(IError); ok {
				go b.onError(brick.(IError).Errors())
			}
		}
	}
}

func (b *Board) Connect(out IOutput, in IInput) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		in.Linked(out.Output())
	}()
}

func (b *Board) Sequence(bricks ...IBrick) {
	for i := 0; i < len(bricks); i++ {
		if i+1 < len(bricks) {
			b.Connect(bricks[i].(IOutput), bricks[i+1].(IInput))
		}
	}
}

func (b *Board) Start() {
	for _, brick := range b.bricks {
		ob, ok := brick.(IEntry)
		if ok {
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				ob.Start()
			}()
		}
	}
}

func (b *Board) Stop() {
	for _, b := range b.bricks {
		ob, ok := b.(IEntry)
		if ok {
			ob.Stop()
		}
	}
	b.wg.Wait()
}

func (b *Board) onError(inQueue <-chan *ErrMessage) {
	for msg := range inQueue {
		if b.errHandler != nil {
			b.errHandler(msg)
		}
	}
}
