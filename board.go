package flow

import (
	"errors"
	"sync"
)

type Board struct {
	name          string
	wg            sync.WaitGroup
	bricks        map[string]IBrick
	errHandler    func(string, error)
	failedHanlder func(string, interface{})
}

func NewBoard(name string) *Board {
	return &Board{
		name:   name,
		bricks: make(map[string]IBrick),
	}
}

func (b *Board) SetErrHandler(errHandler func(string, error)) {
	b.errHandler = errHandler
}

func (b *Board) SetFailedHandler(failedHanlder func(string, interface{})) {
	b.failedHanlder = failedHanlder
}

func (b *Board) AddBricks(bricks ...IBrick) {
	for _, brick := range bricks {
		_, ok := b.bricks[brick.Name()]
		if false == ok {
			b.bricks[brick.Name()] = brick

			if _, ok := brick.(IError); ok {
				go b.onError(brick.(IBrick).Name(), brick.(IError).Errors())
			}
			if _, ok := brick.(IFailed); ok {
				go b.onFailed(brick.(IBrick).Name(), brick.(IFailed).Failed())
			}
		} else {
			panic(errors.New("Duplicate Brick Name:" + brick.(IBrick).Name()))
		}
	}
}

func (b *Board) Connect(out ISucceed, in IInput) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		in.Linked(out.Succeed())
	}()
}

func (b *Board) Sequence(bricks ...IBrick) {
	for i := 0; i < len(bricks); i++ {
		if i+1 < len(bricks) {
			b.Connect(bricks[i].(ISucceed), bricks[i+1].(IInput))
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

func (b *Board) onError(name string, inQueue <-chan error) {
	for err := range inQueue {
		if b.errHandler != nil {
			b.errHandler(b.name+"/"+name, err)
		}
	}
}

func (b *Board) onFailed(name string, inQueue <-chan interface{}) {
	for msg := range inQueue {
		if b.failedHanlder != nil {
			b.failedHanlder(b.name+"/"+name, msg)
		}
	}
}
