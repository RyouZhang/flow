package flow

import (
	"fmt"
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

func (b *Board) Add(bricks ...IBrick) *Board {
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
	return b
}

func (b *Board) Connect(outName string, inName string) *Board {
	out, ok := b.bricks[outName]
	if false == ok {
		panic(errors.New(fmt.Sprintf("Invalid Brick %s", outName)))
	}	
	in, ok := b.bricks[inName]
	if false == ok {
		panic(errors.New(fmt.Sprintf("Invalid Brick %s", inName)))
	}
	b.connectBrick(out.(ISucceed), in.(IInput))
	return b
}

func (b *Board) connectBrick(out ISucceed, in IInput) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		in.Linked(out.Succeed())
	}()
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
