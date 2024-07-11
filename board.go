package flow

import (
	"errors"
	"fmt"
	"sync"
)

type Board struct {
	name       string
	bricks     map[string]IBrick
	errHandler func(string, error)
	lc         ILifeCycle
}

func NewBoard(name string) *Board {
	return &Board{
		name:   name,
		bricks: make(map[string]IBrick),
		lc:     &sync.WaitGroup{},
	}
}

func NewBoardWithLifeCycle(name string, lc ILifeCycle) *Board {
	if lc == nil {
		return nil
	}
	return &Board{
		name:   name,
		bricks: make(map[string]IBrick),
		lc:     lc,
	}
}

func (b *Board) SetErrHandler(errHandler func(string, error)) {
	b.errHandler = errHandler
}

func (b *Board) Add(bricks ...IBrick) *Board {
	for _, brick := range bricks {
		_, ok := b.bricks[brick.Name()]
		if false == ok {
			b.bricks[brick.Name()] = brick
			// add life cycle
			brick.AddLifeCycle(b.lc)

			if _, ok := brick.(IError); ok {
				go b.onError(brick.(IBrick).Name(), brick.(IError).Errors())
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
	in.(IInput).Linked(out.(IOutput).Output())
	return b
}

func (b *Board) RouteConnect(outName string, inName string, method func(*Message) bool) *Board {
	out, ok := b.bricks[outName]
	if false == ok {
		panic(errors.New(fmt.Sprintf("Invalid Brick %s", outName)))
	}
	in, ok := b.bricks[inName]
	if false == ok {
		panic(errors.New(fmt.Sprintf("Invalid Brick %s", inName)))
	}
	in.(IInput).Linked(out.(IRoute).RouteOutput(method))
	return b
}

func (b *Board) Start() {
	for _, brick := range b.bricks {
		ob, ok := brick.(IEntry)
		if ok {
			go func() {
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
	b.lc.Wait()
}

func (b *Board) onError(name string, inQueue <-chan error) {
	for err := range inQueue {
		if b.errHandler != nil {
			b.errHandler(b.name+"/"+name, err)
		}
	}
}
