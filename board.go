package flow

import (
	"errors"
	"fmt"
	"sync"
)

type Board struct {
	name       string
	wg         sync.WaitGroup
	bricks     map[string]IBrick
	errHandler func(string, error)
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

func (b *Board) Add(bricks ...IBrick) *Board {
	for _, brick := range bricks {
		_, ok := b.bricks[brick.Name()]
		if false == ok {
			b.bricks[brick.Name()] = brick

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
	b.connectBrick(out.(IOutput), in.(IInput))
	return b
}

func (b *Board) connectBrick(out IOutput, in IInput) {
	// b.wg.Add(1)
	// target := out.Output()
	// go func() {
	// 	defer b.wg.Done()
	in.Linked(out.Output())
	// }()
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
	b.routeConnectBrick(out.(IRoute), in.(IInput), method)
	return b
}

func (b *Board) routeConnectBrick(out IRoute, in IInput, method func(*Message) bool) {
	b.wg.Add(1)
	target := out.RouteOutput(method)
	go func() {
		defer b.wg.Done()
		in.Linked(target)
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
