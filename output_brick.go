package flow

import (
	"github.com/RyouZhang/async-go"
)

type OutputBrick struct {
	name     string
	kernal   func(<-chan interface{}, chan<- interface{})
	errQueue chan interface{}
}

func (b *OutputBrick) Name() string {
	return b.name
}

func (b *OutputBrick) Linked(inQueue <-chan interface{}) {
	b.loop(inQueue)
}

func (b *OutputBrick) Errors() <-chan interface{} {
	return b.errQueue
}

func (b *OutputBrick) loop(inQueue <-chan interface{}) {
	defer func() {
		close(b.errQueue)
	}()
Start:
	_, err := async.Lambda(func() (interface{}, error) {
		b.kernal(inQueue, b.errQueue)
		return nil, nil
	}, 0)
	if err != nil {
		goto Start
	}
}

func NewOutputBrick(
	name string,
	kernal func(<-chan interface{}, chan<- interface{})) *OutputBrick {
	return &OutputBrick{
		name:     name,
		kernal:   kernal,
		errQueue: make(chan interface{}, 16),
	}
}
