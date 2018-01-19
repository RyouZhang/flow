package flow

import (
	"github.com/RyouZhang/async-go"
)

type OutputBrick struct {
	name      string
	kernal    func(<-chan interface{}, chan<- interface{}, chan<- error)
	errQueue  chan error
	failQueue chan interface{}
}

func (b *OutputBrick) Name() string {
	return b.name
}

func (b *OutputBrick) Linked(inQueue <-chan interface{}) {
	b.loop(inQueue)
}

func (b *OutputBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *OutputBrick) Failed() <-chan interface{} {
	return b.failQueue
}

func (b *OutputBrick) loop(inQueue <-chan interface{}) {
	defer func() {
		close(b.errQueue)
	}()
Start:
	_, err := async.Lambda(func() (interface{}, error) {
		b.kernal(inQueue, b.failQueue, b.errQueue)
		return nil, nil
	}, 0)
	if err != nil {
		goto Start
	}
}

func NewOutputBrick(
	name string,
	kernal func(<-chan interface{}, chan<- interface{}, chan<- error)) *OutputBrick {
	return &OutputBrick{
		name:      name,
		kernal:    kernal,
		failQueue: make(chan interface{}, 16),
		errQueue:  make(chan error, 16),
	}
}
