package flow

import (
	"github.com/RyouZhang/async-go"
)

type BaseBrick struct {
	name      string
	kernal    func(<-chan interface{}, chan<- interface{}, chan<- interface{}, chan<- error)
	errQueue  chan error
	failQueue chan interface{}
	outQueue  chan interface{}
}

func (b *BaseBrick) Name() string {
	return b.name
}

func (b *BaseBrick) Linked(inQueue <-chan interface{}) {
	b.loop(inQueue)
}

func (b *BaseBrick) Succeed() <-chan interface{} {
	return b.outQueue
}

func (b *BaseBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *BaseBrick) loop(inQueue <-chan interface{}) {
	defer func() {
		close(b.errQueue)
		close(b.outQueue)
	}()
Start:
	_, err := async.Lambda(func() (interface{}, error) {
		b.kernal(inQueue, b.outQueue, b.failQueue, b.errQueue)
		return nil, nil
	}, 0)
	if err != nil {
		b.errQueue <- err
		goto Start
	}
}

func NewBaseBrick(
	name string,
	kernal func(<-chan interface{}, chan<- interface{}, chan<- interface{}, chan<- error),
	chanSize int) *BaseBrick {
	l := &BaseBrick{
		name:      name,
		kernal:    kernal,
		outQueue:  make(chan interface{}, chanSize),
		failQueue: make(chan interface{}, chanSize),
		errQueue:  make(chan error, 8),
	}
	return l
}
