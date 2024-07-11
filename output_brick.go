package flow

import (
	"github.com/RyouZhang/async-go"
)

type OutputBrick struct {
	name         string
	kernal       func(*Message) error
	gracefulStop func()
	errQueue     chan error
}

func (b *OutputBrick) Name() string {
	return b.name
}

func (b *OutputBrick) Linked(inQueue <-chan *Message) {
	go b.loop(inQueue)
}

func (b *OutputBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *OutputBrick) loop(inQueue <-chan *Message) {
	defer func() {
		close(b.errQueue)
	}()
	for msg := range inQueue {
		_, err := async.Safety(func() (interface{}, error) {
			err := b.kernal(msg)
			return nil, err
		})
		if err != nil {
			b.errQueue <- err
		}
	}
	if b.gracefulStop != nil {
		b.gracefulStop()
	}
}

func NewOutputBrick(
	name string,
	kernal func(*Message) error,
	gracefulStop func()) *OutputBrick {
	return &OutputBrick{
		name:         name,
		kernal:       kernal,
		gracefulStop: gracefulStop,
		errQueue:     make(chan error, 8),
	}
}
