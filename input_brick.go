package flow

import (
	"github.com/RyouZhang/async-go"
)

type InputBrick struct {
	name     string
	kernal   func(chan<- interface{}, chan<- error, <-chan bool)
	shutdown chan bool
	errQueue chan error
	outQueue chan interface{}
}

func (b *InputBrick) Name() string {
	return b.name
}

func (b *InputBrick) Succeed() <-chan interface{} {
	return b.outQueue
}

func (b *InputBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *InputBrick) Start() {
	b.loop()
}

func (b *InputBrick) Stop() {
	b.shutdown <- true
	close(b.shutdown)
}

func (b *InputBrick) loop() {
	defer func() {
		close(b.errQueue)
		close(b.outQueue)
	}()
Start:
	_, err := async.Lambda(func() (interface{}, error) {
		b.kernal(b.outQueue, b.errQueue, b.shutdown)
		return nil, nil
	}, 0)
	if err != nil {
		b.errQueue <- err
		goto Start
	}
}

func NewInputBrick(
	name string,
	kernal func(chan<- interface{}, chan<- error, <-chan bool),
	chanSize int) *InputBrick {
	return &InputBrick{
		name:     name,
		kernal:   kernal,
		shutdown: make(chan bool),
		errQueue: make(chan error, 8),
		outQueue: make(chan interface{}, chanSize),
	}
}
