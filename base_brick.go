package flow

import (
	"github.com/RyouZhang/async-go"
)

type BaseBrick struct {
	name string
	lc   ILifeCycle

	kernal   func(*Message) (*Message, error)
	errQueue chan error
	outQueue chan *Message
}

func (b *BaseBrick) Name() string {
	return b.name
}

func (b *BaseBrick) AddLifeCycle(lc ILifeCycle) {
	b.lc = lc
}

func (b *BaseBrick) Linked(inQueue <-chan *Message) {
	b.loop(inQueue)
}

func (b *BaseBrick) Output() <-chan *Message {
	return b.outQueue
}

func (b *BaseBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *BaseBrick) loop(inQueue <-chan *Message) {
	defer func() {
		close(b.errQueue)
		close(b.outQueue)
		b.lc.Done()
	}()
	for msg := range inQueue {
		res, err := async.Safety(func() (interface{}, error) {
			return b.kernal(msg)
		})
		if err != nil {
			b.errQueue <- err
			continue
		}
		b.outQueue <- res.(*Message)
	}
}

func NewBaseBrick(
	name string,
	kernal func(*Message) (*Message, error),
	chanSize int) *BaseBrick {
	l := &BaseBrick{
		name:     name,
		kernal:   kernal,
		outQueue: make(chan *Message, chanSize),
		errQueue: make(chan error, 8),
	}
	return l
}
