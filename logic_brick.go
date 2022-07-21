package flow

import (
	"runtime"
	"sync"

	"github.com/RyouZhang/async-go"
)

type LogicBrick struct {
	name     string
	workers  chan bool
	wg       sync.WaitGroup
	kernal   func(*Message, chan<- *Message) error
	errQueue chan error
	outQueue chan *Message
}

func (b *LogicBrick) Name() string {
	return b.name
}

func (b *LogicBrick) Linked(inQueue <-chan *Message) {
	b.loop(inQueue)
}

func (b *LogicBrick) Output() <-chan *Message {
	return b.outQueue
}

func (b *LogicBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *LogicBrick) loop(inQueue <-chan *Message) {
	for msg := range inQueue {
		b.workers <- true
		b.wg.Add(1)
		go func(msg *Message) {
			defer func() {
				b.wg.Done()
				<-b.workers
			}()
			_, err := async.Safety(func() (interface{}, error) {
				err := b.kernal(msg, b.outQueue)
				return nil, err
			})
			if err != nil {
				b.errQueue <- err
			}
		}(msg)
	}
	b.wg.Wait()
	close(b.outQueue)
	close(b.errQueue)
}

func NewLogicBrick(
	name string,
	kernal func(*Message, chan<- *Message) error,
	max_worker int,
	chanSize int) *LogicBrick {
	if max_worker <= 1 {
		max_worker = 1
	}
	l := &LogicBrick{
		name:     name,
		kernal:   kernal,
		workers:  make(chan bool, max_worker),
		outQueue: make(chan *Message, chanSize),
		errQueue: make(chan error, 8),
	}
	return l
}
