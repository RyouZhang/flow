package flow

import (
	"sync"

	"github.com/RyouZhang/async-go"
)

type LogicBrick struct {
	name    string
	workers chan bool
	wg      sync.WaitGroup
	kernal  func(*Message, chan<- *Message) error

	// support merge
	inMux    sync.WaitGroup
	inOnce   sync.Once
	inQueue  chan *Message
	resQueue chan *Message

	// support route
	chanSize      int
	outQueues     []*routeItem
	useDefaultOut bool
	outQueue      chan *Message
	errQueue      chan error
}

func (b *LogicBrick) Name() string {
	return b.name
}

func (b *LogicBrick) Linked(queue <-chan *Message) {
	b.inMux.Add(1)
	go func() {
		defer b.inMux.Done()
		for msg := range queue {
			b.inQueue <- msg
		}
	}()
	go b.inOnce.Do(func() {
		b.inMux.Wait()
		close(b.inQueue)
	})
}

func (b *LogicBrick) Output() <-chan *Message {
	b.useDefaultOut = true
	return b.outQueue
}

func (b *LogicBrick) RouteOutput(method func(*Message) bool) <-chan *Message {
	output := make(chan *Message, b.chanSize)
	b.outQueues = append(b.outQueues, &routeItem{
		method:   method,
		outQueue: output,
	})
	return output
}

func (b *LogicBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *LogicBrick) loop() {
	for msg := range b.inQueue {
		b.workers <- true
		b.wg.Add(1)
		go func(msg *Message) {
			defer func() {
				b.wg.Done()
				<-b.workers
			}()
			_, err := async.Safety(func() (interface{}, error) {
				err := b.kernal(msg, b.resQueue)
				return nil, err
			})
			if err != nil {
				b.errQueue <- err
			}
		}(msg)
	}
	b.wg.Wait()
	close(b.resQueue)
}

func (b *LogicBrick) pump() {
	for msg := range b.resQueue {
		flag := false
		for _, item := range b.outQueues {
			if item.method == nil {
				continue
			}
			res, err := async.Safety(func() (interface{}, error) {
				res := item.method(msg)
				return res, nil
			})
			if err != nil {
				b.errQueue <- err
			} else {
				if res.(bool) {
					flag = true
					item.outQueue <- msg
					break
				}
			}
		}
		if false == flag && b.useDefaultOut == true {
			b.outQueue <- msg
		}
	}
	for _, item := range b.outQueues {
		close(item.outQueue)
	}
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
		name:          name,
		kernal:        kernal,
		chanSize:      chanSize,
		useDefaultOut: false,
		workers:       make(chan bool, max_worker),
		outQueue:      make(chan *Message, chanSize),
		errQueue:      make(chan error, 8),
		inQueue:       make(chan *Message, chanSize),
		resQueue:      make(chan *Message, chanSize),
		outQueues:     make([]*routeItem, 0),
	}
	go l.loop()
	go l.pump()
	return l
}
