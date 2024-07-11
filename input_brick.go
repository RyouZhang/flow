package flow

import (
	"github.com/RyouZhang/async-go"
)

type InputBrick struct {
	name string
	lc   ILifeCycle

	kernal   func(chan<- *Message, chan<- error, <-chan bool)
	shutdown chan bool

	chanSize      int
	useDefaultOut bool
	errQueue      chan error
	resQueue      chan *Message
	outQueue      chan *Message
	outQueues     []*routeItem
}

func (b *InputBrick) Name() string {
	return b.name
}

func (b *InputBrick) AddLifeCycle(lc ILifeCycle) {
	b.lc = lc
	b.lc.Add(1)
}

func (b *InputBrick) Output() <-chan *Message {
	b.useDefaultOut = true
	return b.outQueue
}

func (b *InputBrick) RouteOutput(method func(*Message) bool) <-chan *Message {
	output := make(chan *Message, b.chanSize)
	b.outQueues = append(b.outQueues, &routeItem{
		method:   method,
		outQueue: output,
	})
	return output
}

func (b *InputBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *InputBrick) Start() {
	b.loop()
}

func (b *InputBrick) Stop() {
	defer func() {
		b.lc.Done()
	}()
	b.shutdown <- true
	close(b.shutdown)
}

func (b *InputBrick) loop() {
	defer func() {
		close(b.resQueue)
	}()
Start:
	_, err := async.Lambda(func() (interface{}, error) {
		b.kernal(b.resQueue, b.errQueue, b.shutdown)
		return nil, nil
	}, 0)
	if err != nil {
		b.errQueue <- err
		goto Start
	}
}

func (b *InputBrick) pump() {
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

func NewInputBrick(
	name string,
	kernal func(chan<- *Message, chan<- error, <-chan bool),
	chanSize int) *InputBrick {
	b := &InputBrick{
		name:          name,
		kernal:        kernal,
		chanSize:      chanSize,
		useDefaultOut: false,
		shutdown:      make(chan bool),
		errQueue:      make(chan error, 8),
		resQueue:      make(chan *Message, chanSize),
		outQueue:      make(chan *Message, chanSize),
		outQueues:     make([]*routeItem, 0),
	}
	go b.pump()
	return b
}
