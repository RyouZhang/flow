package flow

import (
	"github.com/RyouZhang/async-go"
)

type routeItem struct {
	method   func(*Message) bool
	outQueue chan *Message
}

type RouteBrick struct {
	name string
	lc   ILifeCycle

	chanSize  int
	errQueue  chan error
	outQueues []*routeItem
}

func (b *RouteBrick) Name() string {
	return b.name
}

func (b *RouteBrick) AddLifeCycle(lc ILifeCycle) {
	b.lc = lc
	b.lc.Add(1)
}

func (b *RouteBrick) Linked(inQueue <-chan *Message) {
	go b.loop(inQueue)
}

func (b *RouteBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *RouteBrick) RouteOutput(method func(*Message) bool) <-chan *Message {
	output := make(chan *Message, b.chanSize)
	b.outQueues = append(b.outQueues, &routeItem{
		method:   method,
		outQueue: output,
	})
	return output
}

func (b *RouteBrick) loop(inQueue <-chan *Message) {
	defer func() {
		close(b.errQueue)
		b.lc.Done()
	}()
	for msg := range inQueue {
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
					item.outQueue <- msg
					break
				}
			}
		}
	}
	for _, item := range b.outQueues {
		close(item.outQueue)
	}
}

func NewRouteBrick(
	name string,
	chanSize int) *RouteBrick {
	return &RouteBrick{
		name:      name,
		chanSize:  chanSize,
		outQueues: make([]*routeItem, 0),
		errQueue:  make(chan error, 8),
	}
}
