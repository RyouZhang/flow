package flow

import (
	"fmt"
	"sync"

	"github.com/RyouZhang/async-go"
)

type PriorityBrick struct {
	name string
	lc   ILifeCycle

	workers chan bool
	wg      sync.WaitGroup
	kernal  func(*Message, chan<- *Message) error

	// support merge
	stopKey    int
	inQueueMux sync.RWMutex
	inQueues   []<-chan *Message
	resQueue   chan *Message

	// support route
	chanSize      int
	outQueues     []*routeItem
	useDefaultOut bool
	outQueue      chan *Message
	errQueue      chan error
}

func (b *PriorityBrick) Name() string {
	return b.name
}

func (b *PriorityBrick) AddLifeCycle(lc ILifeCycle) {
	b.lc = lc
}

func (b *PriorityBrick) Linked(queue <-chan *Message) {
	b.inQueueMux.Lock()
	defer b.inQueueMux.Unlock()
	b.inQueues = append(b.inQueues, queue)
	b.stopKey = b.stopKey<<1 | 1
}

func (b *PriorityBrick) Output() <-chan *Message {
	b.useDefaultOut = true
	return b.outQueue
}

func (b *PriorityBrick) RouteOutput(method func(*Message) bool) <-chan *Message {
	output := make(chan *Message, b.chanSize)
	b.outQueues = append(b.outQueues, &routeItem{
		method:   method,
		outQueue: output,
	})
	return output
}

func (b *PriorityBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *PriorityBrick) handler(queue <-chan *Message) error {
	select {
	case msg, ok := <-queue:
		{
			if false == ok {
				return fmt.Errorf("Closed")
			}
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
			return nil
		}
	default:
		return fmt.Errorf("Downgrade")
	}
}

func (b *PriorityBrick) loop() {
	defer func() {
		close(b.resQueue)
	}()
	flagKey := 0
	for {
	PULL:
		b.inQueueMux.RLock()
		if len(b.inQueues) == 0 {
			b.inQueueMux.RUnlock()
			continue
		}
		for i, queue := range b.inQueues {
			err := b.handler(queue)
			switch {
			case err != nil && err.Error() == "Closed":
				flagKey = flagKey | 1<<i
			case err != nil && err.Error() == "Downgrade":
			default:
				b.inQueueMux.RUnlock()
				goto PULL
			}
		}
		b.inQueueMux.RUnlock()
		if flagKey == b.stopKey {
			return
		}
	}
}

func (b *PriorityBrick) pump() {
	defer func() {
		b.lc.Done()
	}()
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

func NewPriorityBrick(
	name string,
	kernal func(*Message, chan<- *Message) error,
	max_worker int,
	chanSize int) *PriorityBrick {
	if max_worker <= 1 {
		max_worker = 1
	}
	l := &PriorityBrick{
		name:          name,
		kernal:        kernal,
		chanSize:      chanSize,
		useDefaultOut: false,
		workers:       make(chan bool, max_worker),
		outQueue:      make(chan *Message, chanSize),
		errQueue:      make(chan error, 8),
		inQueues:      make([]<-chan *Message, 0),
		resQueue:      make(chan *Message, chanSize),
		outQueues:     make([]*routeItem, 0),
	}
	go l.loop()
	go l.pump()
	return l
}
