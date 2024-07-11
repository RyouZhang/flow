package flow

import (
	"sync"

	"github.com/RyouZhang/async-go"
)

type ParallelBrick struct {
	name string
	lc   ILifeCycle

	hash      func(*Message) int
	subQueues []chan *Message
	wg        sync.WaitGroup
	kernal    func(*Message, chan<- *Message) error

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

func (b *ParallelBrick) Name() string {
	return b.name
}

func (b *ParallelBrick) AddLifeCycle(lc ILifeCycle) {
	b.lc = lc
}

func (b *ParallelBrick) Linked(queue <-chan *Message) {
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

func (b *ParallelBrick) Output() <-chan *Message {
	b.useDefaultOut = true
	return b.outQueue
}

func (b *ParallelBrick) RouteOutput(method func(*Message) bool) <-chan *Message {
	output := make(chan *Message, b.chanSize)
	b.outQueues = append(b.outQueues, &routeItem{
		method:   method,
		outQueue: output,
	})
	return output
}

func (b *ParallelBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *ParallelBrick) loop() {
	for msg := range b.inQueue {
		val := b.hash(msg)
		queue := b.subQueues[val%len(b.subQueues)]
		queue <- msg
	}
	for i, _ := range b.subQueues {
		close(b.subQueues[i])
	}
	b.subQueues = nil
	b.wg.Wait()
	close(b.resQueue)
}

func (b *ParallelBrick) pump() {
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
	b.lc.Done()
}

func (b *ParallelBrick) worker(queue <-chan *Message) {
	defer b.wg.Done()
	for msg := range queue {
		_, err := async.Safety(func() (interface{}, error) {
			err := b.kernal(msg, b.resQueue)
			return nil, err
		})
		if err != nil {
			b.errQueue <- err
		}
	}
}

func NewParallelBrick(
	name string,
	hash func(*Message) int,
	kernal func(*Message, chan<- *Message) error,
	workers int,
	chanSize int) *ParallelBrick {
	b := &ParallelBrick{
		name:          name,
		hash:          hash,
		kernal:        kernal,
		chanSize:      chanSize,
		useDefaultOut: false,
		subQueues:     make([]chan *Message, 0),
		outQueue:      make(chan *Message, chanSize),
		errQueue:      make(chan error, 8),
		inQueue:       make(chan *Message, chanSize),
		resQueue:      make(chan *Message, chanSize),
		outQueues:     make([]*routeItem, 0),
	}
	if workers <= 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		b.wg.Add(1)
		queue := make(chan *Message, chanSize)
		b.subQueues = append(b.subQueues, queue)
		go b.worker(queue)
	}
	go b.loop()
	go b.pump()
	return b
}
