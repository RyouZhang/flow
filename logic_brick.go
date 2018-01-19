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
	kernal   func(interface{}, chan<- interface{}, chan<- interface{}, chan<- error)
	errQueue chan error
	failQueue chan interface{}
	outQueue chan interface{}
}

func (b *LogicBrick) Name() string {
	return b.name
}

func (b *LogicBrick) Linked(inQueue <-chan interface{}) {
	b.loop(inQueue)
}

func (b *LogicBrick) Succeed() <-chan interface{} {
	return b.outQueue
}

func (b *LogicBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *LogicBrick) loop(inQueue <-chan interface{}) {
	for msg := range inQueue {
		<-b.workers
		b.wg.Add(1)
		go func(msg interface{}) {
			defer func() {
				b.wg.Done()
				b.workers <- true
			}()
			_, err := async.Lambda(func() (interface{}, error) {
				b.kernal(msg, b.outQueue, b.failQueue, b.errQueue)
				return nil, nil
			}, 0)
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
	kernal func(interface{}, chan<- interface{}, chan<- interface{}, chan<- error),
	max_worker int,
	chanSize int) *LogicBrick {
	if max_worker <= 1 {
		max_worker = 1
	}
	if max_worker > runtime.NumCPU() {
		max_worker = runtime.NumCPU()
	}
	l := &LogicBrick{
		name:     name,
		kernal:   kernal,
		workers:  make(chan bool, max_worker),
		outQueue: make(chan interface{}, chanSize),
		failQueue: make(chan interface{}, chanSize),
		errQueue: make(chan error, 16),
	}
	for i := 0; i < max_worker; i++ {
		l.workers <- true
	}

	return l
}
