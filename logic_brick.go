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
	kernal   func(interface{}) (interface{}, error)
	errQueue chan interface{}
	outQueue chan interface{}
}

func (b *LogicBrick) Name() string {
	return b.name
}

func (b *LogicBrick) Linked(inQueue <-chan interface{}) {
	b.loop(inQueue)
}

func (b *LogicBrick) Output() <-chan interface{} {
	return b.outQueue
}

func (b *LogicBrick) Errors() <-chan interface{} {
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
			res, err := async.Lambda(func() (interface{}, error) {
				return b.kernal(msg)
			}, 0)
			if err == nil {
				b.outQueue <- res
			}
				// b.errQueue <- &ErrMessage{
				// 	Path:   b.Name(),
				// 	Raw:    msg.Raw,
				// 	Reason: err.Error(),
				// }
		}(msg)
	}
	b.wg.Wait()
	close(b.outQueue)
	close(b.errQueue)
}

func NewLogicBrick(
	name string,
	kernal func(interface{}) (interface{}, error),
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
		errQueue: make(chan interface{}, 16),
	}
	for i := 0; i < max_worker; i++ {
		l.workers <- true
	}

	return l
}
