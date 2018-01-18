package flow

import (
	"runtime"
	"sync"
)

type LogicBrick struct {
	name     string
	workers  chan bool
	wg       sync.WaitGroup
	kernal   func(*Message) (*Message, error)
	logQueue chan *LogMessage
	errQueue chan *ErrMessage
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

func (b *LogicBrick)Errors() <-chan *ErrMessage {
	return b.errQueue
}

func (b *LogicBrick)Logs() <-chan *LogMessage {
	return b.logQueue
}

func (b *LogicBrick) loop(inQueue <-chan *Message) {
	for msg := range inQueue {
		<-b.workers
		b.wg.Add(1)
		go func(msg *Message) {
			defer func() {
				b.wg.Done()
				b.workers <- true
			}()
			res, err := b.kernal(msg)
			if err != nil {
				b.errQueue <- &ErrMessage{
					Name: b.name,
					Raw: msg.Raw,
					Reason: err.Error(),
				}
			} else {
				b.outQueue <- res
			}
		}(msg)
	}
	b.wg.Wait()
	close(b.outQueue)
	close(b.errQueue)
	close(b.logQueue)
}

func NewLogicBrick(
	name string,
	kernal func(*Message) (*Message, error),
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
		outQueue: make(chan *Message, chanSize),
		logQueue: make(chan *LogMessage, 16),
		errQueue: make(chan *ErrMessage, 16),
	}
	for i := 0; i < max_worker; i++ {
		l.workers <- true
	}

	return l
}
