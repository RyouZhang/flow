package flow

import (
	"github.com/RyouZhang/async-go"
)

type InputBrick struct {
	name     string
	kernal   func(chan<- *Message, chan<- *LogMessage, <-chan bool)
	shutdown chan bool
	logQueue chan *LogMessage
	outQueue chan *Message
}

func (b *InputBrick) Name() string {
	return b.name
}

func (b *InputBrick) Output() <-chan *Message {
	return b.outQueue
}

func (b *InputBrick)Logs() <-chan *LogMessage {
	return b.logQueue
}

func (b *InputBrick) Start() {
	defer close(b.outQueue)
Start:
	_, err := async.Lambda(func()(interface{}, error) {
		b.kernal(b.outQueue,  b.logQueue, b.shutdown)
		return nil, nil
	}, 0)
	if err != nil {
		b.logQueue <- &LogMessage{
			Level: 3,
			Name: b.Name(),
			Raw: err.Error(),
		}
		goto Start
	}
}

func (b *InputBrick) Stop() {
	b.shutdown <- true
	close(b.shutdown)
	close(b.logQueue)
}

func NewInputBrick(
	name string,
	kernal func(chan<- *Message, chan<- *LogMessage, <-chan bool),
	size int) *InputBrick {
	return &InputBrick{
		name:     name,
		kernal:   kernal,
		shutdown: make(chan bool),
		outQueue: make(chan *Message, size),
		logQueue: make(chan *LogMessage, 16),
	}
}
