package flow

import (
	"github.com/RyouZhang/async-go"
)

type OutputBrick struct {
	name     string
	kernal   func(<-chan *Message, chan<- *LogMessage, chan<- *ErrMessage)
	logQueue chan *LogMessage
	errQueue chan *ErrMessage
}

func (b *OutputBrick) Name() string {
	return b.name
}

func (b *OutputBrick) Linked(inQueue <-chan *Message) {
	b.loop(inQueue)
}

func (b *OutputBrick)Errors() <-chan *ErrMessage {
	return b.errQueue
}

func (b *OutputBrick)Logs() <-chan *LogMessage {
	return b.logQueue
}

func (b *OutputBrick) loop(inQueue <-chan *Message) {
	defer func() {
		close(b.errQueue)
		close(b.logQueue)
	}()
Start:
	_, err := async.Lambda(func()(interface{}, error) {
		b.kernal(inQueue, b.logQueue, b.errQueue)
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

func NewOutputBrick(
	name string,
	kernal func(<-chan *Message, chan<- *LogMessage, chan<- *ErrMessage)) *OutputBrick {
	return &OutputBrick{
		name:     name,
		kernal:   kernal,
		logQueue: make(chan *LogMessage, 16),
		errQueue: make(chan *ErrMessage, 16),
	}
}
