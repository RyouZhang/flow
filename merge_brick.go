package flow

import (
	"sync"
)

type MergeBrick struct {
	name     string
	once     sync.Once
	wg       sync.WaitGroup
	outQueue chan *Message
}

func (b *MergeBrick) Name() string {
	return b.name
}

func (b *MergeBrick) Output() <-chan *Message {
	return b.outQueue
}

func (b *MergeBrick) Linked(inQueue <-chan *Message) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for msg := range inQueue {
			b.outQueue <- msg
		}
	}()
	go b.once.Do(func() {
		b.wg.Wait()
		close(b.outQueue)
	})
}

func NewMergeBrick(name string, chanSize int) *MergeBrick {
	return &MergeBrick{
		name:     name,
		outQueue: make(chan *Message, chanSize)}
}
