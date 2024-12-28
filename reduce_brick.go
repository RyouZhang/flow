package flow

import (
	"time"

	"github.com/RyouZhang/async-go"
)

type ReduceBrick struct {
	name string
	lc   ILifeCycle

	windowSeconds int //sec
	msgDic        map[string][]*Message

	mapping  func(*Message) string
	reduce   func(...*Message) (*Message, error)
	errQueue chan error
	outQueue chan *Message
}

func (b *ReduceBrick) Name() string {
	return b.name
}

func (b *ReduceBrick) AddLifeCycle(lc ILifeCycle) {
	b.lc = lc
}

func (b *ReduceBrick) Linked(inQueue <-chan *Message) {
	go b.loop(inQueue)
}

func (b *ReduceBrick) Output() <-chan *Message {
	return b.outQueue
}

func (b *ReduceBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *ReduceBrick) loop(inQueue <-chan *Message) {
	timer := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case <-timer.C:
			{
				ts := time.Now()

				deleteKeys := make([]string, 0)
				for key, target := range b.msgDic {
					if target[0].Timestamp()+int64(b.windowSeconds*1000) < ts.UnixMilli() {
						continue
					}
					deleteKeys = append(deleteKeys, key)
					res, err := async.Safety(func() (interface{}, error) {
						return b.reduce(target...)
					})
					if err != nil {
						b.errQueue <- err
					} else {
						b.outQueue <- res.(*Message)
					}
				}
				for i, _ := range deleteKeys {
					delete(b.msgDic, deleteKeys[i])
				}
				timer.Reset(500 * time.Millisecond)
			}
		case msg, ok := <-inQueue:
			{
				if false == ok {
					timer.Stop()
					goto End
				}
				if b.mapping == nil || b.reduce == nil {
					b.outQueue <- msg
					continue
				}
				res, err := async.Safety(func() (interface{}, error) {
					key := b.mapping(msg)
					return key, nil
				})
				if err != nil {
					b.errQueue <- err
				}
				key := res.(string)

				_, ok := b.msgDic[key]
				if false == ok {
					b.msgDic[key] = []*Message{msg}
				} else {
					b.msgDic[key] = append(b.msgDic[key], msg)
				}
			}
		}
	}
End:
	for _, target := range b.msgDic {
		res, err := async.Safety(func() (interface{}, error) {
			return b.reduce(target...)
		})
		if err != nil {
			b.errQueue <- err
		} else {
			b.outQueue <- res.(*Message)
		}
	}
	close(b.outQueue)
	close(b.errQueue)
	b.lc.Done()
}

func NewReduceBrick(
	name string,
	windowSeconds int,
	mapping func(*Message) string,
	reduce func(...*Message) (*Message, error),
	chanSize int) *ReduceBrick {
	l := &ReduceBrick{
		name:          name,
		windowSeconds: windowSeconds,
		mapping:       mapping,
		reduce:        reduce,
		msgDic:        make(map[string][]*Message),
		outQueue:      make(chan *Message, chanSize),
		errQueue:      make(chan error, 8),
	}
	return l
}
