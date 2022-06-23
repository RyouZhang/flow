package flow

import (
	"time"

	"github.com/RyouZhang/async-go"
)

type ReduceBrick struct {
	name          string
	windowSeconds int //sec
	msgKeys       []string
	msgDic        map[string][]*Message

	mapping  func(*Message) string
	reduce   func(...*Message) (*Message, error)
	errQueue chan error
	outQueue chan *Message
}

func (b *ReduceBrick) Name() string {
	return b.name
}

func (b *ReduceBrick) Linked(inQueue <-chan *Message) {
	b.loop(inQueue)
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

				index := -1
				for i, key := range b.msgKeys {
					target, ok := b.msgDic[key]
					if !ok {
						index = i
						continue
					}
					if target[0].Timestamp()+int64(b.windowSeconds*1000) <= ts.UnixMilli() {
						index = i

						res, err := async.Safety(func() (interface{}, error) {
							return b.reduce(target...)
						})
						if err != nil {
							b.errQueue <- err
						} else {
							b.outQueue <- res.(*Message)
						}
					} else {
						break
					}
				}
				switch {
				case index == -1:
					//do nothing
				case index == len(b.msgDic)-1:
					{
						b.msgDic = make(map[string][]*Message)
						b.msgKeys = make([]string, 0)
					}
				case index == 0:
					{
						key := b.msgKeys[0]
						delete(b.msgDic, key)
						b.msgKeys = b.msgKeys[1:]
					}
				default:
					{
						for i := 0; i <= index; i++ {
							delete(b.msgDic, b.msgKeys[i])
						}
						b.msgKeys = b.msgKeys[index+1:]
					}
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
					b.msgKeys = append(b.msgKeys, key)
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
		msgKeys:       make([]string, 0),
		msgDic:        make(map[string][]*Message),
		outQueue:      make(chan *Message, chanSize),
		errQueue:      make(chan error, 8),
	}
	return l
}
