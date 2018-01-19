package flow

import (
	"reflect"

	"github.com/ugorji/go/codec"
)

var (
	bh codec.BincHandle
)

func init() {
	bh.MapType = reflect.TypeOf(map[string]interface{}(nil))
}

func deepCopy(source interface{}, target interface{}) error {
	var (
		err error
		raw []byte
	)
	enc := codec.NewEncoderBytes(&raw, &bh)
	err = enc.Encode(source)
	if err != nil {
		return err
	}
	dec := codec.NewDecoderBytes(raw, &bh)
	err = dec.Decode(target)
	if err != nil {
		return err
	}
	return nil
}

type CloneBrick struct {
	name      string
	chanSize  int
	errQueue  chan *ErrMessage
	outQueues []chan *Message
}

func (b *CloneBrick) Name() string {
	return b.name
}

func (b *CloneBrick) Linked(inQueue chan *Message) {
	go b.loop(inQueue)
}

func (b *CloneBrick) Errors() <-chan *ErrMessage {
	return b.errQueue
}

func (b *CloneBrick) Output() <-chan *Message {
	output := make(chan *Message, b.chanSize)
	b.outQueues = append(b.outQueues, output)
	return output
}

func (b *CloneBrick) loop(inQueue chan *Message) {
	for msg := range inQueue {
		for _, output := range b.outQueues {
			temp := &Message{}
			err := deepCopy(msg, temp)
			if err != nil {
				break
			}
			output <- temp
		}
	}
	for _, output := range b.outQueues {
		close(output)
	}
}

func NewCloneBrick(name string, chanSize int) *CloneBrick {
	return &CloneBrick{
		name:      name,
		chanSize:  chanSize,
		outQueues: make([]chan *Message, 0),
		errQueue:  make(chan *ErrMessage, chanSize),
	}
}
