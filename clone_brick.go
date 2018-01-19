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

func deepCopy(source interface{}) (interface{}, error) {
	var (
		err error
		raw []byte
	)
	enc := codec.NewEncoderBytes(&raw, &bh)
	err = enc.Encode(source)
	if err != nil {
		return nil, err
	}
	target := reflect.New(reflect.TypeOf(source)).Interface()
	dec := codec.NewDecoderBytes(raw, &bh)
	err = dec.Decode(target)
	if err != nil {
		return nil, err
	}
	return target, nil
}

type CloneBrick struct {
	name      string
	chanSize  int
	errQueue  chan interface{}
	outQueues []chan interface{}
}

func (b *CloneBrick) Name() string {
	return b.name
}

func (b *CloneBrick) Linked(inQueue chan interface{}) {
	go b.loop(inQueue)
}

func (b *CloneBrick) Errors() <-chan interface{} {
	return b.errQueue
}

func (b *CloneBrick) Output() <-chan interface{} {
	output := make(chan interface{}, b.chanSize)
	b.outQueues = append(b.outQueues, output)
	return output
}

func (b *CloneBrick) loop(inQueue chan interface{}) {
	for msg := range inQueue {
		for _, output := range b.outQueues {
			temp, err := deepCopy(msg)
			if err != nil {
				//todo
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
		outQueues: make([]chan interface{}, 0),
		errQueue:  make(chan interface{}, chanSize),
	}
}
