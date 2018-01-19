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

type SplitBrick struct {
	name      string
	deepCopy  bool
	chanSize  int
	errQueue  chan error
	outQueues []chan interface{}
}

func (b *SplitBrick) Name() string {
	return b.name
}

func (b *SplitBrick) Linked(inQueue <-chan interface{}) {
	go b.loop(inQueue)
}

func (b *SplitBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *SplitBrick) Succeed() <-chan interface{} {
	output := make(chan interface{}, b.chanSize)
	b.outQueues = append(b.outQueues, output)
	return output
}

func (b *SplitBrick) loop(inQueue <-chan interface{}) {
	for msg := range inQueue {
		for _, output := range b.outQueues {
			if b.deepCopy {
				temp, err := deepCopy(msg)
				if err != nil {
					b.errQueue <- err
					break
				}
				output <- temp
			} else {
				output <- msg
			}
		}
	}
	for _, output := range b.outQueues {
		close(output)
	}
}

func NewSplitBrick(name string, deepCopy bool, chanSize int) *SplitBrick {
	return &SplitBrick{
		name:      name,
		deepCopy:  deepCopy,
		chanSize:  chanSize,
		outQueues: make([]chan interface{}, 0),
		errQueue:  make(chan error, 8),
	}
}
