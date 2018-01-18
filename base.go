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

func DeepCopy(source interface{}, target interface{}) error {
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

type Message struct {
	Id     string
	Raw    []byte
	Object map[string]interface{}
	Extra  map[string]interface{}
}

type LogMessage struct {
	Level int8
	Name  string
	Raw   string
}

type ErrMessage struct {
	Name   string
	Raw    []byte
	Reason string
}

type IBrick interface {
	Name() string
}

type IInput interface {
	Linked(<-chan *Message)
}

type IEntry interface {
	Start()
	Stop()
}

type IOutput interface {
	Output() <-chan *Message
}

type IError interface {
	Errors() <-chan *ErrMessage
}

type ILogs interface {
	Logs() <-chan *LogMessage
}
