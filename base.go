package flow

import (
	"context"
)

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
	Errors() <-chan error
}

type Message struct {
	Ctx     context.Context
	Headers map[string]string
	Data    interface{}
}
