package flow

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

type IRoute interface {
	RouteOutput(func(*Message) bool) <-chan *Message
}
