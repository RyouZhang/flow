package flow

type IBrick interface {
	Name() string
}

type IInput interface {
	Linked(<-chan interface{})
}

type IEntry interface {
	Start()
	Stop()
}

type IOutput interface {
	Output() <-chan interface{}
}

type IError interface {
	Errors() <-chan interface{}
}
