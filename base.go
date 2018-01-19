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

type ISucceed interface {
	Succeed() <-chan interface{}
}

type IFailed interface {
	Failed() <-chan interface{}
}

type IError interface {
	Errors() <-chan error
}
