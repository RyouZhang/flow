package flow

type Message struct {
	Id     string		//MD5 Raw Is A Good Choose!
	Raw    []byte
	Object map[string]interface{}
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
