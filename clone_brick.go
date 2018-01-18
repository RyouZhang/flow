package flow

type CloneBrick struct {
	name     string
	chanSize int
	errQueue chan *ErrMessage
	outQueue chan *Message
}

func (b *CloneBrick) Name() string {
	return b.name
}

func (b *CloneBrick) Linked(inQueue chan *Message) {
	go func() {
		for msg := range inQueue {
			b.outQueue <- msg
		}
		close(b.outQueue)
	}()
}

func (b *CloneBrick) Errors() <-chan *ErrMessage {
	return b.errQueue
}

func (b *CloneBrick) Output() <-chan *Message {
	output := make(chan *Message, b.chanSize)
	go func() {
		defer close(output)
		for msg := range b.outQueue {
			temp := &Message{}
			err := DeepCopy(msg, temp)
			if err == nil {		
				output <- temp
			} else {
				b.errQueue <- &ErrMessage{
					Name: b.Name(),
					Raw: msg.Raw,
					Reason: err.Error(),
				}
			}
		}
	}()
	return output
}

func NewCloneBrick(name string, chanSize int) *CloneBrick {
	return &CloneBrick{
		name:     name,
		chanSize: chanSize,
		outQueue: make(chan *Message, chanSize),
		errQueue: make(chan *ErrMessage, chanSize),
	}
}
