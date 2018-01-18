package flow

type OutputBrick struct {
	name     string
	kernal   func(<-chan *Message, chan<- *LogMessage, chan<- *ErrMessage)
	logQueue chan *LogMessage
	errQueue chan *ErrMessage
}

func (b *OutputBrick) Name() string {
	return b.name
}

func (b *OutputBrick) Linked(inQueue <-chan *Message) {
	b.loop(inQueue)
}

func (b *OutputBrick)Errors() <-chan *ErrMessage {
	return b.errQueue
}

func (b *OutputBrick)Logs() <-chan *LogMessage {
	return b.logQueue
}

func (b *OutputBrick) loop(inQueue <-chan *Message) {
	defer func() {
		close(b.errQueue)
		close(b.logQueue)
	}()
	b.kernal(inQueue, b.logQueue, b.errQueue)
}

func NewOutputBrick(
	name string,
	kernal func(<-chan *Message, chan<- *LogMessage, chan<- *ErrMessage)) *OutputBrick {
	return &OutputBrick{
		name:     name,
		kernal:   kernal,
		logQueue: make(chan *LogMessage, 16),
		errQueue: make(chan *ErrMessage, 16),
	}
}
