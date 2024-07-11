package flow

type SplitBrick struct {
	name string
	lc   ILifeCycle

	deepCopy  func(*Message) (*Message, error)
	chanSize  int
	errQueue  chan error
	outQueues []chan *Message
}

func (b *SplitBrick) Name() string {
	return b.name
}

func (b *SplitBrick) AddLifeCycle(lc ILifeCycle) {
	b.lc = lc
}

func (b *SplitBrick) Linked(inQueue <-chan *Message) {
	go b.loop(inQueue)
}

func (b *SplitBrick) Errors() <-chan error {
	return b.errQueue
}

func (b *SplitBrick) Output() <-chan *Message {
	output := make(chan *Message, b.chanSize)
	b.outQueues = append(b.outQueues, output)
	return output
}

func (b *SplitBrick) loop(inQueue <-chan *Message) {
	defer func() {
		b.lc.Done()
	}()
	for msg := range inQueue {
		for _, output := range b.outQueues {
			if b.deepCopy != nil {
				temp, err := b.deepCopy(msg)
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

func NewSplitBrick(
	name string,
	deepCopy func(*Message) (*Message, error),
	chanSize int) *SplitBrick {
	return &SplitBrick{
		name:      name,
		deepCopy:  deepCopy,
		chanSize:  chanSize,
		outQueues: make([]chan *Message, 0),
		errQueue:  make(chan error, 8),
	}
}
