package dedup


import (
    "fmt"
    "sync"
)


type Puller struct {
    queue Queue
    state *SharedState
    messagesExist bool
    maxInflight int
    wg *sync.WaitGroup
}


// Only call with mutex locked.
func (p *Puller) processMessages(messages []QueueMessage) {
    for _, message := range messages {
        if storedMessage, exists := p.state.keepMessages[message.UniqueID()]; exists {
            if storedMessage.MessageID() != message.MessageID() {
                p.state.deleteMessages[message.ReceiptHandle()] = struct{}{}
            } else {
                // If for some reason the same message is delivered more than once from the queue.
                continue
            }
        } else {
            p.state.keepMessages[message.UniqueID()] = message
        }
    }
}


// Only call with mutex locked.
func (p *Puller) atMaxInflight() bool {
    return len(p.state.keepMessages) + len(p.state.deleteMessages) >= p.maxInflight
}


func (p *Puller) getMessagesUntilMaxInflight() {
    for {
        messages, err := p.queue.PullMessagesBatch()
        if err != nil {
            fmt.Println("Error pulling messages", err)
            p.messagesExist = false
            return
        }
        if len(messages) == 0 {
            p.messagesExist = false
            break
        }
        p.state.mu.Lock()
        p.processMessages(messages)
        if p.atMaxInflight() {
            fmt.Println("Reaching max inflight messages from puller")
            p.state.mu.Unlock()
            break
        }
        p.state.mu.Unlock()
    }
}


func (p *Puller) Start() {
    p.wg.Add(1)
    go func() {
        defer p.wg.Done()
        p.getMessagesUntilMaxInflight()
    }()
}


func (p *Puller) MessagesExist() bool {
    return p.messagesExist
}


func NewPuller(queue Queue, state *SharedState, messagesExist bool, maxInflight int,  wg *sync.WaitGroup) *Puller {
    return &Puller{
        queue: queue,
        state: state,
        messagesExist: messagesExist,
        maxInflight: maxInflight,
        wg: wg,
    }
}
