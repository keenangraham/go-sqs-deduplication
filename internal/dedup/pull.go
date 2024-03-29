package dedup


import (
    "fmt"
    "sync"
)


type Puller struct {
    wg *sync.WaitGroup
    state *SharedState
    queue Queue
    messagesExist bool
}


// Only call with mutex locked.
func (p *Puller) processMessages(messages []QueueMessage) {
    for _, message := range messages {
        if storedMessage, exists := p.state.keepMessages[message.UniqueID()]; exists {
            if storedMessage.MessageID() != message.MessageID() {
                p.state.deleteMessages[message.ReceiptHandle()] = struct{}{}
            } else {
                continue
            }
        } else {
            p.state.keepMessages[message.UniqueID()] = message
        }
    }
}


// Only call with mutex locked.
func (p *Puller) atMaxInflight() bool {
    return len(p.state.keepMessages) + len(p.state.deleteMessages) >= p.state.maxInflightMessages
}


func (p *Puller) getMessagesUntilMaxInflight() {
    for {
        messages, err := p.queue.PullMessagesBatch()
        if err != nil {
            fmt.Println("Error pulling messages", err)
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
