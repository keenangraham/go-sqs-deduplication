package dedup


import (
    "sync"
)


type Reseter struct {
    queue Queue
    keepChannel chan string
    wg *sync.WaitGroup
}


func (r *Reseter) getBatchOfMessagesToKeep() []string {
    maxMessages := 10
    var messages []string
    for i := 0; i < maxMessages; i++ {
        message, ok := <- r.keepChannel
        if !ok {
            return messages
        }
        messages = append(messages, message)
    }
    return messages
}


func (r *Reseter) resetMessages() {
    receiptHandles := r.getBatchOfMessagesToKeep()
    for len(receiptHandles) > 0 {
        r.queue.ResetVisibilityBatch(receiptHandles)
        receiptHandles = r.getBatchOfMessagesToKeep()
    }
}


func (r *Reseter) Start() {
    r.wg.Add(1)
    go func() {
        defer r.wg.Done()
        r.resetMessages()
    }()
}
