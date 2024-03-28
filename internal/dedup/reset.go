package dedup


type Reseter struct {
    wg *sync.WaitGroup
    queue *Queue
    keepChannel chan string
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
    messages := r.getBatchOfMessagesToKeep()
    for len(messages) > 0 {
        resetVisibilityBatch(r.client, r.queueUrl, messages)
        messages = r.getBatchOfMessagesToKeep()
    }
}


func (r *Reseter) Start() {
    r.wg.Add(1)
    go func() {
        defer r.wg.Done()
        r.resetMessages()
    }()
}
