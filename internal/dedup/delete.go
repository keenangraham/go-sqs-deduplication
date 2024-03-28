package dedup


type Deleter struct {
    wg *sync.WaitGroup
    queue *Queue
    deleteChannel chan string
}


func (d *Deleter) getBatchOfMessagesToDelete() []string {
    maxMessages := 10
    var messages []string
    for i := 0; i < maxMessages; i++ {
        message, ok := <- d.deleteChannel
        if !ok {
            return messages
        }
        messages = append(messages, message)
    }
    return messages
}


func (d *Deleter) deleteMessages() {
    messages := d.getBatchOfMessagesToDelete()
    for len(messages) > 0 {
        deleteMessagesBatch(d.client, d.queueUrl, messages)
        messages = d.getBatchOfMessagesToDelete()
    }
}


func (d *Deleter) Start() {
    d.wg.Add(1)
    go func() {
        defer d.wg.Done()
        d.deleteMessages()
    }()
}
