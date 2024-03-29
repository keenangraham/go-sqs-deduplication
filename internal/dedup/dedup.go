package dedup


import (
    "fmt"
)


type Deduplicator struct {
    numWorkers int
    maxInflight int
    queue Queue
}


func (d *Deduplicator) Run() {
    fmt.Println("Running loop")
    numWorkers := 40
    maxInflight := 100000
    queueUrl := "https://sqs.us-west-2.amazonaws.com/618537831167/test-queue"
    config, err := config.LoadDefaultConfig(
        context.TODO(), 
        config.WithSharedConfigProfile("default"),
    )
    if err != nil {
        fmt.Println(err)
        return
    }
    client := sqs.NewFromConfig(config)

    var wg sync.WaitGroup
    state := SharedState{
        keepMessages: make(map[string]SQSMessage),
        deleteMessages: make(map[string]struct{}),
        wg: &wg,
        maxInflightMessages: maxInflight,
    }
    var pullers []*Puller
    for i := 0; i < numWorkers; i++ {
        puller := Puller{
            state: &state,
            messagesExist: true,
            client: client,
            queueUrl: &queueUrl,
        }
        pullers = append(pullers, &puller)
    }
    for {
        fmt.Println("Pulling Messages")
        for _, puller := range pullers {
            puller.Start()
        }
        wg.Wait()
        state.lock.Lock()
        fmt.Println("Messages to keep:", len(state.keepMessages))
        fmt.Println("Messages to delete:", len(state.deleteMessages))
        state.lock.Unlock()

        fmt.Println("Deleting duplicates")
        deleteChannel := make(chan string, 10000)
        wg.Add(1)
        go func() {
            defer wg.Done()
            state.lock.Lock()
            defer state.lock.Unlock()
            for receiptHandle := range state.deleteMessages {
                deleteChannel <- receiptHandle
            }
            state.deleteMessages = make(map[string]struct{})
            close(deleteChannel)
        }()
        var deleters []*Deleter
        for i := 0; i < numWorkers; i++ {
            deleter := Deleter{
                state: &state,
                client: client,
                queueUrl: &queueUrl,
                deleteChannel: deleteChannel,
            }
            deleters = append(deleters, &deleter)
        }
        for _, deleter := range deleters{
            deleter.Start()
        }
        wg.Wait()

        if queueEmpty(pullers) {
            fmt.Println("Queue empty")
            break
        }

        state.lock.Lock()
        if len(state.keepMessages) >= maxInflight {
            fmt.Println("Max inflight for keep messages")
            break
        }
        state.lock.Unlock()
    }

    fmt.Println("Reseting visibility on messages to keep")
    keepChannel := make(chan string, 10000)
    wg.Add(1)
    go func() {
        defer wg.Done()
        state.lock.Lock()
        defer state.lock.Unlock()
        for _, message := range state.keepMessages {
            keepChannel <- message.ReceiptHandle
        }
        state.keepMessages = make(map[string]SQSMessage)
        close(keepChannel)
    }()
    var reseters []*Reseter
    for i := 0; i < numWorkers; i++ {
        reseter := Reseter{
            state: &state,
            client: client,
            queueUrl: &queueUrl,
            keepChannel: keepChannel,
        }
        reseters = append(reseters, &reseter)
    }
    for _, reseter := range reseters {
        reseter.Start()
    }
    wg.Wait()
    fmt.Println("All done")
}


func queueEmpty(pullers []*Puller) bool {
    for _, puller := range pullers {
        if puller.messagesExist {
            return false
        }
    }
    return true
}



func NewDeduplicator(queue Queue) *Deduplicator {
    return &Deduplicator{
        queue: queue,
    }
}
