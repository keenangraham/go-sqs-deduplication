package dedup


import (
    "fmt"
    "sync"
)


type Startable interface {
    Start()
}


func startAll[T Startable](actors []T) {
    for _, actor := range actors {
        actor.Start()
    }
}


type DeduplicatorConfig struct {
    Queue Queue
    NumWorkers int
    MaxInflight int
}


type Deduplicator struct {
    config *DeduplicatorConfig
    wg *sync.WaitGroup
    state *SharedState
    pullers []*Puller
    deleters []*Deleter
    reseters []*Reseter
    keepChannel chan string
    deleteChannel chan string
}


func NewDeduplicator(config *DeduplicatorConfig) *Deduplicator {
    return &Deduplicator{
        config: config,
        wg: &sync.WaitGroup{},
        state: &SharedState{
            keepMessages: make(map[string]QueueMessage),
            deleteMessages: make(map[string]struct{}),
        }}
}


func (d *Deduplicator) createPullers() {
    numWorkers := d.config.NumWorkers
    pullers := make([]*Puller, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        puller := &Puller{
            queue: d.config.Queue,
            state: d.state,
            wg: d.wg,
            messagesExist: true,
            maxInflight: d.config.MaxInflight,
        }
        pullers = append(pullers, puller)
    }
    d.pullers = pullers
}


func (d *Deduplicator) createDeleters() {
    numWorkers := d.config.NumWorkers
    deleters := make([]*Deleter, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        deleter := &Deleter{
            queue: d.config.Queue,
            deleteChannel: d.deleteChannel,
            wg: d.wg,
        }
        deleters = append(deleters, deleter)
    }
    d.deleters = deleters
}


func (d *Deduplicator) createReseters() {
    numWorkers := d.config.NumWorkers
    reseters := make([]*Reseter, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        reseter := Reseter{
            queue: d.config.Queue,
            keepChannel: d.keepChannel,
            wg: d.wg,
        }
        reseters = append(reseters, &reseter)
    }
    d.reseters = reseters
}


func (d *Deduplicator) createKeepChannel() {
    d.keepChannel = make(chan string, 10000)
}


func (d *Deduplicator) createDeleteChannel() {
    d.deleteChannel = make(chan string, 100000)
}


func (d *Deduplicator) queueEmpty() bool {
    for _, puller := range d.pullers {
        if puller.messagesExist {
            return false
        }
    }
    return true
}


func (d *Deduplicator) sendMessagesToDelete() {
    d.wg.Add(1)
    go func() {
        defer d.wg.Done()
        d.state.mu.Lock()
        defer d.state.mu.Unlock()
        for receiptHandle := range d.state.deleteMessages {
            d.deleteChannel <- receiptHandle
        }
        d.state.deleteMessages = make(map[string]struct{})
        close(d.deleteChannel)
    }()
}


func (d *Deduplicator) sendMessagesToResetVisibility() {
    d.wg.Add(1)
    go func() {
        defer d.wg.Done()
        d.state.mu.Lock()
        defer d.state.mu.Unlock()
        for _, message := range d.state.keepMessages {
            d.keepChannel <- message.ReceiptHandle()
        }
        d.state.keepMessages = make(map[string]QueueMessage)
        close(d.keepChannel)
    }()
}


func (d *Deduplicator) printInfo() {
    d.state.mu.Lock()
    fmt.Println("Messages to keep:", len(d.state.keepMessages))
    fmt.Println("Messages to delete:", len(d.state.deleteMessages))
    d.state.mu.Unlock()
}


func (d *Deduplicator) atMaxInflight() bool {
    d.state.mu.Lock()
    defer d.state.mu.Unlock()
    if len(d.state.keepMessages) >= d.config.MaxInflight {
       return true
    }
    return false
}


func (d *Deduplicator) Run() {

    fmt.Println("Running loop")
    
    d.createPullers()

    // Run pull message/delete duplicates loop until no more messages, or
    // max inflight of unique messages reached.
    for {
        fmt.Println("Pulling Messages")

        startAll(d.pullers)
        
        d.wg.Wait() // Wait for senders

        d.printInfo() // How many messages to keep/delete
        
        d.createDeleteChannel() //create new delete channel

        d.createDeleters() // create deleters
        
        startAll(d.deleters) // start deleters
        
        d.sendMessagesToDelete() // send messages to deleters
        
        d.wg.Wait() // Wait for deleters and delete sender

        if d.queueEmpty() {
            fmt.Println("Queue empty")
            break
        }

        if d.atMaxInflight() {
            fmt.Println("Max inflight for keep messages")
            break
        }
    }

    // Reset the visibility of messages to keep
    fmt.Println("Reseting visibility on messages to keep")
    
    d.createKeepChannel()
    
    d.createReseters()
    
    startAll(d.reseters)
    
    d.sendMessagesToResetVisibility()
    
    d.wg.Wait() // Wait for messages to reset
    
    fmt.Println("All done")
}
