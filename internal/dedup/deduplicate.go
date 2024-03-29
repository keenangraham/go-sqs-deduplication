package dedup


import (
    "fmt"
)


type Deduplicator struct {
    queue Queue
}


func (d *Deduplicator) Run() {
    fmt.Println("Running loop")
}


func NewDeduplicator(queue Queue) *Deduplicator {
    return &Deduplicator{
        queue: queue,
    }
}
