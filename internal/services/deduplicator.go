package services


import (
    "fmt"
    "github.com/keenangraham/go-sqs-deduplication/internal/dedup"
)


type Deduplicator struct {
    queue dedup.Queue
}


func (d *Deduplicator) Run() {
    fmt.Println("Running loop")
}


func NewDeduplicator(queue dedup.Queue) *Deduplicator {
    return &Deduplicator{
        queue: queue,
    }
}
