package main


import (
    "github.com/keenangraham/go-sqs-deduplication/internal/sqs"
    "github.com/keenangraham/go-sqs-deduplication/internal/dedup"
)


func main() {
    queueUrl := "https://sqs.us-west-2.amazonaws.com/618537831167/test-queue"
    config := &sqs.QueueConfig{
        QueueUrl: &queueUrl,
        ProfileName: "default",
        MessageParser: sqs.InvalidationQueueMessageParser,
    }
    queue := sqs.NewQueue(config)
    deduplicator := dedup.NewDeduplicator(
        &dedup.DeduplicatorConfig{
            Queue: queue,
            NumWorkers: 20,
            MaxInflight: 10000,
        })
    deduplicator.Run()
}
