package dedup_test

import (
    "testing"
    "github.com/keenangraham/go-sqs-deduplication/internal/memory"
    "github.com/keenangraham/go-sqs-deduplication/internal/dedup"
)


func TestDeduplicatorHalfDuplicate(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(1000)
    inMemoryQueue.AddMessages(generatedMessages)
    generatedMessages = memory.GenerateInMemoryMessages(1000)
    inMemoryQueue.AddMessages(generatedMessages)
    config := &dedup.DeduplicatorConfig{
        Queue: inMemoryQueue,
        NumWorkers: 20,
        MaxInflight: 1500,
    }
    deduplicator := dedup.NewDeduplicator(config)
    deduplicator.Run()
    if len(inMemoryQueue.GetDeletedMessages()) != 1000 {
        t.Errorf("Expected 1000 messages to be deleted, got %d", len(inMemoryQueue.GetDeletedMessages()))
    }
    if len(inMemoryQueue.GetResetMessages()) != 1000 {
        t.Errorf("Expected 10000 messages to be reset, got %d", len(inMemoryQueue.GetResetMessages()))
    }
    if inMemoryQueue.MessagesLen() != 0 {
        t.Error("Queue should be empty")
    }
}


func TestDeduplicatorAllUnique(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(3000)
    inMemoryQueue.AddMessages(generatedMessages)
    config := &dedup.DeduplicatorConfig{
        Queue: inMemoryQueue,
        NumWorkers: 5,
        MaxInflight: 10000,
    }
    deduplicator := dedup.NewDeduplicator(config)
    deduplicator.Run()
    if len(inMemoryQueue.GetDeletedMessages()) != 0 {
        t.Errorf("Expected 0 messages to be deleted, got %d", len(inMemoryQueue.GetDeletedMessages()))
    }
    if len(inMemoryQueue.GetResetMessages()) != 3000 {
        t.Errorf("Expected 3000 messages to be reset, got %d", len(inMemoryQueue.GetResetMessages()))
    }
    if inMemoryQueue.MessagesLen() != 0 {
        t.Error("Queue should be empty")
    }
}
