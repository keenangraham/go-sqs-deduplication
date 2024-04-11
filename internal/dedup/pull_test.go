package dedup_test


import (
    "sync"
    "testing"
    "github.com/keenangraham/go-sqs-deduplication/internal/memory"
    "github.com/keenangraham/go-sqs-deduplication/internal/dedup"
)


func TestPullerAllUnique(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(203)
    inMemoryQueue.AddMessages(generatedMessages)
    state := dedup.NewSharedState(
        make(map[string]dedup.QueueMessage),
        make(map[string]struct{}),
    )
    wg := &sync.WaitGroup{}
    puller1 := dedup.NewPuller(
        inMemoryQueue,
        state,
        true,
        false,
        1000,
        wg,
    )
    puller2 := dedup.NewPuller(
        inMemoryQueue,
        state,
        true,
        false,
        1000,
        wg,
    )
    puller1.Start()
    puller2.Start()
    wg.Wait()
    if state.KeepMessagesLen() != 203 {
        t.Errorf("Unexpected unique messages to keep %d", state.KeepMessagesLen())
    }
    if state.DeleteMessagesLen() != 0 {
        t.Errorf("Unexpected duplicate messages to delete %d", state.DeleteMessagesLen())
    }
    if puller1.MessagesExist() || puller2.MessagesExist() {
        t.Errorf("Expected no messages to exist on puller")
    }
}


func TestPullerHalfDuplicates(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(203)
    inMemoryQueue.AddMessages(generatedMessages)
    generatedMessages = memory.GenerateInMemoryMessages(203)
    inMemoryQueue.AddMessages(generatedMessages)
    state := dedup.NewSharedState(
        make(map[string]dedup.QueueMessage),
        make(map[string]struct{}),
    )
    wg := &sync.WaitGroup{}
    puller1 := dedup.NewPuller(
        inMemoryQueue,
        state,
        true,
        false,
        1000,
        wg,
    )
    puller2 := dedup.NewPuller(
        inMemoryQueue,
        state,
        true,
        false,
        1000,
        wg,
    )
    puller1.Start()
    puller2.Start()
    wg.Wait()
    if state.KeepMessagesLen() != 203 {
        t.Errorf("Unexpected unique messages to keep %d", state.KeepMessagesLen())
    }
    if state.DeleteMessagesLen() != 203 {
        t.Errorf("Unexpected duplicate messages to delete %d", state.DeleteMessagesLen())
    }
}


func TestPullerAllDuplicates(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    duplicateMessages := memory.MakeDuplicateInMemoryMessages("abc", 83)
    inMemoryQueue.AddMessages(duplicateMessages)
    state := dedup.NewSharedState(
        make(map[string]dedup.QueueMessage),
        make(map[string]struct{}),
    )
    wg := &sync.WaitGroup{}
    puller1 := dedup.NewPuller(
        inMemoryQueue,
        state,
        true,
        false,
        1000,
        wg,
    )
    puller2 := dedup.NewPuller(
        inMemoryQueue,
        state,
        true,
        false,
        1000,
        wg,
    )
    puller1.Start()
    puller2.Start()
    wg.Wait()
    if state.KeepMessagesLen() != 1 {
        t.Errorf("Unexpected unique messages to keep %d", state.KeepMessagesLen())
    }
    if state.DeleteMessagesLen() != 82 {
        t.Errorf("Unexpected duplicate messages to delete %d", state.DeleteMessagesLen())
    }
}


func TestPullerMaxInflight(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    duplicateMessages := memory.MakeDuplicateInMemoryMessages("abc", 500)
    inMemoryQueue.AddMessages(duplicateMessages)
    state := dedup.NewSharedState(
        make(map[string]dedup.QueueMessage),
        make(map[string]struct{}),
    )
    wg := &sync.WaitGroup{}
    puller1 := dedup.NewPuller(
        inMemoryQueue,
        state,
        true,
        false,
        100,
        wg,
    )
    puller2 := dedup.NewPuller(
        inMemoryQueue,
        state,
        true,
        false,
        100,
        wg,
    )
    puller1.Start()
    puller2.Start()
    wg.Wait()
    if state.KeepMessagesLen() != 1 {
        t.Errorf("Unexpected unique messages to keep %d", state.KeepMessagesLen())
    }
    if state.DeleteMessagesLen() != 109 {
        t.Errorf("Unexpected duplicate messages to delete %d", state.DeleteMessagesLen())
    }
    if !puller1.MessagesExist() && !puller2.MessagesExist() {
        t.Errorf("Expected messages to exist on puller")
    }
}
