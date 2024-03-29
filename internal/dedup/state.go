package dedup


import (
    "sync"
)


type SharedState struct {
    keepMessages map[string]QueueMessage
    deleteMessages map[string]struct{}
    mu sync.Mutex
    maxInflightMessages int
}
