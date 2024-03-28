package dedup


type SharedState struct {
    keepMessages map[string]SQSMessage
    deleteMessages map[string]struct{}
    mu sync.Mutex
    maxInflightMessages int
}
