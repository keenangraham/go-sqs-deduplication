# go-sqs-deduplication

### Usage

Note the deduplicator expects SQS messages in following format:

```json
{
    "data": {
        "uuid": "abc123"
     }
}
```

Where `uuid` is the unique identifier used to deduplicate messages.

The code can be extended to work with other queues or message formats.

Run once:
```bash
go run cmd/dedup.go -queueURL=someURL -numWorkers=100  -profileName=someProfile
```

Run forever:
```bash
$ go run cmd/dedup.go -queueURL=someURL -numWorkers=50 -runForever -secondsToSleepBetweenRuns=600
```

Help:
```
  -maxInflight int
    	Maximum number of inflight messages allowed by queue (default 100000)
  -numWorkers int
    	Number of concurrent workers to use (default 20)
  -profileName string
    	AWS profile to use
  -queueURL string
    	SQS URL (required)
  -runForever
    	Runs in a loop with secondsToSleepBetweenRuns
  -secondsToSleepBetweenRuns int
    	Time to sleep between runs if running forever (default 60)
```