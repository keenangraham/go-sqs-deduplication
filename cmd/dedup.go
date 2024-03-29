package main


import (
    "fmt"
    "flag"
    "os"
    "github.com/keenangraham/go-sqs-deduplication/internal/sqs"
    "github.com/keenangraham/go-sqs-deduplication/internal/dedup"
)


type CommandLineOptions struct {
    QueueURL string
    ProfileName string
    NumWorkers int
    MaxInflight int
    RunForever bool
    SecondsToSleepBetweenRuns int
}


func parseCommandLineOptions() CommandLineOptions {
    var opts CommandLineOptions
    flag.StringVar(&opts.QueueURL, "queueURL", "", "SQS URL (required)")
    flag.StringVar(&opts.ProfileName, "profileName", "", "AWS profile to use")
    flag.IntVar(&opts.NumWorkers, "numWorkers", 20, "Number of concurrent workers to use")
    flag.IntVar(&opts.MaxInflight, "maxInflight", 100000, "Maximum number of inflight messages allowed by queue")
    flag.BoolVar(&opts.RunForever, "runForever", false, "Runs in a loop with secondsToSleepBetweenRuns")
    flag.IntVar(&opts.SecondsToSleepBetweenRuns,"secondsToSleepBetweenRuns", 60, "Time to sleep between runs if running forever")
    flag.Parse()
    if opts.QueueURL == "" {
        fmt.Println("The 'queueURL' flag is required")
        flag.PrintDefaults()
        os.Exit(1)
    }
    return opts
}


func main() {
    opts := parseCommandLineOptions()
    fmt.Printf("Got command-line arguments: %+v\n", opts)
    config := &sqs.QueueConfig{
        QueueUrl: &opts.QueueURL,
        ProfileName: opts.ProfileName,
        MessageParser: sqs.InvalidationQueueMessageParser, // Use custom parser for other message formats.
    }
    queue := sqs.NewQueue(config) // Use different queue implementation for other queue types.
    deduplicator := dedup.NewDeduplicator(
        &dedup.DeduplicatorConfig{
            Queue: queue,
            NumWorkers: opts.NumWorkers,
            MaxInflight: opts.MaxInflight,
        })
    if opts.RunForever {
        deduplicator.RunForever(opts.SecondsToSleepBetweenRuns)
    } else {
        deduplicator.Run()
    }
}
