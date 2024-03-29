package main


import (
    "fmt"
    "flag"
    "github.com/keenangraham/go-sqs-deduplication/internal/sqs"
    "github.com/keenangraham/go-sqs-deduplication/internal/dedup"
)




type CommandLineOptions struct {
    QueueURL string
    ProfileName string
    NumWorkers int
    MaxInflight int
}


func parseCommandLineOptions() CommandLineOptions {
    var opts CommandLineOptions
    flag.StringVar(&opts.QueueURL, "queueURL", "https://sqs.us-west-2.amazonaws.com/618537831167/test-queue", "SQS URL")
    flag.StringVar(&opts.ProfileName, "profileName", "", "AWS profile to use")
    flag.IntVar(&opts.NumWorkers, "numWorkers", 20, "Number of concurrent workers to use")
    flag.IntVar(&opts.MaxInflight, "maxInflight", 100000, "Maximum number of inflight messages allowed by queue")
    flag.Parse()
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
    queue := sqs.NewQueue(config)
    deduplicator := dedup.NewDeduplicator(
        &dedup.DeduplicatorConfig{
            Queue: queue,
            NumWorkers: opts.NumWorkers,
            MaxInflight: opts.MaxInflight,
        })
    deduplicator.Run()
}
