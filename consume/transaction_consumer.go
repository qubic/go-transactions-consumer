package consume

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"time"
)

type TransactionConsumer struct {
	kafkaClient   *kgo.Client
	elasticClient ElasticDocumentClient
	metrics       *Metrics
	currentTick   uint32
	currentEpoch  uint32
}

type Transaction struct {
	Hash  string `json:"hash"`
	Epoch uint32 `json:"epoch"`
	Tick  uint32 `json:"tick"`
	// TODO add properties
}

func NewTransactionConsumer(client *kgo.Client, elasticClient ElasticDocumentClient, metrics *Metrics) *TransactionConsumer {
	return &TransactionConsumer{
		kafkaClient:   client,
		metrics:       metrics,
		elasticClient: elasticClient,
	}
}

func (c *TransactionConsumer) Consume() error {
	for {
		count, err := c.ConsumeBatch()
		if err == nil {
			log.Printf("Processed [%d] transactions. Latest tick: [%d]", count, c.currentTick)
		} else {
			// if there is an error consuming we abort. We need to fix the error before trying again.
			log.Fatalf("Error consuming transactions: %v", err) // exits
			// TODO return error
		}
		time.Sleep(time.Second)
	}
}

func (c *TransactionConsumer) ConsumeBatch() (int, error) {
	ctx := context.Background()
	fetches := c.kafkaClient.PollRecords(ctx, 1000) // batch process max x messages in one run
	if errs := fetches.Errors(); len(errs) > 0 {
		// All errors are retried internally when fetching, but non-retryable errors are
		// returned from polls so that users can notice and take action.
		for _, err := range errs {
			log.Printf("Error: %v", err)
		}
		return -1, errors.New("Error fetching records")
	}

	var documents []EsDocument
	// We can iterate through a record iterator...
	iter := fetches.RecordIter()
	for !iter.Done() {

		record := iter.Next()

		var transaction Transaction
		err := json.Unmarshal(record.Value, &transaction)
		if err != nil {
			return -1, errors.Wrapf(err, "Error unmarshalling value %s", string(record.Value))
		}

		documents = append(documents, EsDocument{
			id:      transaction.Hash,
			payload: record.Value,
		})

		// transactions should be ordered by tick (not 100% but close enough, as order is only guaranteed within one tick)
		if transaction.Tick > c.currentTick {
			c.currentTick = transaction.Tick
			c.currentEpoch = transaction.Epoch
			c.metrics.IncProcessedTicks()

		}
		c.metrics.IncProcessedMessages()
	}

	err := c.elasticClient.BulkIndex(ctx, documents)
	if err != nil {
		return -1, errors.Wrapf(err, "Error bulk indexing [%d] documents.", len(documents))
	}
	c.metrics.SetProcessedTick(c.currentEpoch, c.currentTick)

	err = c.kafkaClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "Error committing offsets")
	}
	return len(documents), nil
}
