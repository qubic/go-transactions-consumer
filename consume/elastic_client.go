package consume

import (
	"bytes"
	"context"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/pkg/errors"
	"log"
	"runtime"
	"time"
)

type ElasticDocumentClient interface {
	BulkIndex(ctx context.Context, data []EsDocument) error
}

type ElasticClient struct {
	esClient  *elasticsearch.Client
	indexName string
}

func NewElasticClient(esClient *elasticsearch.Client, indexName string) *ElasticClient {
	return &ElasticClient{
		esClient:  esClient,
		indexName: indexName,
	}
}

type EsDocument struct {
	id      string
	payload []byte
}

func (c *ElasticClient) BulkIndex(ctx context.Context, data []EsDocument) error {
	start := time.Now().UnixMilli()
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      c.indexName,              // The default index name
		Client:     c.esClient,               // The Elasticsearch client
		NumWorkers: min(runtime.NumCPU(), 8), // 8 parallel connections are enough
	})
	if err != nil {
		return errors.Wrap(err, "Error creating bulk indexer")
	}

	for _, d := range data {
		item := esutil.BulkIndexerItem{
			Action:       "index",
			DocumentID:   d.id,
			RequireAlias: true,
			Body:         bytes.NewReader(d.payload),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				msg := "Error indexing document"
				if err != nil {
					log.Printf("%s [%s]: %s: [%s]", msg, d.id, string(d.payload), err)
				} else {
					log.Printf("%s [%s]: %s: [%s: %s]", msg, d.id, string(d.payload), res.Error.Type, res.Error.Reason)
				}
			},
		}
		err = bi.Add(ctx, item)
	}

	err = bi.Close(ctx)
	if err != nil {
		return errors.Wrap(err, "Error closing bulk indexer")
	}

	biStats := bi.Stats()
	end := time.Now().UnixMilli()
	if biStats.NumFailed > 0 {
		return errors.Errorf("%d errors indexing [%d] documents",
			biStats.NumFailed,
			biStats.NumFlushed,
		)
	} else {
		log.Printf("Indexed %d documents (%d bytes, %d requests) in %dms.",
			biStats.NumFlushed,
			biStats.FlushedBytes,
			biStats.NumRequests,
			end-start,
		)
	}
	return nil
}

type FakeElasticClient struct {
	LatestBatch []EsDocument
}

func (c *FakeElasticClient) BulkIndex(_ context.Context, data []EsDocument) error {
	log.Printf("Bulk index [%d] documents.", len(data))
	c.LatestBatch = data
	return nil
}
