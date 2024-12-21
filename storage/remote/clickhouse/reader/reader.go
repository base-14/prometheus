package reader

import (
	"context"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/clickhouse/client"
	"github.com/prometheus/prometheus/storage/remote/clickhouse/query"
)

type Reader struct {
	client  client.Client
	builder query.Builder
}

func NewReader(client client.Client, builder query.Builder) *Reader {
	return &Reader{
		client:  client,
		builder: builder,
	}
}

// Read implements storage.QueryableClient
func (r *Reader) Read(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	// Implementation will go here
	return nil, nil
}

// Type implements storage.QueryableClient
func (r *Reader) Type() string {
	return "clickhouse"
}
