package spannerdriver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

type SpannerConnector struct {
	cfg    *Config
	client *spanner.Client
}

// NewConnector returns database/sql/driver.Connector implementation for cloud spanner.
func NewConnector(cfg *Config) (driver.Connector, error) {
	opts := append(cfg.ClientOptions, option.WithUserAgent(userAgent))
	client, err := spanner.NewClientWithConfig(
		context.Background(),
		cfg.database,
		cfg.ClientConfig,
		opts...,
	)
	if err != nil {
		return nil, err
	}
	return &SpannerConnector{cfg: cfg, client: client}, nil
}

func (c *SpannerConnector) Client() *spanner.Client {
	return c.client
}

// Driver implements database/sql/driver.Connector interface
func (c *SpannerConnector) Driver() driver.Driver {
	return &SpannerDriver{}
}

// Connect implements database/sql/driver.Connector interface
func (c *SpannerConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn := &spannerConn{
		client:  c.client,
		closech: make(chan struct{}),
	}
	conn.startWatcher()
	if err := conn.watchCancel(ctx); err != nil {
		conn.cleanup()
		return nil, err
	}
	defer conn.finish()

	return conn, nil
}
