package spannerdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

const userAgent = "go-sql-driver/spanner v0.0.1"

func init() {
	sql.Register("spanner", &SpannerDriver{})
}

type SpannerDriver struct{}

// Open implements database/sql/driver.Driver interface
func (d *SpannerDriver) Open(database string) (driver.Conn, error) {
	cfg := &Config{database: database}
	connector := &SpannerConnector{cfg: cfg}
	return connector.Connect(context.Background())
}

// OpenConnector implements database/sql/driver.DriverContext interface
func (d *SpannerDriver) OpenConnector(database string) (driver.Connector, error) {
	cfg := NewConfig(database)
	return NewConnector(cfg)
}
