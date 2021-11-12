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
	cfg := &Config{Database: database}
	connector, err := NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements database/sql/driver.DriverContext interface
func (d *SpannerDriver) OpenConnector(database string) (driver.Connector, error) {
	cfg := NewConfig(database)
	return NewConnector(cfg)
}
