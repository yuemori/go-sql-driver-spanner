package spannerdriver

import (
	"database/sql/driver"
)

var _ driver.Connector = &SpannerConnector{}
