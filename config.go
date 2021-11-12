package spannerdriver

import (
	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

type Config struct {
	Database string

	ClientConfig  spanner.ClientConfig
	ClientOptions []option.ClientOption
}

func NewConfig(database string) *Config {
	return &Config{
		Database:      database,
		ClientOptions: make([]option.ClientOption, 0),
	}
}
