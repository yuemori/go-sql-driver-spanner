package spannerdriver

import (
	"errors"
	"log"
	"os"
)

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...interface{})
}

var errLog = Logger(log.New(os.Stderr, "[spanner] ", log.Ldate|log.Ltime|log.Lshortfile))

// SetLogger is used to set the logger for critical errors.
// The initial logger is os.Stderr.
func SetLogger(logger Logger) error {
	if logger == nil {
		return errors.New("logger is nil")
	}
	errLog = logger
	return nil
}
