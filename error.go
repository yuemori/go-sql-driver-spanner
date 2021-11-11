package spannerdriver

import (
	"log"
	"os"

	"github.com/pkg/errors"
)

var (
	ErrInvalidConn                = errors.New("invalid connection")
	ErrWriteInReadOnlyTransaction = errors.New("cannot write in read-only transaction")
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

func notImplementedError(receiver interface{}, name string) error {
	return errors.Errorf("%T does not implemented: %s", receiver, name)
}
