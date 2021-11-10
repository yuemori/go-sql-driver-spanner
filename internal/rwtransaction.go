package internal

import (
	"context"
	"errors"

	"cloud.google.com/go/spanner"
)

// RWConnector starts a Cloud Spanner read-write
// transaction and provides blocking APIs to
// query, exec, rollback and commit.
type RWConnector struct {
	QueryIn  chan *RWQueryMessage
	QueryOut chan *RWQueryMessage

	ExecIn  chan *RWExecMessage
	ExecOut chan *RWExecMessage

	RollbackIn chan struct{}
	CommitIn   chan struct{}
	Errors     chan error // only for starting, commit and rollback

	Ready chan struct{}
}

func NewRWConnector(ctx context.Context, c *spanner.Client) *RWConnector {
	connector := &RWConnector{
		QueryIn:    make(chan *RWQueryMessage),
		QueryOut:   make(chan *RWQueryMessage),
		ExecIn:     make(chan *RWExecMessage),
		ExecOut:    make(chan *RWExecMessage),
		RollbackIn: make(chan struct{}),
		CommitIn:   make(chan struct{}),
		Errors:     make(chan error),
		Ready:      make(chan struct{}),
	}

	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		connector.Ready <- struct{}{}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-connector.QueryIn:
				msg.It = tx.Query(msg.Ctx, msg.Stmt)
				connector.QueryOut <- msg
			case msg := <-connector.ExecIn:
				msg.Rows, msg.Error = tx.Update(msg.Ctx, msg.Stmt)
				connector.ExecOut <- msg
			case <-connector.RollbackIn:
				return ErrAborted
			case <-connector.CommitIn:
				return nil
			}
		}
	}
	go func() {
		_, err := c.ReadWriteTransaction(ctx, fn)
		connector.Errors <- err
	}()
	return connector
}

type RWQueryMessage struct {
	Ctx  context.Context   // in
	Stmt spanner.Statement // in

	It *spanner.RowIterator // out
}

type RWExecMessage struct {
	Ctx  context.Context   // in
	Stmt spanner.Statement // in

	Rows  int64 // out
	Error error // out
}

var ErrAborted = errors.New("aborted")
