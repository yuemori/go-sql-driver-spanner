package spannerdriver

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/yuemori/go-sql-driver-spanner/internal"
)

type roTx struct {
	close func()
}

func (tx *roTx) Commit() error {
	tx.close()
	return nil
}

func (tx *roTx) Rollback() error {
	tx.close()
	return nil
}

type rwTx struct {
	connector *internal.RWConnector
	close     func()
}

func (tx *rwTx) Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator {
	tx.connector.QueryIn <- &internal.RWQueryMessage{
		Ctx:  ctx,
		Stmt: stmt,
	}
	msg := <-tx.connector.QueryOut
	return msg.It
}

func (tx *rwTx) ExecContext(ctx context.Context, stmt spanner.Statement) (int64, error) {
	tx.connector.ExecIn <- &internal.RWExecMessage{
		Ctx:  ctx,
		Stmt: stmt,
	}
	msg := <-tx.connector.ExecOut
	return msg.Rows, msg.Error
}

func (tx *rwTx) Commit() error {
	tx.connector.CommitIn <- struct{}{}
	err := <-tx.connector.Errors
	if err == nil {
		tx.close()
	}
	return err
}

func (tx *rwTx) Rollback() error {
	tx.connector.RollbackIn <- struct{}{}
	err := <-tx.connector.Errors
	if err == internal.ErrAborted {
		tx.close()
		return nil
	}
	return err
}
