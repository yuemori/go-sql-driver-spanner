package spannerdriver

import (
	"context"
)

type rwTx struct {
	conn  *spannerConn
	ctx   context.Context
	close func()
}

type roTx struct {
	conn  *spannerConn
	ctx   context.Context
	close func()
}

func (tx *rwTx) Commit() (err error) {
	if tx.conn == nil || tx.conn.rwTx == nil || tx.conn.closed.IsSet() {
		return ErrInvalidConn
	}
	_, err = tx.conn.rwTx.Commit(tx.ctx)
	tx.conn = nil
	return
}

func (tx *rwTx) Rollback() (err error) {
	if tx.conn == nil || tx.conn.rwTx == nil || tx.conn.closed.IsSet() {
		return ErrInvalidConn
	}
	tx.conn.rwTx.Rollback(tx.ctx)
	tx.conn = nil
	return
}

func (tx *roTx) Commit() (err error) {
	if tx.conn == nil || tx.conn.roTx == nil || tx.conn.closed.IsSet() {
		return ErrInvalidConn
	}
	tx.conn.roTx.Close()
	tx.conn = nil
	return
}

func (tx *roTx) Rollback() (err error) {
	if tx.conn == nil || tx.conn.roTx == nil || tx.conn.closed.IsSet() {
		return ErrInvalidConn
	}
	tx.conn.roTx.Close()
	tx.conn = nil
	return
}
