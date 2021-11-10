package spannerdriver

import (
	"database/sql/driver"
	"encoding/base64"
	"io"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

type spannerRows struct {
	it *spanner.RowIterator

	colsOnce sync.Once
	cols     []string

	dirtyRow *spanner.Row
}

func (r *spannerRows) getColumns() {
	r.colsOnce.Do(func() {
		row, err := r.it.Next()
		if err == iterator.Done {
			return
		} else if err != nil {
			log.Printf("error: error from *spanner.RowIterator %+v", err)
			return
		}
		r.dirtyRow = row
		r.cols = row.ColumnNames()
	})
}

// Columns implements database/sql/driver.Rows interface.
func (r *spannerRows) Columns() []string {
	r.getColumns()
	return r.cols
}

// Close implements database/sql/driver.Rows interface.
func (r *spannerRows) Close() error {
	r.it.Stop()
	return nil
}

// Next implements database/sql/driver.Rows interface.
func (r *spannerRows) Next(dest []driver.Value) error {
	r.getColumns()
	var row *spanner.Row
	if r.dirtyRow != nil {
		row = r.dirtyRow
		r.dirtyRow = nil
	} else {
		var err error
		row, err = r.it.Next() // returns io.EOF when there is no next
		if err == iterator.Done {
			return io.EOF
		}
		if err != nil {
			return err
		}
	}

	for i := 0; i < row.Size(); i++ {
		var col spanner.GenericColumnValue
		if err := row.Column(i, &col); err != nil {
			return err
		}
		switch col.Type.Code {
		case sppb.TypeCode_INT64:
			var v spanner.NullInt64
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Int64
		case sppb.TypeCode_FLOAT64:
			var v spanner.NullFloat64
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Float64
		case sppb.TypeCode_STRING:
			var v spanner.NullString
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.StringVal
		case sppb.TypeCode_BYTES:
			// The column value is a base64 encoded string.
			var v spanner.NullString
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.IsNull() {
				dest[i] = []byte(nil)
			} else {
				b, err := base64.StdEncoding.DecodeString(v.StringVal)
				if err != nil {
					return err
				}
				dest[i] = b
			}
		case sppb.TypeCode_BOOL:
			var v spanner.NullBool
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Bool
		case sppb.TypeCode_DATE:
			var v spanner.NullDate
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.IsNull() {
				dest[i] = v.Date // typed nil
			} else {
				dest[i] = v.Date.In(time.Local) // TODO(jbd): Add note about this.
			}
		case sppb.TypeCode_TIMESTAMP:
			var v spanner.NullTime
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Time
		}
		// TODO(jbd): Implement other types.
		// How to handle array and struct?
	}
	return nil
}

// // ColumnTypeDatabaseTypeName implements database/sql/driver.RowsColumnTypeDatabaseTypeName interface.
// func (r *spannerRows) ColumnTypeDatabaseTypeName(index int) string {
// 	return ""
// }
//
// // ColumnTypeLength implements database/sql/driver.RowsColumnTypeLength interface.
// func (r *spannerRows) ColumnTypeLength(index int) (length int64, ok bool) {
// 	return int64(0), false
// }
//
// // ColumnTypeNullable implements database/sql/driver.RowsColumnTypeNullable interface.
// func (r *spannerRows) ColumnTypeNullable(index int) (nullable, ok bool) {
// 	return false, false
// }
//
// // ColumnTypePrecisionScale implements database/sql/driver.RowsColumnTypePrecisionScale interface.
// func (r *spannerRows) ColumnTypePrecisionScale(index int) (presicion, scale int64, ok bool) {
// 	return int64(0), int64(0), false
// }
//
// // ColumnTypeScanType implements database/sql/driver.RowsColumnTypeScanType interface.
// func (r *spannerRows) ColumnTypeScanType(index int) reflect.Type {
// 	return reflect.TypeOf("")
// }
