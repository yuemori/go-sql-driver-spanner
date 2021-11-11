package spannerdriver

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"database/sql"
	"database/sql/driver"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	instanceapi "cloud.google.com/go/spanner/admin/instance/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
)

var (
	_ driver.Driver        = &SpannerDriver{}
	_ driver.DriverContext = &SpannerDriver{}
)

var (
	dsn      string
	project  string
	instance string
	database string
)

func init() {
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	emulator_host := env("SPANNER_EMULATOR_HOST", "")
	if emulator_host == "" {
		panic("cannot setup spanner because env 'SPANNER_EMULATOR_HOST' is not set")
	}

	project = env("SPANNER_GCP_PROJECT", "sql-driver-spanner-project")
	instance = env("SPANNER_INSTANCE", "sql-driver-spanner-instance")
	database = env("SPANNER_DATABASE", "testdb")
	dsn = fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
}

var clientConfig = spanner.ClientConfig{
	SessionPoolConfig: spanner.SessionPoolConfig{
		MinOpened: 1,
		MaxOpened: 10, // FIXME: integration_test requires more than a single session
	},
}

type DBTest struct {
	*testing.T
	db     *sql.DB
	client *spanner.Client
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return rows
}

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatalf("error connecting database: %+v", err)
	}
	defer db.Close()
	dbt := &DBTest{db: db, T: t}

	deleteInstance(ctx, t)
	createInstance(ctx, t)
	createDatabase(ctx, t)

	for _, test := range tests {
		dropTable(ctx, t)
		createTable(ctx, t)
		test(dbt)
	}

	deleteInstance(ctx, t)
}

func createDatabase(ctx context.Context, t *testing.T) {
	client, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting instance: %+v", err)
	}
	defer client.Close()

	_, err = client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{
		Name: dsn,
	})
	if err == nil {
		return
	} else if status.Code(err) != codes.NotFound {
		t.Fatalf("error get database: %+v", err)
	}

	op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE %s", database),
	})
	if err != nil {
		t.Fatalf("error create database: %+v", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("error create database operation: %+v", err)
	}
}

func deleteInstance(ctx context.Context, t *testing.T) {
	client, err := instanceapi.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting instance: %+v", err)
	}
	defer client.Close()

	_, err = client.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: dsn,
	})
	if status.Code(err) == codes.NotFound {
		return
	} else if err != nil {
		t.Fatalf("error get instance: %+v", err)
	}

	err = client.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	})
	if err != nil {
		t.Fatalf("error delete instance: %+v", err)
	}
}

func createInstance(ctx context.Context, t *testing.T) {
	client, err := instanceapi.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting instance: %+v", err)
	}
	defer client.Close()

	_, err = client.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	})
	if err == nil {
		return
	} else if status.Code(err) != codes.NotFound {
		t.Fatalf("error get instance: %+v", err)
	}

	op, err := client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", project),
		InstanceId: instance,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("/projects/%s/instanceConfigs/test", project),
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		t.Fatalf("error create instance: %+v", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("error create instance operation: %+v", err)
	}
}

func createTable(ctx context.Context, t *testing.T) {
	client, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting database api: %+v", err)
	}
	defer client.Close()

	op, err := client.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dsn,
		Statements: []string{"CREATE TABLE test (`Id` STRING(36) NOT NULL, `Value` BOOL) PRIMARY KEY (`Id`)"},
	})
	if err != nil {
		t.Fatalf("error create table: %+v", err)
	}

	if err := op.Wait(ctx); err != nil {
		t.Fatalf("error create table operation: %+v", err)
	}
}

func dropTable(ctx context.Context, t *testing.T) {
	client, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting database api: %+v", err)
	}
	defer client.Close()

	op, err := client.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dsn,
		Statements: []string{"DROP TABLE test"},
	})
	if err != nil {
		t.Fatalf("error drop table: %+v", err)
	}

	if err := op.Wait(ctx); err != nil {
		t.Fatalf("error drop table operation: %+v", err)
	}
}

func TestCRUD(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		if rows.Next() {
			dbt.Error("unexpected data in empty table")
		}
		rows.Close()

		// Create Data
		res := dbt.mustExec("INSERT INTO test (Id, Value) VALUES (\"userId1\", true)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %+v", err)
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Create Data with params
		res = dbt.mustExec("INSERT INTO test (Id, Value) VALUES (@id, @value)", "userId2", false)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %+v", err)
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Read
		rows = dbt.mustQuery("SELECT value FROM test WHERE Id = @id", "userId1")
		if rows.Next() {
			rows.Scan(&out)
			if true != out {
				dbt.Errorf("true != %t", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Update
		res = dbt.mustExec("UPDATE test SET value = @value1 WHERE value = @value2", false, true)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check Update
		rows = dbt.mustQuery("SELECT value FROM test Where Id = \"userId1\"")
		if rows.Next() {
			rows.Scan(&out)
			if false != out {
				dbt.Errorf("false != %t", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE value = @value", false)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 2 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check for unexpected rows
		rows = dbt.mustQuery("SELECT * FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Fatalf("unexpected rows found: %v", out)
		}
	})
}

func TestInvalidQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		_, err := dbt.db.Query("this is invalid query")
		if err == nil {
			dbt.Fatal("db.Query does not returns expected error")
		}
	})
}

func TestReadOnlyTransaction(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx := context.Background()
		tx, err := dbt.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			dbt.Fatal(err)
		}

		defer tx.Rollback()

		if _, err := tx.QueryContext(ctx, "SELECT * FROM test"); err != nil {
			dbt.Errorf("expected nil, got %v", err)
		}

		if _, err := tx.ExecContext(ctx, "INSERT INTO test VALUES (true)"); err != ErrWriteInReadOnlyTransaction {
			dbt.Errorf("expected ErrWriteInReadOnlyTransaction, got %v", err)
		}
	})
}

func TestContextCancelBegin(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx, cancel := context.WithCancel(context.Background())
		conn, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatal(err)
		}
		defer conn.Close()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			dbt.Fatal(err)
		}

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(100*time.Millisecond, cancel).Stop()

		time.Sleep(200 * time.Millisecond)

		// This query will be canceled.
		startTime := time.Now()
		if _, err := tx.ExecContext(ctx, "INSERT INTO test VALUES (true)"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Transaction is canceled, so expect an error.
		switch err := tx.Commit(); err {
		case sql.ErrTxDone:
			// because the transaction has already been rollbacked.
			// the database/sql package watches ctx
			// and rollbacks when ctx is canceled.
		case context.Canceled:
			// the database/sql package rollbacks on another goroutine,
			// so the transaction may not be rollbacked depending on goroutine scheduling.
		default:
			dbt.Errorf("expected sql.ErrTxDone or context.Canceled, got %v", err)
		}

		// cannot begin a transaction (on a different conn) with a canceled context
		if _, err := dbt.db.BeginTx(ctx, nil); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}
