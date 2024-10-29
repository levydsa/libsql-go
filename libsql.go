//go:build cgo
// +build cgo

package libsql

/*
#cgo CFLAGS: -I${SRCDIR}/lib/
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/lib/x86_64-apple-darwin
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/lib/aarch64-apple-darwin
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/x86_64-unknown-linux-gnu
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/aarch64-unknown-linux-gnu
#cgo LDFLAGS: -llibsql
#cgo darwin LDFLAGS: -framework Security
#cgo darwin LDFLAGS: -framework CoreFoundation
#include <libsql.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
	"unsafe"
)

type Batchable interface {
	Batch(query string) error
}

type config struct {
	authToken      *string
	readYourWrites *bool
	encryptionKey  *string
	syncInterval   *time.Duration
}

type Option interface {
	apply(*config) error
}

type option func(*config) error

func (o option) apply(c *config) error {
	return o(c)
}

func WithAuthToken(authToken string) Option {
	return option(func(o *config) error {
		if o.authToken != nil {
			return fmt.Errorf("authToken already set")
		}
		if authToken == "" {
			return fmt.Errorf("authToken must not be empty")
		}
		o.authToken = &authToken
		return nil
	})
}

func WithReadYourWrites(readYourWrites bool) Option {
	return option(func(o *config) error {
		if o.readYourWrites != nil {
			return fmt.Errorf("read your writes already set")
		}
		o.readYourWrites = &readYourWrites
		return nil
	})
}

func WithEncryption(key string) Option {
	return option(func(o *config) error {
		if o.encryptionKey != nil {
			return fmt.Errorf("encryption key already set")
		}
		if key == "" {
			return fmt.Errorf("encryption key must not be empty")
		}
		o.encryptionKey = &key
		return nil
	})
}

func WithSyncInterval(interval time.Duration) Option {
	return option(func(o *config) error {
		if o.syncInterval != nil {
			return fmt.Errorf("sync interval already set")
		}
		o.syncInterval = &interval
		return nil
	})
}

func NewEmbeddedReplicaConnector(dbPath string, primaryUrl string, opts ...Option) (*Connector, error) {
	var config config
	errs := make([]error, 0, len(opts))
	for _, opt := range opts {
		if err := opt.apply(&config); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	authToken := ""
	if config.authToken != nil {
		authToken = *config.authToken
	}

	readYourWrites := true
	if config.readYourWrites != nil {
		readYourWrites = *config.readYourWrites
	}

	encryptionKey := ""
	if config.encryptionKey != nil {
		encryptionKey = *config.encryptionKey
	}

	syncInterval := time.Duration(0)
	if config.syncInterval != nil {
		syncInterval = *config.syncInterval
	}

	return NewConnector(ConnectorOptions{
		url:               primaryUrl,
		path:              dbPath,
		authToken:         authToken,
		encryptionKey:     encryptionKey,
		notReadYourWrites: !readYourWrites,
		syncInterval:      uint64(syncInterval.Milliseconds()),
	})
}

// goErr takes ownership of err, any references to err after calling goErr are invalid
func goErr(err *C.libsql_error_t) error {
	defer C.libsql_error_deinit(err)
	return errors.New(C.GoString(C.libsql_error_message(err)))
}

func cString(s string) (*C.char, func()) {
	if s == "" {
		return nil, func() {}
	}

	cs := C.CString(s)
	return cs, func() {
		C.free(unsafe.Pointer(cs))
	}
}

func init() {
	sql.Register("libsql", Driver{})
}

type Replicated struct {
	FrameNo      int
	FramesSynced int
}

type Driver struct{}

func (d Driver) Open(dbAddress string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dbAddress)
	if err != nil {
		return nil, err
	}

	return connector.Connect(context.Background())
}

func (d Driver) OpenConnector(dbAddress string) (driver.Connector, error) {
	if strings.TrimSpace(dbAddress) == ":memory:" {
		return NewConnector(ConnectorOptions{})
	}

	u, err := url.Parse(dbAddress)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "file":
		return NewConnector(ConnectorOptions{
			path: u.Opaque,
		})
	case "http", "https", "libsql":
		authToken := u.Query().Get("authToken")
		path := u.Query().Get("path")

		return NewConnector(ConnectorOptions{
			path:      path,
			url:       "libsql://" + u.Hostname(),
			authToken: authToken,
		})
	}

	return nil, fmt.Errorf("unsupported URL scheme: %s\nThis driver supports only URLs that start with libsql://, file:, https:// or http://", u.Scheme)
}

type ConnectorOptions struct {
	url               string
	path              string
	authToken         string
	encryptionKey     string
	syncInterval      uint64
	webpki            bool
	notReadYourWrites bool
}

type Connector struct {
	inner C.libsql_database_t
}

func NewConnector(opt ConnectorOptions) (*Connector, error) {
	path, free := cString(opt.path)
	defer free()

	url, free := cString(opt.url)
	defer free()

	authToken, free := cString(opt.authToken)
	defer free()

	// encryptionKey, free := cString(opt.encryptionKey)
	// defer free()

	db := C.libsql_database_init(C.libsql_database_desc_t{
		path:                 path,
		url:                  url,
		auth_token:           authToken,
		encryption_key:       nil, // TODO: not supported, for now
		not_read_your_writes: C.bool(opt.notReadYourWrites),
		webpki:               C.bool(opt.webpki),
		sync_interval:        C.uint64_t(opt.syncInterval),
	})
	if db.err != nil {
		return nil, goErr(db.err)
	}

	return &Connector{inner: db}, nil
}

func (c *Connector) Sync() (Replicated, error) {
	sync := C.libsql_database_sync(c.inner)
	if sync.err != nil {
		return Replicated{}, goErr(sync.err)
	}

	return Replicated{
		FrameNo:      int(sync.frame_no),
		FramesSynced: int(sync.frames_synced),
	}, nil

}

func (c *Connector) Close() error {
	if c.inner.inner == nil {
		return nil
	}

	C.libsql_database_deinit(c.inner)
	c.inner.inner = nil

	return nil
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn := C.libsql_database_connect(c.inner)
	if conn.err != nil {
		return nil, goErr(conn.err)
	}

	return &connection{inner: conn}, nil
}

func (c *Connector) Driver() driver.Driver {
	return Driver{}
}

type connection struct {
	inner          C.libsql_connection_t
	tx             C.libsql_transaction_t
	in_transaction bool
}

func (conn *connection) Batch(query string) error {
	cQuery, free := cString(query)
	defer free()

	var batch C.libsql_batch_t

	if conn.in_transaction {
		batch = C.libsql_transaction_batch(conn.tx, cQuery)
	} else {
		batch = C.libsql_connection_batch(conn.inner, cQuery)
	}

	if batch.err != nil {
		return goErr(batch.err)
	}

	return nil
}

func (conn *connection) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

func (conn *connection) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

func (conn *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, fmt.Errorf("read only transactions are not supported")
	}

	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, fmt.Errorf("isolation level %d is not supported", opts.Isolation)
	}

	conn.in_transaction = true
	conn.tx = C.libsql_connection_transaction(conn.inner)
	if conn.tx.err != nil {
		return nil, goErr(conn.tx.err)
	}

	return &transaction{conn: conn}, nil
}

func (conn *connection) Close() error {
	C.libsql_connection_deinit(conn.inner)
	return nil
}

func (conn *connection) PrepareContext(ctx context.Context, queryString string) (driver.Stmt, error) {
	query := C.CString(queryString)
	defer C.free(unsafe.Pointer(query))

	stmt := C.libsql_connection_prepare(conn.inner, query)
	if stmt.err != nil {
		return nil, goErr(stmt.err)
	}

	return &statement{inner: stmt}, nil
}

type statement struct {
	inner C.libsql_statement_t
}

func (stmt *statement) Close() error {
	if stmt.inner.inner == nil {
		return nil
	}

	C.libsql_statement_deinit(stmt.inner)
	stmt.inner.inner = nil

	return nil
}

func (stmt *statement) NumInput() int {
	return -1
}

func (stmt *statement) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))

	for i := range named {
		named[i] = driver.NamedValue{Ordinal: i, Value: args[i]}
	}

	return stmt.ExecContext(context.Background(), named)
}

func (stmt *statement) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))

	for i := range named {
		named[i] = driver.NamedValue{Ordinal: i, Value: args[i]}
	}

	return stmt.QueryContext(context.Background(), named)
}

func (stmt *statement) bindSingle(arg driver.NamedValue, toValue func(any) C.libsql_value_t) error {
	if arg.Name == "" {
		bind := C.libsql_statement_bind_value(stmt.inner, toValue(arg.Value))
		if bind.err != nil {
			return goErr(bind.err)
		}
	} else {
		name := C.CString(arg.Name)
		defer C.free(unsafe.Pointer(name))

		bind := C.libsql_statement_bind_named(stmt.inner, name, toValue(arg.Value))
		if bind.err != nil {
			return goErr(bind.err)
		}
	}

	return nil
}

func (stmt *statement) Bind(args []driver.NamedValue) error {
	// TODO: Be more resilient to unordered positional arguments.
	for _, arg := range args {
		switch arg.Value.(type) {
		case bool:
			err := stmt.bindSingle(arg, func(a any) C.libsql_value_t {
				v := 0
				if a.(bool) {
					v = 1
				}
				return C.libsql_integer(C.int64_t(v))
			})
			if err != nil {
				return err
			}
		case int64:
			err := stmt.bindSingle(arg, func(a any) C.libsql_value_t {
				return C.libsql_integer(C.int64_t(a.(int64)))
			})
			if err != nil {
				return err
			}
		case float64:
			err := stmt.bindSingle(arg, func(a any) C.libsql_value_t {
				return C.libsql_real(C.double(a.(float64)))
			})

			if err != nil {
				return err
			}
		case time.Time:
			valueString := arg.Value.(time.Time).Format(time.RFC3339Nano)
			value := C.CString(valueString)
			defer C.free(unsafe.Pointer(value))

			err := stmt.bindSingle(arg, func(a any) C.libsql_value_t {
				return C.libsql_text(value, C.ulong(len(valueString)))
			})
			if err != nil {
				return err
			}
		case string:
			valueString := arg.Value.(string)
			value := C.CString(valueString)
			defer C.free(unsafe.Pointer(value))

			err := stmt.bindSingle(arg, func(a any) C.libsql_value_t {
				return C.libsql_text(value, C.size_t(len(valueString)))
			})
			if err != nil {
				return err
			}
		case []byte:
			valueBytes := arg.Value.([]byte)
			value := C.CBytes(valueBytes)
			defer C.free(unsafe.Pointer(value))

			err := stmt.bindSingle(arg, func(a any) C.libsql_value_t {
				return C.libsql_blob((*C.uchar)(value), C.size_t(len(valueBytes)))
			})
			if err != nil {
				return err
			}
		case nil:
			err := stmt.bindSingle(arg, func(a any) C.libsql_value_t {
				return C.libsql_null()
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (stmt *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	err := stmt.Bind(args)
	if err != nil {
		return nil, err
	}

	exec := C.libsql_statement_execute(stmt.inner)
	if exec.err != nil {
		return nil, goErr(exec.err)
	}

	return &result{rowsAffected: int64(exec.rows_changed)}, nil
}

func (stmt *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	err := stmt.Bind(args)
	if err != nil {
		return nil, err
	}

	rows := C.libsql_statement_query(stmt.inner)
	if rows.err != nil {
		return nil, goErr(rows.err)
	}

	return &Rows{inner: rows}, nil
}

type Rows struct {
	inner C.libsql_rows_t
}

func fromValue(v C.libsql_value_t) driver.Value {
	switch v._type {
	case C.LIBSQL_TYPE_INTEGER:
		return int64(*(*C.int64_t)(unsafe.Pointer(&v.value[0])))
	case C.LIBSQL_TYPE_REAL:
		return float64(*(*C.double)(unsafe.Pointer(&v.value[0])))
	case C.LIBSQL_TYPE_TEXT:
		slice := *(*C.libsql_slice_t)(unsafe.Pointer(&v.value[0]))
		defer C.libsql_slice_deinit(slice)

		str := C.GoString((*C.char)(slice.ptr))

		{
			str := strings.TrimSuffix(str, "Z")
			for _, format := range []string{
				"2006-01-02 15:04:05.999999999-07:00",
				"2006-01-02T15:04:05.999999999-07:00",
				"2006-01-02 15:04:05.999999999",
				"2006-01-02T15:04:05.999999999",
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05",
				"2006-01-02 15:04",
				"2006-01-02T15:04",
				"2006-01-02",
			} {
				if t, err := time.ParseInLocation(format, str, time.UTC); err == nil {
					return t
				}
			}
		}

		return str
	case C.LIBSQL_TYPE_BLOB:
		slice := *(*C.libsql_slice_t)(unsafe.Pointer(&v.value[0]))
		defer C.libsql_slice_deinit(slice)

		return C.GoBytes(slice.ptr, C.int(slice.len))
	case C.LIBSQL_TYPE_NULL:
		return nil
	}

	panic("unreachable")
}

func (rows *Rows) Close() error {
	C.libsql_rows_deinit(rows.inner)
	return nil
}

func (rows *Rows) Columns() []string {
	columns := make([]string, C.libsql_rows_column_length(rows.inner))

	for i := range columns {
		name := C.libsql_rows_column_name(rows.inner, C.int(i))
		defer C.libsql_slice_deinit(name)

		columns[i] = C.GoString((*C.char)(name.ptr))
	}

	return columns
}

func (rows *Rows) Next(dest []driver.Value) error {
	row := C.libsql_rows_next(rows.inner)

	if row.err != nil {
		return goErr(row.err)
	}

	if C.libsql_row_empty(row) {
		return io.EOF
	}

	for i := range dest {
		result := C.libsql_row_value(row, C.int(i))

		if result.err != nil {
			return goErr(result.err)
		}

		dest[i] = fromValue(result.ok)
	}

	return nil
}

type result struct {
	rowsAffected int64
}

func (res *result) LastInsertId() (int64, error) {
	return -1, nil
}

func (res *result) RowsAffected() (int64, error) {
	return res.rowsAffected, nil
}

type transaction struct {
	conn *connection
}

func (t *transaction) Commit() error {
	if !t.conn.in_transaction {
		return errors.New("Not inside a transaction")
	}

	C.libsql_transaction_commit(t.conn.tx)
	t.conn.tx = C.libsql_transaction_t{}
	t.conn.in_transaction = false
	return nil
}

func (t *transaction) Rollback() error {
	if !t.conn.in_transaction {
		return errors.New("Not inside a transaction")
	}

	C.libsql_transaction_rollback(t.conn.tx)
	t.conn.tx = C.libsql_transaction_t{}
	t.conn.in_transaction = false
	return nil
}
