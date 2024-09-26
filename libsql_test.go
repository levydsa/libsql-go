package libsql

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"

	"gotest.tools/assert"
)

type DBType int

const (
	DB_MEMORY DBType = iota
	DB_LOCAL
	DB_REMOTE
	DB_EMBEDDED
)

func eachDatabase(t *testing.T, f func(*testing.T, *sql.DB, DBType)) {
	memoryDB, err := sql.Open("libsql", ":memory:")
	assert.NilError(t, err)

	localDB, err := sql.Open("libsql", "file:test.db")
	assert.NilError(t, err)

	remoteDB, err := sql.Open(
		"libsql",
		fmt.Sprintf(
			"%s?authToken=%s",
			os.Getenv("TURSO_URL"),
			os.Getenv("TURSO_AUTH_TOKEN"),
		),
	)
	assert.NilError(t, err)

	embeddedDB, err := sql.Open(
		"libsql",
		fmt.Sprintf(
			"%s?authToken=%s&path=test-embedded.db",
			os.Getenv("TURSO_URL"),
			os.Getenv("TURSO_AUTH_TOKEN"),
		),
	)
	assert.NilError(t, err)

	f(t, memoryDB, DB_MEMORY)
	f(t, localDB, DB_LOCAL)
	f(t, remoteDB, DB_REMOTE)
	f(t, embeddedDB, DB_EMBEDDED)
}

func TestQuery(t *testing.T) {
	eachDatabase(t, func(t *testing.T, db *sql.DB, dbType DBType) {
		_, err := db.Exec("drop table if exists test")
		assert.NilError(t, err)

		_, err = db.Exec("create table if not exists test (i integer, r real, t text, b blob)")
		assert.NilError(t, err)

		for i := 0; i < 100; i++ {
			_, err = db.Exec(
				"insert into test values (?, ?, ?, ?)",
				i,
				math.Exp(float64(i)/100),
				fmt.Sprint(i),
				[]byte{uint8(i)},
			)

			assert.NilError(t, err)
		}

		rows, err := db.Query("select * from test")

		for i := 0; i < 100; i++ {
			var (
				ri int64
				rr float64
				rt string
				rb []byte
			)

			rows.Next()
			rows.Scan(&ri, &rr, &rt, &rb)

			assert.Equal(t, ri, int64(i))

			{
				epsilon := 1e-9
				a, b := rr, math.Exp(float64(i)/100)

				diff := math.Abs(a - b)
				threshold := epsilon * (math.Abs(a) + math.Abs(b))

				if dbType == DB_REMOTE {
					assert.Assert(t, diff <= threshold)
				} else {
					assert.Equal(t, diff, float64(0))
				}
			}

			assert.Equal(t, rt, fmt.Sprint(i))
			assert.DeepEqual(t, rb, []byte{uint8(i)})
		}
	})
}
