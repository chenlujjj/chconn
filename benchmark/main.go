package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/vahid-sohrabloo/chconn/v2/chpool"
	"github.com/vahid-sohrabloo/chconn/v2/column"
	"github.com/vahid-sohrabloo/chconn/v2/types"
)

func main() {
	conn, err := chpool.New(os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	// to check if the connection is alive
	err = conn.Ping(context.Background())
	if err != nil {
		panic(err)
	}

	err = conn.Exec(context.Background(), `DROP TABLE IF EXISTS example_table`)
	if err != nil {
		panic(err)
	}

	err = conn.Exec(context.Background(), `CREATE TABLE example_table (
		Col1 UInt64
	  , Col2 String
	  , Col3 Array(UInt8)
	  , Col4 DateTime
  ) Engine Null`)
	if err != nil {
		panic(err)
	}

	col1 := column.New[uint64]()
	col2 := column.NewString()
	col3 := column.New[uint8]().Array()
	col4 := column.New[types.DateTime]()
	rows := 1_000_000
	col1.SetWriteBufferSize(rows)
	col2.SetWriteBufferSize(rows)
	col3.SetWriteBufferSize(rows)
	col4.SetWriteBufferSize(rows)

	startInsert := time.Now()
	for y := 0; y < rows; y++ {
		col1.Append(uint64(y))
		col2.Append("Golang SQL database driver")
		col3.Append([]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9})
		col4.Append(types.TimeToDateTime(time.Now()))
	}

	ctxInsert, cancelInsert := context.WithTimeout(context.Background(), time.Second*30)
	// insert data
	err = conn.Insert(ctxInsert, "INSERT INTO example_table (Col1,Col2,Col3,Col4) VALUES", col1, col2, col3, col4)
	if err != nil {
		cancelInsert()
		panic(err)
	}
	cancelInsert()
	fmt.Println("inserted 1M rows in ", time.Since(startInsert))
}
