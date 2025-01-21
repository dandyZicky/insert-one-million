package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// 1. worker pool
// 2. database connection pool
// 3. failover

var CSV_FILE string = "./majestic_million.csv"

var MAX_CORE int = 12

func main() {
	runtime.GOMAXPROCS(MAX_CORE)

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error reading .env file")
	}

	// dbService := os.Getenv("DB")
	// dbName := os.Getenv("DB_NAME")
	// dbUrl := os.Getenv("DB_URL")
	// dbPort := os.Getenv("DB_PORT")
	// user := os.Getenv("USER")
	// pass := os.Getenv("PASS")

	// connString := fmt.Sprintf("%s://%s:%s/%s?user=%s&password=%s", dbService, dbUrl, dbPort, dbName, user, pass)
	connString := os.Getenv("DB_CONN_STRING")

	file, err := os.Open(CSV_FILE)

	if err != nil {
		log.Fatal("Error reading csv")
	}

	defer file.Close()

	// preparing csv reader
	reader := csv.NewReader(file)
	record, err := reader.Read()

	// parse config from conn string
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatal("Error parsing connection string")
	}
	config.MaxConns = 50

	// prepare db driver
	// ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	// defer cancel() // Ensure resources are released
	//
	ctx := context.TODO()

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Fatal("Error creating connection pool")
	}

	defer pool.Close()

	err = pool.Ping(ctx)
	if err != nil {
		log.Fatalln("Cannot connect do db: ", err.Error())
	}

	var wg sync.WaitGroup

	// configure the limit of goroutine workers
	semaphore := make(chan struct{}, 50)
	start_time := time.Now()
	query := `INSERT INTO popular_website
	(global_rank, tld_rank, domain, tld, ref_subnets, ref_ips, idn_domain, idn_tld, prev_global_rank, prev_tld_rank, prev_ref_subnets, prev_ref_ips)
	VALUES `

	batch := make([][]string, 0, 499)
	batch_chan := make(chan [][]string)

	for {
		record, err = reader.Read()

		if record == nil {
			break
		}

		batch = append(batch, record)

		// fmt.Println(len(batch))
		if err != nil {
			log.Println(record)
			log.Fatal("Error reading file")
		}

		if len(batch) >= 500 {
			// fmt.Println("500 batch size reached")
			wg.Add(1)
			semaphore <- struct{}{}
			go func() {
				defer func() {
					<-semaphore
				}()
				defer wg.Done()

				conn, err := pool.Acquire(ctx)

				if err != nil {
					log.Fatalln("Error acquiring a pool: ", err.Error())
				}
				defer conn.Release()

				placeholders := []string{}
				args := []interface{}{}
				b := <-batch_chan
				for i, rec := range b {
					placeholder := fmt.Sprintf(
						"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
						i*12+1, i*12+2, i*12+3, i*12+4, i*12+5, i*12+6, i*12+7, i*12+8, i*12+9, i*12+10, i*12+11, i*12+12)
					placeholders = append(placeholders, placeholder)

					for _, value := range rec {
						args = append(args, value)
					}
				}
				batch_chan <- b[:0]

				execQuery := query + strings.Join(placeholders, ",")

				_, err = conn.Exec(ctx, execQuery, args...)
				conn.Release()
				if err != nil {
					log.Println(execQuery)
					log.Fatal(err.Error())
				}
				// time.Sleep(* time.Millisecond)
				// fmt.Println("\nQuery execution done\n")
			}()
			batch_chan <- batch
			// log.Println("Processing batch")
			batch = <-batch_chan
			// log.Println("Batch done, executing query")
		}
	}
	wg.Wait()

	elapsed_time := time.Now().UnixMilli() - start_time.UnixMilli()
	log.Println("Elapsed time in ms: ", elapsed_time)
}
