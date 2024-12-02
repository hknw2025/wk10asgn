package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	_ "modernc.org/sqlite"
)

func main() {
	// Open/Create SQLite database
	db, err := sql.Open("sqlite", "./movie_data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Define table schemas
	createTableStatements := []string{
		`CREATE TABLE IF NOT EXISTS directors (
			id INTEGER PRIMARY KEY,
			first_name TEXT,
			last_name TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS directors_genres (
			director_id TEXT,
			genre TEXT,
			prob TEXT,
			FOREIGN KEY(director_id) REFERENCES directors(id)
		);`,
	}

	// Create tables
	for _, stmt := range createTableStatements {
		_, err := db.Exec(stmt)
		if err != nil {
			log.Fatalf("Failed to create table: %s", err)
		}
	}

	// Process CSV files
	csvFiles := []struct {
		Path      string
		TableName string
	}{
		{"IMDB-directors.csv", "directors"},
		{"IMDB-directors_genres.csv", "directors_genres"},
	}

	for _, csvFile := range csvFiles {
		fmt.Println("Processing", csvFile.Path)
		records, err := readCSV(csvFile.Path)
		if err != nil {
			log.Fatalf("Failed to read CSV: %s", err)
		}
		err = insertData(db, csvFile.TableName, records)
		if err != nil {
			log.Fatalf("Failed to insert data: %s", err)
		}
		fmt.Printf("Data inserted into %s successfully\n", csvFile.TableName)
	}
}

func readCSV(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return records, nil
}

func insertData(db *sql.DB, tableName string, records [][]string) error {
	// remove header
	records = records[1:]
	// Build a prepared statement with placeholders
	placeholder := ""
	for i := range records[0] {
		if i > 0 {
			placeholder += ", "
		}
		placeholder += "?"
	}

	// Prepare the insert statement
	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, placeholder))
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Insert each row of data
	for _, record := range records {
		// Convert []string to []any
		args := make([]any, len(record))
		for i, v := range record {
			args[i] = v
		}

		// Execute the statement with the converted arguments
		_, err := stmt.Exec(args...)
		if err != nil {
			return err
		}
	}

	return nil
}
