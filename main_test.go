package main

import (
	"database/sql"
	"log"
	"testing"

	_ "modernc.org/sqlite"
)

func TestMain(m *testing.M) {
	// Connect to the database
	db, err := sql.Open("sqlite", "./movie_data.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// SQL query to join movies and roles
	query := `
		SELECT directors.id, directors.first_name, directors.last_name, directors_genres.genre, directors_genres.prob 
		FROM directors
		JOIN directors_genres ON directors.id = directors_genres.director_id;
	`

	// Execute the query
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	// Iterate over the rows
	for rows.Next() {
		var id int
		var firstName, lastName, genre, prob string
		err := rows.Scan(&id, &firstName, &lastName, &genre, &prob)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		log.Printf("ID: %d, Name: %s %s, Genre: %s, Probability: %s", id, firstName, lastName, genre, prob)
	}
}

func TestReadCSV(t *testing.T) {
	_, err := readCSV("IMDB-directors.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %s", err)
	}
}

func TestInsertData(t *testing.T) {
	db, err := sql.Open("sqlite", "./test_movie_data.db")
	if err != nil {
		t.Fatalf("Failed to open database: %s", err)
	}
	defer db.Close()

	records := [][]string{
		{"id", "first_name", "last_name"},
		{"1", "John", "Doe"},
	}

	err = insertData(db, "directors", records)
	if err != nil {
		t.Fatalf("Failed to insert data: %s", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM directors").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query data: %s", err)
	}

	if count != 1 {
		t.Fatalf("Expected 1 row, got %d", count)
	}
}
