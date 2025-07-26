package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	_ "modernc.org/sqlite"
)

var (
	db     *sql.DB
	dbOnce sync.Once
	dbErr  error
)

type Package struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func initDB() error {
	dbOnce.Do(func() {
		var err error
		db, err = sql.Open("sqlite", "rippkgs-index.sqlite")
		if err != nil {
			dbErr = fmt.Errorf("failed to open sqlite: %w", err)
			return
		}
		dbErr = db.Ping()
	})
	return dbErr
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`{"status":"ok"}`))
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "missing query ?q=", http.StatusBadRequest)
		return
	}

	query := `
        SELECT name, version FROM (
            SELECT name, version, 1 AS rank
            FROM packages
            WHERE name = ?

            UNION ALL

            SELECT name, version, 2 AS rank
            FROM packages
            WHERE name LIKE ? || '%'

            UNION ALL

            SELECT name, version, 3 AS rank
            FROM packages
            WHERE name LIKE '%' || ? || '%'
        )
        GROUP BY name, version, rank
        ORDER BY rank, name
        LIMIT 50;
    `

	rows, err := db.Query(query, q, q, q)
	if err != nil {
		http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []Package
	for rows.Next() {
		var n, v sql.NullString
		if err := rows.Scan(&n, &v); err != nil {
			http.Error(w, "scan error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		results = append(results, Package{
			Name:    n.String,
			Version: v.String,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"results": results})
}

func EntryPoint(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		healthHandler(w, r)
		return
	}

	if r.URL.Path == "/search" {
		searchHandler(w, r)
		return
	}

	http.NotFound(w, r)
}
