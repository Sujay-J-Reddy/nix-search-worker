package pkgsearch

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	_ "modernc.org/sqlite"
)

var (
	db          *sql.DB
	gcsBucket   = "nixery-bucket"
	gcsObject   = "packages/rippkgs-index.sqlite"
	localDBPath = "/tmp/rippkgs-index.sqlite"
)

type Package struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func downloadDBFromGCS() error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("GCS client error: %w", err)
	}
	defer client.Close()

	rc, err := client.Bucket(gcsBucket).Object(gcsObject).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to read from GCS: %w", err)
	}
	defer rc.Close()

	f, err := os.Create(localDBPath)
	if err != nil {
		return fmt.Errorf("failed to create local DB file: %w", err)
	}
	defer f.Close()

	_, err = io.Copy(f, rc)
	if err != nil {
		return fmt.Errorf("failed to copy DB: %w", err)
	}

	log.Println("Downloaded DB from GCS → /tmp/")
	return nil
}

func initDB() error {
	if err := downloadDBFromGCS(); err != nil {
		return err
	}

	var err error
	db, err = sql.Open("sqlite", localDBPath+"?mode=ro&_busy_timeout=5000&_journal_mode=OFF")
	if err != nil {
		return fmt.Errorf("failed to open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db.Ping()
	// var err error
	// db, err = sql.Open("sqlite", "rippkgs-index.sqlite")
	// if err != nil {
	// 	return fmt.Errorf("failed to open sqlite: %w", err)
	// }
	// return db.Ping()
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
	if db == nil {
		if err := initDB(); err != nil {
			http.Error(w, "DB init failed: "+err.Error(), 500)
			return
		}
		log.Println("DB initialized successfully")
	}

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
