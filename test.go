package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		username := r.URL.Query().Get("username")
		password := r.URL.Query().Get("password")

		// SQL Injection vulnerability
		db, err := sql.Open("sqlite3", "./example.db")
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		var user string
		err = db.QueryRow("SELECT username FROM users WHERE username = '" + username + "' AND password = '" + password + "'").Scan(&user)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Command Injection vulnerability
		cmd := fmt.Sprintf("echo 'Hello, %s!'", username)
		output, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%s", output)

		// Path Traversal vulnerability
		filePath := r.URL.Query().Get("file")
		if filePath != "" {
			http.ServeFile(w, r, "./"+filePath)
		}
	})

	// Cross-Site Scripting (XSS) vulnerability
	http.HandleFunc("/xss", func(w http.ResponseWriter, r *http.Request) {
		input := r.URL.Query().Get("input")
		fmt.Fprintf(w, "<h1>%s</h1>", input)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
