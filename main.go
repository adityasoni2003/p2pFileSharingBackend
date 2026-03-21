package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// -------------------- TYPES --------------------

type Message struct {
	Type      string          `json:"type"`
	Offer     json.RawMessage `json:"offer,omitempty"`
	Answer    json.RawMessage `json:"answer,omitempty"`
	Candidate json.RawMessage `json:"candidate,omitempty"`
}

// -------------------- GLOBALS --------------------

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var rooms = make(map[string]map[*websocket.Conn]bool)
var mu sync.Mutex

var sessionLimiter = NewRateLimiter(10, 60)
var wsLimiter = NewRateLimiter(20, 60)
var iceLimiter = NewRateLimiter(5, 60)

func enableCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")

	allowedOrigins := map[string]bool{
		"https://sharethat-green.vercel.app": true,
		"http://localhost:5173":              true,
	}

	if allowedOrigins[origin] {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}

	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
}

func iceHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w, r)

	ip := getIP(r)
	if !iceLimiter.Allow(ip) {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	apiKey := os.Getenv("METERED_API_KEY")
	if apiKey == "" {
		http.Error(w, "Missing API key", http.StatusInternalServerError)
		return
	}

	url := "https://p2pfileshare.metered.live/api/v1/turn/credentials?apiKey=" + apiKey

	resp, err := http.Get(url)
	if err != nil {
		http.Error(w, "Failed to fetch ICE servers", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Failed to read response", http.StatusInternalServerError)
		return
	}

	log.Println("ICE servers fetched")
	w.Write(body)
}

func createSession(w http.ResponseWriter, r *http.Request) {
	enableCORS(w, r)

	ip := getIP(r)
	if !sessionLimiter.Allow(ip) {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	id := uuid.New().String()

	mu.Lock()
	rooms[id] = make(map[*websocket.Conn]bool)
	mu.Unlock()

	log.Println("Session created:", id)

	json.NewEncoder(w).Encode(map[string]string{
		"sessionId": id,
	})
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	ip := getIP(r)
	if !wsLimiter.Allow(ip) {
		http.Error(w, "Too many connections", http.StatusTooManyRequests)
		return
	}

	sessionId := r.URL.Query().Get("sessionId")
	if sessionId == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	mu.Lock()

	if _, ok := rooms[sessionId]; !ok {
		rooms[sessionId] = make(map[*websocket.Conn]bool)
	}

	if len(rooms[sessionId]) >= 2 {
		mu.Unlock()
		conn.Close()
		http.Error(w, "Room full", http.StatusForbidden)
		return
	}

	rooms[sessionId][conn] = true
	count := len(rooms[sessionId])

	mu.Unlock()

	log.Println("Client connected to session:", sessionId)

	// ✅ READY signal
	if count == 2 {
		log.Println("Both clients connected → sending READY")

		mu.Lock()
		for client := range rooms[sessionId] {
			client.WriteMessage(websocket.TextMessage, []byte(`{"type":"ready"}`))
		}
		mu.Unlock()
	}

	// ---------------- CLEANUP ----------------

	defer func() {
		mu.Lock()
		delete(rooms[sessionId], conn)

		// 🧹 remove empty room
		if len(rooms[sessionId]) == 0 {
			delete(rooms, sessionId)
			log.Println("Room deleted:", sessionId)
		}

		mu.Unlock()

		conn.Close()
		log.Println("Client disconnected")
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			break
		}

		mu.Lock()
		for client := range rooms[sessionId] {
			if client != conn {
				err := client.WriteMessage(websocket.TextMessage, data)
				if err != nil {
					log.Println("Write error:", err)
					client.Close()
					delete(rooms[sessionId], client)
				}
			}
		}
		mu.Unlock()
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/session", createSession)
	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/ice", iceHandler)

	log.Println("Server running on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
