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

type Message struct {
	Type      string          `json:"type"`
	Offer     json.RawMessage `json:"offer,omitempty"`
	Answer    json.RawMessage `json:"answer,omitempty"`
	Candidate json.RawMessage `json:"candidate,omitempty"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// sessionId -> clients
var rooms = make(map[string]map[*websocket.Conn]bool)
var mu sync.Mutex

func enableCORS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Handle preflight request
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
}

func iceHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w, r)

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

	// Optional: log for debugging
	log.Println("ICE servers fetched")

	// ✅ Return same response to frontend
	w.Write(body)
}

// 🔹 Create session
func createSession(w http.ResponseWriter, r *http.Request) {
	enableCORS(w, r)
	id := uuid.New().String()

	mu.Lock()
	rooms[id] = make(map[*websocket.Conn]bool)
	mu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{
		"sessionId": id,
	})
}

// 🔹 WebSocket handler
func handleWS(w http.ResponseWriter, r *http.Request) {
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
	rooms[sessionId][conn] = true
	mu.Unlock()

	// ✅ 🔥 ADD READY LOGIC HERE
	if len(rooms[sessionId]) == 2 {
		log.Println("Both clients connected → sending READY")

		for client := range rooms[sessionId] {
			client.WriteMessage(websocket.TextMessage, []byte(`{"type":"ready"}`))
		}
	}

	log.Println("Client connected to session:", sessionId)

	defer func() {
		mu.Lock()
		delete(rooms[sessionId], conn)
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
				client.WriteMessage(websocket.TextMessage, data)
			}
		}
		mu.Unlock()
	}
}

func main() {
	http.HandleFunc("/session", createSession)
	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/ice", iceHandler)

	log.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
