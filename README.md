# 🚀 ShareThat Backend

Backend service for **ShareThat** — a peer-to-peer file sharing application built using WebRTC.

👉 Frontend Repo: https://github.com/adityasoni2003/p2pFileSharingFrontend

---

## 🧠 Overview

This backend acts as a **signaling server** for establishing WebRTC connections between peers.

It does **NOT store any files**.  
Its only responsibility is to:
- Create sessions
- Manage rooms
- Relay signaling messages (offer, answer, ICE candidates)

---

## ⚙️ Tech Stack

- **Go (Golang)**
- **Gorilla WebSocket**
- **WebRTC Signaling**
- **STUN/TURN (via Metered API)**

