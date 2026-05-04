# Live Location Tracker

A real-time live location tracking system where authenticated users can share their current location and see other users moving on a map. Built with a focus on high-throughput architecture using Kafka and WebSockets.

## 🎥 Demo Video
[Youtube Video Link](https://youtu.be/-ywoyygui6M)

## 🚀 Tech Stack
- **Backend**: Node.js, Express, Socket.IO
- **Event Streaming**: Kafka (KafkaJS)
- **Authentication**: OIDC / OAuth 2.0 (Custom Provider)
- **Frontend**: HTML/JS, Leaflet.js (Maps)
- **Containerization**: Docker (for Kafka)

## 🛠️ Architecture & Event Flow
This project uses a decoupled event-driven architecture to handle high volumes of location updates efficiently:

1.  **Authentication**: Users must log in via the OIDC provider. The session is shared between HTTP and WebSockets.
2.  **Location Update**: The browser polls geolocation every 10s. If the coordinates change, it sends them via **Socket.IO**.
3.  **Ingestion**: The Socket server acts as a **Kafka Producer**, pushing updates to the `location-updates` topic.
4.  **Fan-out**:
    *   **Broadcasting**: A Kafka Consumer in the main server receives the update and broadcasts it to all connected Socket.IO clients.
    *   **Persistence**: A separate `database-processor.js` (Consumer Group) listens to the same stream to simulate database logging without blocking the real-time broadcast.
5.  **Frontend**: The Leaflet map updates markers in real-time. Markers are tied to authenticated **User IDs**, ensuring persistence even across refreshes.
6.  **Cleanup**: A 2-minute inactivity timeout automatically removes stale users from the map.

## ⚙️ Setup Instructions

### 1. Prerequisites
- Node.js (v18+)
- Docker & Docker Compose (for Kafka)

### 2. Environment Variables
Create a `.env` file in the root based on `.env.example`:
```env
PORT=8000
CLIENT_ID=your_client_id
CLIENT_SECRET=your_client_secret
ISSUER_URL=https://custom-oidc-provider.onrender.com
REDIRECT_URI=http://localhost:8000/callback
SESSION_SECRET=your_random_secret
KAFKA_BROKERS=localhost:9092
```

### 3. Run Kafka
```bash
docker-compose up -d
```

### 4. Install Dependencies
```bash
pnpm install
```

### 5. Initialize Kafka Topic
```bash
node kafka-admin.js
```

### 6. Start the Application
Terminal 1 (Server):
```bash
node index.js
```

Terminal 2 (DB Processor):
```bash
node database-processor.js
```

## 📝 Assumptions & Limitations
- The OIDC provider is assumed to be accessible at the provided URL.
- Database persistence is currently simulated via console logging in a separate consumer group to demonstrate architectural decoupling.
- High accuracy geolocation is requested from the browser; performance may vary based on device hardware.
