# Trivia Battle — Multiplayer Game

Deployable source package for the real-time Trivia Battle game.

## Contents

- `server.py` — HTTP + WebSocket multiplayer backend
- `public/index.html` — frontend UI and game client
- `requirements.txt` — Python dependency pin
- `package.json` — convenience metadata/start command

## Game

- 2–8 players per room
- 6-character room code
- Host starts when at least 2 players are present
- Five synchronized trivia rounds
- 15-second answer timer
- One answer per player
- Server-side answer timing and speed-based scoring
- Round results and final leaderboard/winner

## Run locally

Python 3.10+ is recommended.

```bash
python -m pip install -r requirements.txt
python server.py
```

Then open `http://localhost:3000`.

The WebSocket endpoint is `/ws`; the health endpoint is `/health`.

## Deployment requirement

This application requires a deployment service that supports a continuously running Python WebSocket server. The frontend and WebSocket backend should be deployed together, or the frontend must be configured to connect to the public WebSocket endpoint.

For HTTPS deployments, the browser client automatically uses `wss://` for the WebSocket connection.

## Important provenance note

This package contains the available Trivia Battle source recovered in the current runtime. It is not a reconstruction from a remembered description of the game.
