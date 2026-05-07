# Odin Game Server

Boom Beach-inspired learning project with an Odin TCP server and an Odin Raylib client.

## First Slice

- `shared/`: JSON protocol structs and helpers shared by client and server.
- `server/`: local TCP server that returns a deterministic world-map base list.
- `client/`: Raylib client that connects to the server and renders the world map.

## Commands

```sh
odin test shared -collection:game=.
odin build server -collection:game=. -out:bin/server
odin build client -collection:game=. -out:bin/client
```

Run locally in two terminals:

```sh
./bin/server
./bin/client
```

