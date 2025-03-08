#!/bin/bash

DATABASE="dicedb"
HOST="localhost"
PORT="7379"
NUM_REQUESTS="100000"
NUM_CLIENTS="4"

read -p "Enter database name [$DATABASE] (dicedb, redis): " input
DATABASE=${input:-$DATABASE}

read -p "Enter host [$HOST] (localhost, 127.0.0.1): " input
HOST=${input:-$HOST}

read -p "Enter port [$PORT] (7379, 6379): " input
PORT=${input:-$PORT}

read -p "Enter number of requests [$NUM_REQUESTS] (100000, 1000000): " input
NUM_REQUESTS=${input:-$NUM_REQUESTS}

read -p "Enter number of clients [$NUM_CLIENTS] (1, 2, 4, 8, 16, 32): " input
NUM_CLIENTS=${input:-$NUM_CLIENTS}

go run main.go benchmark \
    --database "$DATABASE" \
    --host "$HOST" \
    --port "$PORT" \
    --num-requests "$NUM_REQUESTS" \
    --num-clients "$NUM_CLIENTS"
