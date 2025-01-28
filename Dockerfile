# Stage 1: Build the Go application
FROM golang:1.23.5-bookworm AS builder

# Install necessary packages for building
RUN apt update && apt install -y unzip

# Install DuckDB
RUN curl --fail --location --output duckdb_cli-linux-amd64.zip https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip

# Move the DuckDB binary to the /usr/local/bin directory
RUN mv duckdb /usr/local/bin/duckdb

# Set the working directory inside the container
WORKDIR /app

# Copy the source code
COPY ./src .

# Download dependencies
RUN go mod download

# Build the Go application for production
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -gcflags="-m" -o geocodeur main.go

# Stage 2: Create the final lightweight image
FROM debian:bookworm

# Set the environment variables
ENV GEOCODEUR_CONFIG_PATH="/config/geocodeur.conf"

# Copy the Go binary from the builder stage
COPY --from=builder /app/geocodeur /usr/local/bin/geocodeur

# Default port but geocodeur.conf can override it
EXPOSE 8080

# Command to run the server
CMD ["geocodeur", "server"]
