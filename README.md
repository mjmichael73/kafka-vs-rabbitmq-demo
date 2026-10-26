# Kafka vs RabbitMQ demo

This project publishes the same JSON event to Apache Kafka and RabbitMQ, reads one event back from each broker, and prints the results. It uses one local broker of each type to show the client flows. It is not a performance benchmark or a production deployment.

## Requirements

- Docker Engine with the Docker Compose plugin
- A terminal in this directory

The application dependencies are installed in the Docker image. Do not install the project requirements on the host.

## Run from a fresh checkout

Build the app image and start the demo:

```bash
make build
make run
```

Compose starts Python 3.15.1, Kafka 4.3.1, and RabbitMQ 4.4, waits for both broker health checks, and then runs the app container. Python.org lists 3.15.2 as the latest stable Python release, but the available official `python:3.15-slim` image resolved to 3.15.1 when this project was checked. The output contains a `Kafka received` line and a `RabbitMQ received` line with matching `event_id` values. Each run creates a new ID.

Run the project checks inside the app container:

```bash
make format
make lint
make test
```

The local brokers expose no host ports. The app reaches them on the Compose network. The RabbitMQ management interface is not needed for the demo.

## Troubleshooting

- Confirm Docker is running and `docker compose version` works.
- The first build downloads broker and Python images; allow time for that download.
- If a broker does not become healthy, inspect it with `docker compose logs kafka` or `docker compose logs rabbitmq`, then run `docker compose down` and retry.
- The demo uses one Kafka partition and a single RabbitMQ queue. Its settings are for a local demonstration, not availability or capacity testing.
