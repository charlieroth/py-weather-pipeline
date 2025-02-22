# Check to see if we can use ash, in Alpine images, or default to BASH.
SHELL_PATH = /bin/ash
SHELL = $(if $(wildcard $(SHELL_PATH)),/bin/ash,/bin/bash)

# ==============================================================================
# Define dependencies

ALPINE   := alpine:3.21
POSTGRES := postgres:17.3

# ==============================================================================
# Install dependencies

dev-brew:
	brew update
	brew list uv || brew install uv
	brew list pgcli || brew install pgcli
	brew list watch || brew install watch

dev-docker:
	docker pull $(ALPINE) & \
	docker pull $(POSTGRES) & \
	wait;

# ==============================================================================
# Docker Compose

compose-up:
	cd ./zarf/compose/ && docker compose -f docker-compose.yaml -p compose up -d

compose-build-up: build compose-up

compose-down:
	cd ./zarf/compose/ && docker compose -f docker-compose.yaml down

compose-logs:
	cd ./zarf/compose/ && docker compose -f docker-compose.yaml logs

# ==============================================================================
# Administration

# migrate:

pgcli:
	pgcli postgresql://postgres:postgres@localhost:5432/weather