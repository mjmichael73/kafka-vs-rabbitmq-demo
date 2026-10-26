.PHONY: build format lint test run

build:
	docker compose build

format:
	docker compose run --rm app ruff format .

lint:
	docker compose run --rm app ruff check .

test:
	docker compose run --rm app pytest tests/ -v

run:
	docker compose run --rm app python -m compare_demo
