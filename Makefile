.PHONY: up down logs db-check reset-db

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

db-check:
	python scripts/db_check.py

reset-db:
	docker compose down -v
	docker compose up -d

