.PHONY: up down ps logs shell-pg shell-ch

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

shell-pg:
	docker compose exec postgres psql -U demo -d oltp

shell-ch:
	docker compose exec clickhouse clickhouse-client
