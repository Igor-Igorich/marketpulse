.PHONY: up down test test-integration train replay logs

up:
	docker compose up -d --build

down:
	docker compose down -v

test:
	pytest tests/test_producer_transforms.py tests/test_ml_split.py tests/test_api_mocked.py -v

test-integration:
	docker compose up -d postgres
	pytest tests/test_consumer_idempotency.py -v

train:
	python -m src.ml.train

# make replay DATE=2026-09-15
replay:
	docker compose run --rm live-producer python -m src.producer.replay_producer $(DATE)

logs:
	docker compose logs -f consumer