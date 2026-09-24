# Стадия 1: сборка зависимостей
FROM python:3.11-slim AS builder

WORKDIR /app

# gcc — защитная мера: большинство наших зависимостей ставятся из готовых
# wheel-пакетов и компилятор не нужен, но если какой-то транзитивный пакет
# всё же потребует сборки из исходников, то не хотим падать на этом.
# Ничего не стоит финальному образу — этот слой выбрасывается на Стадии 2.

RUN apt-get update && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Стадия 2: рантайм
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app
ENV PATH=/root/.local/bin:$PATH

COPY --from=builder /root/.local /root/.local
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY sql/ ./sql/

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]