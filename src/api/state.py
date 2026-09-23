"""Общее состояние приложения — вынесено отдельно, чтобы main.py и
роутеры могли вместе ссылаться на него без циклического импорта."""

import asyncpg

pool: asyncpg.Pool | None = None
models: dict[str, dict] = {}
