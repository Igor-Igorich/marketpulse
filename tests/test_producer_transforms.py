import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.producer.live_producer import COLUMNS, row_to_trade
from src.producer.replay_producer import synthetic_trade_id


def test_row_to_trade_normalizes_buysell_to_full_word():
    row = [None] * len(COLUMNS)
    row[COLUMNS.index("TRADENO")] = 12345
    row[COLUMNS.index("SECID")] = "SBER"
    row[COLUMNS.index("BOARDID")] = "TQBR"
    row[COLUMNS.index("TRADEDATE")] = "2026-09-15"
    row[COLUMNS.index("TRADETIME")] = "10:00:00"
    row[COLUMNS.index("TRADE_SESSION_DATE")] = "2026-09-15"
    row[COLUMNS.index("PRICE")] = 287.5
    row[COLUMNS.index("QUANTITY")] = 10
    row[COLUMNS.index("VALUE")] = 2875.0
    row[COLUMNS.index("BUYSELL")] = "S"

    trade = row_to_trade(row, source="live")

    assert trade["side"] == "sell"
    assert trade["trade_id"] == 12345
    assert trade["trade_time"] == "2026-09-15T10:00:00"


def test_synthetic_trade_id_is_negative_and_deterministic():
    id_1 = synthetic_trade_id("SBER", "2026-09-15 10:00:00")
    id_2 = synthetic_trade_id("SBER", "2026-09-15 10:00:00")
    id_3 = synthetic_trade_id("GAZP", "2026-09-15 10:00:00")

    assert id_1 < 0  # реальные TRADENO всегда положительны
    assert id_1 == id_2  # без этого повторный реплей задвоил бы строки
    assert id_1 != id_3  # разные тикеры не пересекаются в ту же минуту
