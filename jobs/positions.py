"""Example jobs: raw trades -> curated positions and curated trades, behind @governed_job."""
from datetime import date, datetime, timedelta

from pyspark.sql import functions as F

from lab.compliance import erase, hash_pii, pii_hash
from lab.decorators import audited
from lab.governance import governed_job

TRADE_COLS = ["trade_id", "account_id", "client_name", "ticker", "qty", "price", "trade_ts"]


def create_tables(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS raw")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS curated")
    props = "TBLPROPERTIES ('format-version'='2', 'write.wap.enabled'='true')"
    # Hidden partitioning: files are grouped by day of trade_ts, but readers just
    # filter on trade_ts. There is no separate trade_date column to keep in sync.
    spark.sql(f"""CREATE TABLE IF NOT EXISTS raw.trades (
        trade_id BIGINT, account_id STRING, client_name STRING, ticker STRING,
        qty INT, price DOUBLE, trade_ts TIMESTAMP)
        USING iceberg PARTITIONED BY (days(trade_ts)) {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS curated.positions (
        account_id STRING, ticker STRING, qty BIGINT, market_value DOUBLE)
        USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS curated.trades (
        trade_id BIGINT, account_id STRING, client_name_sha256 STRING, ticker STRING,
        qty INT, price DOUBLE, trade_ts TIMESTAMP)
        USING iceberg PARTITIONED BY (days(trade_ts)) {props}""")


def load_trades(spark, rows: list[tuple], trade_ts: datetime | None = None) -> None:
    """Append trades. 6-tuples get `trade_ts` (default: now); 7-tuples carry their own."""
    ts = trade_ts or datetime.now()
    rows = [r if len(r) == 7 else (*r, ts) for r in rows]
    spark.createDataFrame(rows, TRADE_COLS).writeTo("raw.trades").append()


POSITIONS_SQL = """
    SELECT account_id, ticker, SUM(qty) AS qty, ROUND(SUM(qty * price), 2) AS market_value
    FROM raw.trades GROUP BY account_id, ticker"""

governed = governed_job(
    inputs={"raw.trades": ["account_id", "ticker", "qty", "price"]},
    output="curated.positions",
    keys=["account_id", "ticker"],
    pii=["client_name"],
)


@governed
def build_positions(spark):
    spark.sql(f"""
        MERGE INTO curated.positions t USING ({POSITIONS_SQL}) s
        ON t.account_id = s.account_id AND t.ticker = s.ticker
        WHEN MATCHED AND (t.qty <> s.qty OR t.market_value <> s.market_value) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


@governed
def build_positions_buggy(spark):
    """A buggy job: appends instead of merging, so keys get duplicated."""
    spark.sql(f"INSERT INTO curated.positions {POSITIONS_SQL}")


@governed_job(inputs={"raw.trades": ["trade_id", "client_name", "trade_ts"]},
              output="curated.trades", keys=["trade_id"], pii=["client_name"])
def publish_trades(spark, run_key: str):
    """Publish one business date of trades, with client names pseudonymised.

    A plain append: running it twice for the same day would duplicate rows. The
    platform makes it safe: @audited skips a run_key that already succeeded, and
    the WAP audit rejects duplicate trade_ids if someone bypasses that.
    """
    day = date.fromisoformat(run_key)
    trades = spark.table("raw.trades").filter(
        (F.col("trade_ts") >= F.lit(day)) & (F.col("trade_ts") < F.lit(day + timedelta(days=1))))
    hash_pii(trades, ["client_name"]).writeTo("curated.trades").append()


@audited(outputs=["raw.trades", "curated.trades"])
def erase_client(spark, client_name: str) -> dict[str, int]:
    """GDPR right to erasure: remove one client from every table that holds them."""
    subject = pii_hash(client_name)
    return {"raw.trades": erase(spark, "raw.trades", "client_name", subject),
            "curated.trades": erase(spark, "curated.trades", "client_name_sha256", subject)}
