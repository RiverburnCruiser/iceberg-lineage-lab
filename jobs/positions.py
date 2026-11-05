"""Example jobs: raw trades -> curated positions, behind @governed_job."""
from lab.governance import governed_job

TRADE_COLS = ["trade_id", "account_id", "client_name", "ticker", "qty", "price"]


def create_tables(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS raw")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS curated")
    props = "TBLPROPERTIES ('format-version'='2', 'write.wap.enabled'='true')"
    spark.sql(f"""CREATE TABLE IF NOT EXISTS raw.trades (
        trade_id BIGINT, account_id STRING, client_name STRING, ticker STRING,
        qty INT, price DOUBLE) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS curated.positions (
        account_id STRING, ticker STRING, qty BIGINT, market_value DOUBLE)
        USING iceberg {props}""")


def load_trades(spark, rows: list[tuple]) -> None:
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
