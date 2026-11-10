"""PII handling and right-to-erasure on Iceberg tables.

Pseudonymise: curated tables keep a salted SHA-256 of a PII column instead of
the value. It still joins (same input -> same hash) but cannot be read back.
The salt is a platform secret (env PII_SALT), never something job authors handle.
A salted hash is still personal data under GDPR, so it is erased as well.

Erase: deleting rows is not enough on Iceberg, because older snapshots still
reference the old data files and time travel can read them. After the delete,
expire every older snapshot so those files are physically removed.
"""
import hashlib
import os
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .contracts import DataContractError


def _salt() -> str:
    return os.environ.get("PII_SALT", "dev-only-salt")


def pii_hash(value: str) -> str:
    """The same salted SHA-256 that `hash_pii` writes, computed in Python."""
    return hashlib.sha256((_salt() + value).encode()).hexdigest()


def _hash_col(col: Column) -> Column:
    return F.sha2(F.concat(F.lit(_salt()), col), 256)


def hash_pii(df: DataFrame, cols: list[str]) -> DataFrame:
    """Replace each PII column `c` with `c_sha256`, a salted SHA-256 of its value."""
    for c in cols:
        df = df.withColumn(f"{c}_sha256", _hash_col(F.col(c))).drop(c)
    return df


def _location(spark: SparkSession, table: str) -> str:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    return next(r.data_type for r in rows if r.col_name == "Location")


def erase(spark: SparkSession, table: str, column: str, subject_hash: str) -> int:
    """Delete rows where sha(column) matches, then expire older snapshots. Returns files deleted.

    Matching on the hash keeps the person's name out of SQL text and logs.
    """
    if not all(c in "0123456789abcdef" for c in subject_hash):
        raise ValueError("subject_hash must be a hex SHA-256")
    if other := [r.name for r in spark.sql(f"SELECT name FROM {table}.refs").collect()
                 if r.name != "main"]:
        raise DataContractError(f"{table}: refs {other} keep old snapshots alive; drop them first")
    match = column if column.endswith("_sha256") else \
        f"sha2(concat({_sql_str(_salt())}, {column}), 256)"
    spark.sql(f"DELETE FROM {table} WHERE {match} = '{subject_hash}'")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    out = spark.sql(f"""CALL system.expire_snapshots(
        table => '{table}', older_than => TIMESTAMP '{now}', retain_last => 1)""").first()
    return out.deleted_data_files_count


def rows_on_disk(spark: SparkSession, table: str, condition: Column) -> int:
    """Count matching rows in every Parquet file under the table's data folder,
    including files no snapshot references. This is the proof that erasure worked."""
    return (spark.read.option("recursiveFileLookup", "true")
            .parquet(f"{_location(spark, table)}/data").filter(condition).count())


def _sql_str(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"
