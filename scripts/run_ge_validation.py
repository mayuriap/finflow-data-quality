"""
FinFlow — Great Expectations Bronze Validation
Validates the Bronze CSV output against expectation suites.
Run: python scripts/run_ge_validation.py
"""

import sys
from pathlib import Path
import pandas as pd


class Validator:
    """Lightweight GE-style validator using pandas."""

    def __init__(self, df: pd.DataFrame, suite_name: str):
        self.df = df
        self.suite_name = suite_name
        self._passed = 0
        self._failed = 0
        self.failures = []

    def _record(self, name: str, success: bool, detail: str = ""):
        status = "  ✓ PASS" if success else "  ✗ FAIL"
        if success:
            self._passed += 1
        else:
            self._failed += 1
            self.failures.append(f"{name} — {detail}")
        print(f"{status}  {name}" + (f"  [{detail}]" if detail else ""))

    def expect_column_to_exist(self, column: str):
        self._record(
            f"column_exists({column})",
            column in self.df.columns,
            f"columns={list(self.df.columns)}" if column not in self.df.columns else ""
        )

    def expect_column_no_nulls(self, column: str, mostly: float = 1.0):
        if column not in self.df.columns:
            self._record(f"no_nulls({column})", False, "column missing")
            return
        null_rate = self.df[column].isna().mean()
        success = (1 - null_rate) >= mostly
        self._record(
            f"no_nulls({column}, mostly={mostly})",
            success,
            f"null_rate={null_rate:.1%}"
        )

    def expect_column_unique(self, column: str):
        if column not in self.df.columns:
            self._record(f"unique({column})", False, "column missing")
            return
        dupes = self.df[column].dropna().duplicated().sum()
        self._record(f"unique({column})", dupes == 0, f"duplicates={dupes}")

    def expect_values_in_set(self, column: str, value_set: list, mostly: float = 1.0):
        if column not in self.df.columns:
            self._record(f"values_in_set({column})", False, "column missing")
            return
        valid_rate = self.df[column].isin(value_set).mean()
        success = valid_rate >= mostly
        self._record(
            f"values_in_set({column}, mostly={mostly})",
            success,
            f"valid_rate={valid_rate:.1%}"
        )

    def expect_values_between(self, column: str, min_val=None, max_val=None, mostly: float = 1.0):
        if column not in self.df.columns:
            self._record(f"values_between({column})", False, "column missing")
            return
        series = pd.to_numeric(self.df[column], errors="coerce").dropna()
        mask = pd.Series([True] * len(series), index=series.index)
        if min_val is not None:
            mask &= series >= min_val
        if max_val is not None:
            mask &= series <= max_val
        valid_rate = mask.mean() if len(mask) > 0 else 1.0
        success = valid_rate >= mostly
        self._record(
            f"values_between({column}, min={min_val}, max={max_val})",
            success,
            f"valid_rate={valid_rate:.1%}"
        )

    def expect_row_count_above(self, min_rows: int):
        n = len(self.df)
        self._record(
            f"row_count_above({min_rows})",
            n >= min_rows,
            f"actual={n:,}"
        )

    def expect_column_not_negative(self, column: str, mostly: float = 1.0):
        if column not in self.df.columns:
            self._record(f"not_negative({column})", False, "column missing")
            return
        series = pd.to_numeric(self.df[column], errors="coerce").dropna()
        neg_rate = (series < 0).mean()
        success = neg_rate <= (1 - mostly)
        self._record(
            f"not_negative({column}, mostly={mostly})",
            success,
            f"negative_rate={neg_rate:.1%}"
        )

    def summary(self) -> dict:
        total = self._passed + self._failed
        return {
            "suite":       self.suite_name,
            "total":       total,
            "passed":      self._passed,
            "failed":      self._failed,
            "success":     self._failed == 0,
            "success_pct": round(self._passed / max(total, 1) * 100, 1),
            "failures":    self.failures,
        }


# ── Expectation suites ────────────────────────────────────────────────────────

def validate_bronze_transactions(df: pd.DataFrame) -> dict:
    """
    Bronze is lenient — raw data has known issues.
    We use 'mostly' thresholds to allow for expected dirty records.
    """
    v = Validator(df, "bronze_transactions")

    # Schema checks
    for col in ["transaction_id", "customer_id", "amount",
                "currency", "status", "transaction_type",
                "transaction_date", "dq_has_issues",
                "_ingestion_timestamp", "_pipeline_layer"]:
        v.expect_column_to_exist(col)

    # Row count
    v.expect_row_count_above(100)

    # transaction_id: 99% non-null (Bronze allows 1% bad)
    v.expect_column_no_nulls("transaction_id", mostly=0.99)

    # amount: 95% non-null (we intentionally introduce ~3% nulls)
    v.expect_column_no_nulls("amount", mostly=0.95)

    # currency: 97% valid ISO codes
    v.expect_values_in_set(
        "currency",
        ["USD", "GBP", "EUR", "JPY", "CHF", "AUD", "CAD", "HKD"],
        mostly=0.97
    )

    # status: 98% valid
    v.expect_values_in_set(
        "status",
        ["COMPLETED", "PENDING", "FAILED", "CANCELLED", "PROCESSING"],
        mostly=0.98
    )

    # transaction_type: 99% valid
    v.expect_values_in_set(
        "transaction_type",
        ["PAYMENT", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "FX_TRADE", "SETTLEMENT"],
        mostly=0.99
    )

    # amount: mostly positive (allow ~2% negatives we intentionally introduced)
    v.expect_column_not_negative("amount", mostly=0.97)

    # pipeline audit column must always be 'bronze'
    v.expect_values_in_set("_pipeline_layer", ["bronze"], mostly=1.0)

    return v.summary()


def validate_bronze_customers(df: pd.DataFrame) -> dict:
    """Customer dimension — stricter because it's smaller and cleaner."""
    v = Validator(df, "bronze_customers")

    for col in ["customer_id", "email", "account_type",
                "risk_rating", "_pipeline_layer"]:
        v.expect_column_to_exist(col)

    v.expect_row_count_above(10)
    v.expect_column_no_nulls("customer_id", mostly=1.0)
    v.expect_column_no_nulls("email", mostly=0.99)
    v.expect_values_in_set(
        "account_type",
        ["RETAIL", "INSTITUTIONAL", "PRIVATE_BANKING"],
        mostly=1.0
    )
    v.expect_values_in_set(
        "risk_rating",
        ["LOW", "MEDIUM", "HIGH"],
        mostly=1.0
    )

    return v.summary()


# ── Loader ────────────────────────────────────────────────────────────────────

def load_bronze_csv(folder: str) -> pd.DataFrame:
    """Reads all CSV partition files from a Bronze folder into one DataFrame."""
    folder_path = Path(folder)
    csv_files = list(folder_path.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {folder}")
    dfs = [pd.read_csv(f) for f in csv_files]
    combined = pd.concat(dfs, ignore_index=True)
    print(f"  Loaded {len(combined):,} rows from {len(csv_files)} file(s) in {folder}")
    return combined


# ── Runner ────────────────────────────────────────────────────────────────────

def run_all(bronze_dir: str = "./data/bronze"):
    print("\n" + "=" * 55)
    print("  FinFlow — Great Expectations Validation")
    print("=" * 55)

    all_results = []

    # Transactions
    print("\n── Bronze Transactions ──────────────────────────────")
    try:
        tx_df = load_bronze_csv(f"{bronze_dir}/transactions")
        result = validate_bronze_transactions(tx_df)
        all_results.append(result)
    except FileNotFoundError as e:
        print(f"  ✗ {e}")
        print("  Run: python ingestion/etl_pipeline.py first")
        sys.exit(1)

    # Customers
    print("\n── Bronze Customers ─────────────────────────────────")
    try:
        cust_df = load_bronze_csv(f"{bronze_dir}/customers")
        result = validate_bronze_customers(cust_df)
        all_results.append(result)
    except FileNotFoundError as e:
        print(f"  ✗ {e}")

    # Overall summary
    print("\n" + "=" * 55)
    print("  VALIDATION SUMMARY")
    print("=" * 55)
    overall_pass = True
    for r in all_results:
        icon = "✓" if r["success"] else "✗"
        print(f"  {icon} {r['suite']:<35} {r['passed']}/{r['total']} ({r['success_pct']}%)")
        if r["failures"]:
            for f in r["failures"]:
                print(f"      → {f}")
        if not r["success"]:
            overall_pass = False

    print("=" * 55)
    print(f"  Overall: {'✓ ALL PASSED' if overall_pass else '✗ FAILURES DETECTED'}")
    print("=" * 55 + "\n")
    return overall_pass


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)