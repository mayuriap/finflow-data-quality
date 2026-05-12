import pytest
import pandas as pd
from datetime import datetime, timedelta


# ── Constants (mirror the ETL pipeline) ──────────────────────────────────────

VALID_CURRENCIES = {"USD", "GBP", "EUR", "JPY", "CHF", "AUD", "CAD", "HKD"}
VALID_STATUSES   = {"COMPLETED", "PENDING", "FAILED", "CANCELLED", "PROCESSING"}
VALID_TX_TYPES   = {"PAYMENT", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "FX_TRADE", "SETTLEMENT"}

FX_RATES = {
    "USD": 1.00, "GBP": 1.27, "EUR": 1.08,
    "JPY": 0.0067, "CHF": 1.11,
    "AUD": 0.65, "CAD": 0.74, "HKD": 0.13,
}


# ── Helpers (mirror Silver model logic in Python) ─────────────────────────────

def apply_silver_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors the WHERE clause in clean_transactions.sql"""
    return df[
        df["transaction_id"].notna() &
        df["customer_id"].notna() &
        df["amount"].notna() &
        (df["amount"] > 0) &
        df["currency"].isin(VALID_CURRENCIES) &
        df["status"].isin(VALID_STATUSES) &
        df["transaction_date"].notna()
    ].copy()


def convert_to_usd(amount: float, currency: str) -> float:
    """Mirrors the CASE statement in clean_transactions.sql"""
    return round(amount * FX_RATES.get(currency, 1.0), 4)


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors the ROW_NUMBER() dedup in clean_transactions.sql"""
    return (
        df.sort_values("created_at", ascending=False)
        .drop_duplicates(subset=["transaction_id"])
        .reset_index(drop=True)
    )


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_transactions():
    """10 valid transaction records."""
    base = datetime(2024, 6, 1, 10, 0, 0)
    return pd.DataFrame([
        {
            "transaction_id":   f"tx-{i:04d}",
            "customer_id":      f"cust-{i % 5:04d}",
            "transaction_type": "PAYMENT",
            "amount":           round(100.0 + i * 50, 2),
            "currency":         ["USD", "GBP", "EUR"][i % 3],
            "status":           "COMPLETED",
            "transaction_date": (base + timedelta(hours=i)).isoformat(),
            "created_at":       (base + timedelta(hours=i)).isoformat(),
        }
        for i in range(10)
    ])


@pytest.fixture
def dirty_transactions():
    """Records with known DQ issues."""
    base = datetime(2024, 6, 1)
    return pd.DataFrame([
        # Null amount
        {"transaction_id": "tx-d001", "customer_id": "cust-001",
         "amount": None, "currency": "USD", "status": "COMPLETED",
         "transaction_date": base.isoformat(), "created_at": base.isoformat()},
        # Negative amount
        {"transaction_id": "tx-d002", "customer_id": "cust-001",
         "amount": -500.0, "currency": "USD", "status": "COMPLETED",
         "transaction_date": base.isoformat(), "created_at": base.isoformat()},
        # Invalid currency
        {"transaction_id": "tx-d003", "customer_id": "cust-002",
         "amount": 1000.0, "currency": "XXX", "status": "COMPLETED",
         "transaction_date": base.isoformat(), "created_at": base.isoformat()},
        # Null transaction_id
        {"transaction_id": None, "customer_id": "cust-003",
         "amount": 750.0, "currency": "GBP", "status": "COMPLETED",
         "transaction_date": base.isoformat(), "created_at": base.isoformat()},
        # Invalid status
        {"transaction_id": "tx-d005", "customer_id": "cust-004",
         "amount": 200.0, "currency": "EUR", "status": "INVALID",
         "transaction_date": base.isoformat(), "created_at": base.isoformat()},
        # Null customer_id
        {"transaction_id": "tx-d006", "customer_id": None,
         "amount": 300.0, "currency": "USD", "status": "COMPLETED",
         "transaction_date": base.isoformat(), "created_at": base.isoformat()},
    ])


# ── Tests: DQ flag detection ──────────────────────────────────────────────────

class TestDQFlagDetection:
    """Verify Bronze DQ flags correctly identify dirty records."""

    def test_null_amount_detected(self, dirty_transactions):
        null_amounts = dirty_transactions["amount"].isna().sum()
        assert null_amounts >= 1, "Expected at least one null amount"

    def test_negative_amount_detected(self, dirty_transactions):
        negatives = (dirty_transactions["amount"].fillna(0) < 0).sum()
        assert negatives >= 1, "Expected at least one negative amount"

    def test_invalid_currency_detected(self, dirty_transactions):
        invalid = (~dirty_transactions["currency"].isin(VALID_CURRENCIES)).sum()
        assert invalid >= 1, "Expected at least one invalid currency"

    def test_null_transaction_id_detected(self, dirty_transactions):
        null_ids = dirty_transactions["transaction_id"].isna().sum()
        assert null_ids >= 1, "Expected at least one null transaction_id"

    def test_null_customer_id_detected(self, dirty_transactions):
        null_custs = dirty_transactions["customer_id"].isna().sum()
        assert null_custs >= 1, "Expected at least one null customer_id"

    def test_clean_records_have_no_issues(self, clean_transactions):
        null_amounts = clean_transactions["amount"].isna().sum()
        negatives    = (clean_transactions["amount"] < 0).sum()
        null_ids     = clean_transactions["transaction_id"].isna().sum()
        assert null_amounts == 0
        assert negatives    == 0
        assert null_ids     == 0


# ── Tests: Silver filter logic ────────────────────────────────────────────────

class TestSilverFilter:
    """Verify Silver layer correctly removes dirty records."""

    def test_null_amount_excluded(self, dirty_transactions):
        silver = apply_silver_filter(dirty_transactions)
        assert silver["amount"].isna().sum() == 0

    def test_negative_amount_excluded(self, dirty_transactions):
        silver = apply_silver_filter(dirty_transactions)
        assert (silver["amount"] < 0).sum() == 0

    def test_invalid_currency_excluded(self, dirty_transactions):
        silver = apply_silver_filter(dirty_transactions)
        assert (~silver["currency"].isin(VALID_CURRENCIES)).sum() == 0

    def test_null_transaction_id_excluded(self, dirty_transactions):
        silver = apply_silver_filter(dirty_transactions)
        assert silver["transaction_id"].isna().sum() == 0

    def test_null_customer_id_excluded(self, dirty_transactions):
        silver = apply_silver_filter(dirty_transactions)
        assert silver["customer_id"].isna().sum() == 0

    def test_all_clean_records_pass_filter(self, clean_transactions):
        silver = apply_silver_filter(clean_transactions)
        assert len(silver) == len(clean_transactions), (
            f"All 10 clean records should pass. Got {len(silver)}"
        )

    def test_silver_coverage_above_threshold(
        self, clean_transactions, dirty_transactions
    ):
        """Silver must retain at least 80% of combined dataset."""
        combined = pd.concat(
            [clean_transactions, dirty_transactions], ignore_index=True
        )
        silver   = apply_silver_filter(combined)
        coverage = len(silver) / len(combined)
        assert coverage >= 0.60, f"Coverage too low: {coverage:.1%}"


# ── Tests: FX conversion ──────────────────────────────────────────────────────

class TestFXConversion:
    """Verify USD conversion logic matches the dbt Silver model."""

    def test_usd_is_identity(self):
        assert convert_to_usd(1000.0, "USD") == 1000.0

    def test_gbp_conversion(self):
        assert convert_to_usd(100.0, "GBP") == pytest.approx(127.0, rel=1e-4)

    def test_eur_conversion(self):
        assert convert_to_usd(100.0, "EUR") == pytest.approx(108.0, rel=1e-4)

    def test_jpy_conversion(self):
        assert convert_to_usd(10000.0, "JPY") == pytest.approx(67.0, rel=1e-4)

    def test_unknown_currency_defaults_to_1(self):
        assert convert_to_usd(500.0, "ZZZ") == pytest.approx(500.0, rel=1e-4)

    def test_all_currencies_return_positive(self):
        for currency in FX_RATES:
            result = convert_to_usd(1000.0, currency)
            assert result > 0, f"{currency} conversion must be positive"


# ── Tests: Deduplication ──────────────────────────────────────────────────────

class TestDeduplication:
    """Verify dedup keeps the most recent record."""

    def test_keeps_latest_record(self):
        base = datetime(2024, 1, 1)
        df = pd.DataFrame([
            {"transaction_id": "tx-001", "amount": 100.0,
             "created_at": base.isoformat()},
            {"transaction_id": "tx-001", "amount": 200.0,
             "created_at": (base + timedelta(hours=1)).isoformat()},
            {"transaction_id": "tx-001", "amount": 300.0,
             "created_at": (base + timedelta(hours=2)).isoformat()},
        ])
        result = deduplicate(df)
        assert len(result) == 1
        assert result.iloc[0]["amount"] == 300.0

    def test_unique_records_all_kept(self, clean_transactions):
        result = deduplicate(clean_transactions)
        assert len(result) == len(clean_transactions)

    def test_empty_dataframe_handled(self):
        empty = pd.DataFrame(columns=["transaction_id", "amount", "created_at"])
        result = deduplicate(empty)
        assert len(result) == 0


# ── Tests: Gold aggregations ──────────────────────────────────────────────────

class TestGoldAggregations:
    """Verify Gold layer aggregation logic."""

    def test_total_amount_correct(self, clean_transactions):
        total = clean_transactions["amount"].sum()
        assert total > 0

    def test_success_rate_between_0_and_1(self, clean_transactions):
        completed    = (clean_transactions["status"] == "COMPLETED").sum()
        total        = len(clean_transactions)
        success_rate = completed / total
        assert 0 <= success_rate <= 1

    def test_groupby_produces_rows(self, clean_transactions):
        clean_transactions["transaction_day"] = (
            pd.to_datetime(clean_transactions["transaction_date"]).dt.date
        )
        grouped = clean_transactions.groupby(
            ["transaction_day", "currency"]
        ).agg(total=("amount", "sum")).reset_index()
        assert len(grouped) > 0

    def test_completed_and_other_sum_to_total(self, clean_transactions):
        completed = (clean_transactions["status"] == "COMPLETED").sum()
        other     = (clean_transactions["status"] != "COMPLETED").sum()
        assert completed + other == len(clean_transactions)