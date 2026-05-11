import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

random.seed(42)

CURRENCIES = ["USD", "GBP", "EUR", "JPY", "CHF", "AUD", "CAD", "HKD"]
TRANSACTION_TYPES = ["PAYMENT", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "FX_TRADE", "SETTLEMENT"]
STATUSES = ["COMPLETED", "PENDING", "FAILED", "CANCELLED", "PROCESSING"]
STATUS_WEIGHTS = [0.78, 0.10, 0.05, 0.04, 0.03]
COUNTERPARTIES = ["BARCLAYS", "HSBC", "DEUTSCHE_BANK", "CITIGROUP", "JPMORGAN",
                  "GOLDMAN_SACHS", "MORGAN_STANLEY", "UBS"]

def generate_customer(customer_id):
    return {
        "customer_id": customer_id,
        "first_name": f"First{random.randint(1,999)}",
        "last_name": f"Last{random.randint(1,999)}",
        "email": f"user{random.randint(1,9999)}@example.com",
        "country_code": random.choice(["GB", "US", "DE", "FR", "SG"]),
        "account_type": random.choice(["RETAIL", "INSTITUTIONAL", "PRIVATE_BANKING"]),
        "risk_rating": random.choice(["LOW", "MEDIUM", "HIGH"]),
        "created_at": "2023-01-01T00:00:00",
        "is_active": random.choice([True, True, True, False]),
    }

def generate_transaction(tx_id, customer_id, base_date):
    currency = random.choice(CURRENCIES)
    amount = round(random.uniform(10.0, 500_000.0), 2)
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
    tx_date = base_date + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    record = {
        "transaction_id": tx_id,
        "customer_id": customer_id,
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "amount": amount,
        "currency": currency,
        "status": status,
        "counterparty": random.choice(COUNTERPARTIES),
        "reference_number": f"REF{random.randint(1000000000,9999999999)}",
        "transaction_date": tx_date.isoformat(),
        "settlement_date": (tx_date + timedelta(days=random.choice([0,1,2]))).isoformat(),
        "description": f"Transaction {tx_id[:8]}",
        "fee_amount": round(amount * 0.001, 4),
        "fee_currency": currency,
        "source_system": random.choice(["CORE_BANKING", "TRADING_PLATFORM", "MOBILE_APP"]),
        "batch_id": f"BATCH_{base_date.strftime('%Y%m%d')}_01",
        "created_at": tx_date.isoformat(),
        "updated_at": tx_date.isoformat(),
    }
    # Intentional DQ issues
    if random.random() < 0.03:
        record["amount"] = None
    if random.random() < 0.02 and record["amount"]:
        record["amount"] = -abs(record["amount"])
    if random.random() < 0.02:
        record["customer_id"] = None
    if random.random() < 0.01:
        record["currency"] = random.choice(["XXX", "ZZZ"])
    return record

def generate_all(n_transactions, output_dir, days_back=90):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Generating {n_transactions} transactions...")

    n_customers = max(500, n_transactions // 20)
    customer_ids = [str(uuid.uuid4()) for _ in range(n_customers)]
    customers = [generate_customer(cid) for cid in customer_ids]
    pd.DataFrame(customers).to_csv(output_path / "customers.csv", index=False)
    print(f"✓ Customers: {len(customers)} rows")

    all_transactions = []
    base = datetime.now() - timedelta(days=days_back)
    for day_offset in range(days_back):
        day_date = base + timedelta(days=day_offset)
        daily_count = n_transactions // days_back
        for _ in range(daily_count):
            tx_id = str(uuid.uuid4())
            customer_id = random.choice(customer_ids)
            all_transactions.append(generate_transaction(tx_id, customer_id, day_date))

    tx_df = pd.DataFrame(all_transactions)
    tx_df.to_csv(output_path / "transactions.csv", index=False)

    with open(output_path / "transactions.jsonl", "w") as f:
        for record in all_transactions:
            f.write(json.dumps(record) + "\n")

    print(f"✓ Transactions: {len(all_transactions)} rows")

    # DQ summary
    null_amt = tx_df["amount"].isna().sum()
    neg_amt = (tx_df["amount"].fillna(0) < 0).sum()
    null_cust = tx_df["customer_id"].isna().sum()
    print(f"\n── DQ issues intentionally introduced ──")
    print(f"  Null amounts:      {null_amt} ({null_amt/len(tx_df)*100:.1f}%)")
    print(f"  Negative amounts:  {neg_amt} ({neg_amt/len(tx_df)*100:.1f}%)")
    print(f"  Null customer IDs: {null_cust} ({null_cust/len(tx_df)*100:.1f}%)")
    print(f"\n✓ Done. Ready to run ETL pipeline.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--output-dir", default="./data/raw")
    parser.add_argument("--days-back", type=int, default=90)
    args = parser.parse_args()
    generate_all(args.records, args.output_dir, args.days_back)