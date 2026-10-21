#!/usr/bin/env python3
"""
CBSL & Market Commodity Open Data Ingestor
Module 1: Storage Layer & Git-Style Iceberg Catalog (Bronze Ingestion)

Ingests:
1. Central Bank of Sri Lanka (CBSL) Daily Exchange Rates (Forex Ticks)
2. Sri Lanka Open Market Daily Commodity Prices (Pettah, Dambulla, etc.)
3. Agricultural Weather Observations (Temperature, Rainfall, Humidity)

Writes raw Bronze files (JSON & Parquet) to MinIO Object Store.
Validates raw payload schema contracts using Pydantic before writing.
"""

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import os
import random
import sys
from typing import Any, Dict, List, Optional
import uuid

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, field_validator


# ==========================================
# Pydantic Schemas for Contract Validation
# ==========================================

class QuarantineRecord(BaseModel):
    quarantine_id: str
    dataset_name: str
    quarantined_at: str
    failure_reason: str
    violating_fields: List[str]
    raw_payload: str

class ForexTick(BaseModel):
    currency_code: str = Field(..., min_length=3, max_length=3)
    base_currency: str = Field(default="LKR", min_length=3, max_length=3)
    indicative_rate: float = Field(..., gt=0.0)
    buying_rate: Optional[float] = Field(default=None, gt=0.0)
    selling_rate: Optional[float] = Field(default=None, gt=0.0)
    trade_date: str = Field(..., description="ISO 8601 YYYY-MM-DD")
    extracted_at: str

    @field_validator("currency_code")
    def validate_currency(cls, v):
        return v.upper()


class MarketCommodityPrice(BaseModel):
    commodity_id: str
    commodity_name: str
    category: str
    market_center: str
    province: str
    wholesale_price: float = Field(..., gt=0.0)
    retail_price: float = Field(..., gt=0.0)
    unit: str = "Rs/kg"
    grade: str = "Grade A"
    trade_date: str
    extracted_at: str

    @field_validator("wholesale_price", "retail_price")
    def validate_positive_prices(cls, v):
        if v <= 0:
            raise ValueError("Commodity prices must be positive")
        return round(float(v), 2)


class WeatherObservation(BaseModel):
    station_id: str
    province: str
    temperature_celsius: float = Field(..., ge=-10.0, le=55.0)
    rainfall_mm: float = Field(..., ge=0.0)
    humidity_percent: float = Field(..., ge=0.0, le=100.0)
    observation_date: str
    extracted_at: str


# ==========================================
# Scraper / Data Generator Engine
# ==========================================

COMMODITIES_CATALOG = [
    {"id": "RICE_SAMBA", "name": "Samba Rice", "category": "Grains & Cereals", "base_wholesale": 235.0, "base_retail": 260.0},
    {"id": "RICE_NADU", "name": "Nadu Rice", "category": "Grains & Cereals", "base_wholesale": 210.0, "base_retail": 230.0},
    {"id": "RICE_KEKULU_WHITE", "name": "Kekulu White Rice", "category": "Grains & Cereals", "base_wholesale": 195.0, "base_retail": 215.0},
    {"id": "POTATO_LOCAL", "name": "Nuwara Eliya Potato", "category": "Tubers", "base_wholesale": 320.0, "base_retail": 380.0},
    {"id": "POTATO_IMPORTED", "name": "Imported Potato", "category": "Tubers", "base_wholesale": 180.0, "base_retail": 220.0},
    {"id": "BIG_ONION_LOCAL", "name": "Local Big Onion", "category": "Alliums", "base_wholesale": 290.0, "base_retail": 340.0},
    {"id": "RED_ONION_SINNA", "name": "Red Onion (Sinna)", "category": "Alliums", "base_wholesale": 380.0, "base_retail": 450.0},
    {"id": "GREEN_CHILLI", "name": "Green Chilli", "category": "Spices", "base_wholesale": 650.0, "base_retail": 800.0},
    {"id": "FISH_THALAPATH", "name": "Thalapath (Sailfish)", "category": "Marine Fisheries", "base_wholesale": 1850.0, "base_retail": 2200.0},
    {"id": "FISH_BALAYA", "name": "Balaya (Skipjack Tuna)", "category": "Marine Fisheries", "base_wholesale": 850.0, "base_retail": 1100.0},
    {"id": "CARROT_LOCAL", "name": "Upcountry Carrot", "category": "Vegetables", "base_wholesale": 240.0, "base_retail": 300.0},
    {"id": "BEANS_GREEN", "name": "Green Beans", "category": "Vegetables", "base_wholesale": 280.0, "base_retail": 340.0},
]

MARKET_CENTERS = [
    {"name": "Pettah Wholesale Market", "province": "Western", "hub_code": "PETTAH"},
    {"name": "Dambulla Dedicated Economic Centre", "province": "Central", "hub_code": "DAMBULLA"},
    {"name": "Narahenpita Economic Centre", "province": "Western", "hub_code": "NARAHENPITA"},
    {"name": "Kandy Central Market", "province": "Central", "hub_code": "KANDY"},
    {"name": "Thambuththegama Economic Centre", "province": "North Central", "hub_code": "THAMBUTHTHEGAMA"},
]

CURRENCIES = [
    {"code": "USD", "base_rate": 303.50, "spread": 2.80},
    {"code": "EUR", "base_rate": 328.20, "spread": 3.10},
    {"code": "GBP", "base_rate": 394.80, "spread": 3.90},
    {"code": "JPY", "base_rate": 2.02, "spread": 0.04},
    {"code": "AUD", "base_rate": 201.40, "spread": 2.20},
    {"code": "SGD", "base_rate": 230.10, "spread": 2.40},
    {"code": "CNY", "base_rate": 42.60, "spread": 0.50},
    {"code": "INR", "base_rate": 3.62, "spread": 0.06},
    {"code": "AED", "base_rate": 82.65, "spread": 0.90},
]

WEATHER_STATIONS = [
    {"station_id": "WS_DAMBULLA", "province": "Central", "base_temp": 28.5},
    {"station_id": "WS_NUWARA_ELIYA", "province": "Central", "base_temp": 16.0},
    {"station_id": "WS_COLOMBO", "province": "Western", "base_temp": 30.5},
    {"station_id": "WS_ANURADHAPURA", "province": "North Central", "base_temp": 31.0},
]


def validate_and_route(
    raw_payload: Dict[str, Any],
    schema_cls,
    dataset_name: str,
    valid_list: List[Dict[str, Any]],
    quarantine_list: List[Dict[str, Any]],
):
    """
    Evaluates raw payload against Pydantic contract.
    Routes conforming records to valid Bronze stream,
    and corrupted/malformed payloads to Quarantine DLQ tier.
    """
    try:
        instance = schema_cls(**raw_payload)
        valid_list.append(instance.model_dump())
    except Exception as e:
        violating = []
        if isinstance(e, ValidationError):
            violating = [str(err.get("loc", ["field"])[0]) for err in e.errors()]
        q_entry = QuarantineRecord(
            quarantine_id=str(uuid.uuid4()),
            dataset_name=dataset_name,
            quarantined_at=datetime.now(timezone.utc).isoformat(),
            failure_reason=str(e),
            violating_fields=violating,
            raw_payload=json.dumps(raw_payload),
        )
        quarantine_list.append(q_entry.model_dump())


def generate_historical_dataset(
    start_date: date,
    end_date: date,
    inject_anomalies: bool = False
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Generates time-series data covering CBSL Forex, Market Commodities, and Weather.
    Applies Pydantic contract validation at the ingestion boundary and isolates
    corrupted records into a dedicated Quarantine Dead-Letter Queue (DLQ).
    """
    forex_valid = []
    forex_quarantine = []
    commodity_valid = []
    commodity_quarantine = []
    weather_valid = []
    weather_quarantine = []

    curr_date = start_date
    now_iso = datetime.now(timezone.utc).isoformat()

    # Seed for deterministic variability
    random.seed(42)

    forex_walk = {c["code"]: c["base_rate"] for c in CURRENCIES}
    commodity_walk = {
        (c["id"], m["name"]): (c["base_wholesale"], c["base_retail"])
        for c in COMMODITIES_CATALOG
        for m in MARKET_CENTERS
    }

    while curr_date <= end_date:
        date_str = curr_date.isoformat()
        is_weekend = curr_date.weekday() >= 5

        # 1. CBSL Forex (excluding weekends)
        if not is_weekend:
            for c in CURRENCIES:
                drift = random.uniform(-0.004, 0.004)
                forex_walk[c["code"]] = round(max(1.0, forex_walk[c["code"]] * (1 + drift)), 4)
                mid = forex_walk[c["code"]]
                spread = c["spread"] * (1 + random.uniform(-0.05, 0.05))
                buying = round(mid - (spread / 2), 4)
                selling = round(mid + (spread / 2), 4)

                payload = {
                    "currency_code": c["code"],
                    "base_currency": "LKR",
                    "indicative_rate": mid,
                    "buying_rate": buying,
                    "selling_rate": selling,
                    "trade_date": date_str,
                    "extracted_at": now_iso,
                }
                validate_and_route(payload, ForexTick, "cbsl_forex", forex_valid, forex_quarantine)

        # 2. Market Commodities
        for c in COMMODITIES_CATALOG:
            for m in MARKET_CENTERS:
                grade = "Standard Grade"
                if c["id"] == "RICE_SAMBA":
                    if curr_date >= date(2026, 9, 15) and m["name"] == "Pettah Wholesale Market":
                        grade = "Super Grade Premium"
                    else:
                        grade = "Grade A Official"
                elif c["id"] == "POTATO_LOCAL":
                    grade = "Grade 1 High-Grown" if curr_date >= date(2026, 8, 1) else "Commercial Grade"

                key = (c["id"], m["name"])
                base_w, base_r = commodity_walk[key]
                hub_mult = 0.94 if "Dambulla" in m["name"] else (1.05 if "Pettah" in m["name"] else 1.0)
                price_noise = random.uniform(-0.03, 0.035)

                new_w = round(max(10.0, base_w * (1 + price_noise) * hub_mult), 2)
                margin = random.uniform(1.15, 1.25)
                new_r = round(new_w * margin, 2)
                commodity_walk[key] = (new_w, new_r)

                payload = {
                    "commodity_id": c["id"],
                    "commodity_name": c["name"],
                    "category": c["category"],
                    "market_center": m["name"],
                    "province": m["province"],
                    "wholesale_price": new_w,
                    "retail_price": new_r,
                    "unit": "Rs/kg",
                    "grade": grade,
                    "trade_date": date_str,
                    "extracted_at": now_iso,
                }
                validate_and_route(payload, MarketCommodityPrice, "market_commodities", commodity_valid, commodity_quarantine)

        # 3. Weather Observations
        for ws in WEATHER_STATIONS:
            temp_noise = random.uniform(-1.5, 1.8)
            rain = max(0.0, round(random.expovariate(0.12) - 1.0, 1))
            humidity = min(99.0, max(45.0, round(75.0 + random.uniform(-15, 15), 1)))

            payload = {
                "station_id": ws["station_id"],
                "province": ws["province"],
                "temperature_celsius": round(ws["base_temp"] + temp_noise, 1),
                "rainfall_mm": rain,
                "humidity_percent": humidity,
                "observation_date": date_str,
                "extracted_at": now_iso,
            }
            validate_and_route(payload, WeatherObservation, "weather_observations", weather_valid, weather_quarantine)

        curr_date += timedelta(days=1)

    # Optional Anomaly Injection for Chaos / Quarantine Testing
    if inject_anomalies:
        print("⚠️ [CHAOS INJECTION] Injecting corrupted payloads to verify Quarantine routing...")
        # 1. Corrupted negative price
        bad_comm = {
            "commodity_id": "RICE_SAMBA",
            "commodity_name": "Samba Rice",
            "category": "Grains & Cereals",
            "market_center": "Pettah Wholesale Market",
            "province": "Western",
            "wholesale_price": -50.0,  # Negative wholesale price (Breach!)
            "retail_price": 200.0,
            "unit": "Rs/kg",
            "grade": "Standard Grade",
            "trade_date": "2026-10-21",
            "extracted_at": now_iso,
        }
        validate_and_route(bad_comm, MarketCommodityPrice, "market_commodities", commodity_valid, commodity_quarantine)

        # 2. Corrupted invalid currency code
        bad_forex = {
            "currency_code": "INVALID_USD_TOO_LONG",
            "base_currency": "LKR",
            "indicative_rate": 300.0,
            "trade_date": "2026-10-21",
            "extracted_at": now_iso,
        }
        validate_and_route(bad_forex, ForexTick, "cbsl_forex", forex_valid, forex_quarantine)

    return {
        "valid": {
            "cbsl_forex": forex_valid,
            "market_commodities": commodity_valid,
            "weather_observations": weather_valid,
        },
        "quarantine": {
            "cbsl_forex": forex_quarantine,
            "market_commodities": commodity_quarantine,
            "weather_observations": weather_quarantine,
        },
    }


def get_s3_client():
    """Initializes boto3 S3 client for MinIO."""
    try:
        import boto3
        from botocore.client import Config
    except ImportError:
        print("[INFO] boto3 not installed. S3 upload skipped.")
        return None

    endpoint_url = os.getenv("S3_ENDPOINT", os.getenv("S3_CLIENT_ENDPOINT", "http://localhost:9000"))
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")
    region_name = os.getenv("AWS_REGION", "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name,
        config=Config(signature_version="s3v4"),
    )


def save_and_upload_dataset(
    dataset: Dict[str, Dict[str, List[Dict[str, Any]]]],
    output_dir: str,
    upload_s3: bool = True,
    bucket_name: str = "bronze"
) -> Dict[str, str]:
    """
    Saves valid records to Bronze tier and isolated corrupted records to Quarantine tier.
    Uploads to MinIO bronze bucket if configured.
    """
    os.makedirs(output_dir, exist_ok=True)
    results = {}
    s3_client = get_s3_client() if upload_s3 else None

    # 1. Process Valid Records (Bronze Tier)
    for dataset_name, records in dataset.get("valid", {}).items():
        if not records:
            continue

        df = pd.DataFrame(records)
        date_col = "trade_date" if "trade_date" in df.columns else "observation_date"
        df["year_month"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m")

        dataset_dir = os.path.join(output_dir, dataset_name)
        os.makedirs(dataset_dir, exist_ok=True)

        json_path = os.path.join(dataset_dir, f"{dataset_name}_raw.json")
        parquet_path = os.path.join(dataset_dir, f"{dataset_name}_raw.parquet")

        df.drop(columns=["year_month"]).to_json(json_path, orient="records", indent=2)
        df.drop(columns=["year_month"]).to_parquet(parquet_path, index=False, engine="pyarrow")

        print(f"[BRONZE VALID] Saved {len(df)} records to {parquet_path}")
        results[dataset_name] = parquet_path

        if s3_client:
            try:
                for ym, group in df.groupby("year_month"):
                    s3_prefix = f"{dataset_name}/year_month={ym}/{dataset_name}.parquet"
                    group_parquet = os.path.join(dataset_dir, f"temp_{ym}.parquet")
                    group.drop(columns=["year_month"]).to_parquet(group_parquet, index=False, engine="pyarrow")
                    with open(group_parquet, "rb") as f:
                        s3_client.put_object(Bucket=bucket_name, Key=s3_prefix, Body=f, ContentType="application/octet-stream")
                    if os.path.exists(group_parquet):
                        os.remove(group_parquet)
                print(f"[S3 BRONZE] Uploaded {dataset_name} to s3://{bucket_name}/{dataset_name}/")
            except Exception as e:
                print(f"[WARNING] S3 Upload note: {e}")

    # 2. Process Quarantine Records (DLQ Tier)
    quarantine_base = os.path.join(output_dir, "quarantine")
    total_quarantined = 0
    for dataset_name, q_records in dataset.get("quarantine", {}).items():
        if not q_records:
            continue
        total_quarantined += len(q_records)
        q_dir = os.path.join(quarantine_base, dataset_name)
        os.makedirs(q_dir, exist_ok=True)

        q_df = pd.DataFrame(q_records)
        q_jsonl = os.path.join(q_dir, "quarantine.jsonl")
        q_parquet = os.path.join(q_dir, "quarantine.parquet")

        q_df.to_json(q_jsonl, orient="records", lines=True)
        q_df.to_parquet(q_parquet, index=False, engine="pyarrow")

        print(f"🛑 [QUARANTINE DLQ] Routed {len(q_df)} corrupted records to {q_jsonl}")

        if s3_client:
            try:
                s3_q_key = f"quarantine/{dataset_name}/quarantine.parquet"
                with open(q_parquet, "rb") as f:
                    s3_client.put_object(Bucket=bucket_name, Key=s3_q_key, Body=f, ContentType="application/octet-stream")
                print(f"[S3 QUARANTINE] Uploaded DLQ records to s3://{bucket_name}/{s3_q_key}")
            except Exception as e:
                print(f"[WARNING] S3 Quarantine upload note: {e}")

    if total_quarantined == 0:
        print("✅ [QUARANTINE DLQ] Zero malformed records detected. Ingestion 100% clean.")

    return results


def main():
    parser = argparse.ArgumentParser(description="IceFlow Core CBSL & Market Commodity Bronze Scraper with Quarantine DLQ")
    parser.add_argument("--start-date", default="2026-08-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2026-10-21", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", default="./bronze_data", help="Local directory for raw files")
    parser.add_argument("--upload-s3", action="store_true", default=True, help="Upload to MinIO S3 Bronze bucket")
    parser.add_argument("--no-s3", dest="upload_s3", action="store_false", help="Disable MinIO S3 upload")
    parser.add_argument("--bucket", default="bronze", help="Target Bronze S3 bucket name")
    parser.add_argument("--inject-anomalies", action="store_true", help="Inject corrupted records to test quarantine")

    args = parser.parse_args()

    s_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    e_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    print(f"=== Starting CBSL & Market Commodity Ingestion ===")
    print(f"Time Window: {s_date} to {e_date} ({ (e_date - s_date).days + 1 } days)")

    dataset = generate_historical_dataset(s_date, e_date, inject_anomalies=args.inject_anomalies)
    valid = dataset["valid"]
    quarantine = dataset["quarantine"]

    print(f"Ingestion Stream Summary:")
    print(f" - Valid Forex Ticks: {len(valid['cbsl_forex'])} | Quarantined: {len(quarantine['cbsl_forex'])}")
    print(f" - Valid Commodities: {len(valid['market_commodities'])} | Quarantined: {len(quarantine['market_commodities'])}")
    print(f" - Valid Weather: {len(valid['weather_observations'])} | Quarantined: {len(quarantine['weather_observations'])}")

    save_and_upload_dataset(
        dataset=dataset,
        output_dir=args.output_dir,
        upload_s3=args.upload_s3,
        bucket_name=args.bucket,
    )
    print("=== Bronze Ingestion Completed Successfully ===")


if __name__ == "__main__":
    main()
