"""
AzurePulse — Cloud Data Engineering & Analytics Platform
Modular Data Ingestion, Data Quality & Transformation Engine (Python / Pandas)
"""

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np


def generate_synthetic_ecommerce_dataset() -> List[Dict[str, Any]]:
    """
    Generates a synthetic enterprise e-commerce transactions dataset (64 records)
    including realistic data engineering edge cases:
    - Exact duplicate records (3 rows)
    - Duplicate business keys / order_id collisions (2 rows)
    - Missing values in customer_segment, region, and unit_price
    - Invalid date strings ("2026-13-45", "INVALID_DATE")
    - Unnormalized whitespace and inconsistent category casing ("  electronics ", "ELECTRONICS", "home & kitchen ")
    - Invalid numeric values (negative quantity -2, negative unit_price -15.00)
    """
    base_records = [
        {"order_id": "ORD-1001", "order_date": "2026-08-01", "product_name": "Surface Pro 11 Enterprise", "category": "Electronics", "quantity": 4, "unit_price": 1299.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1002", "order_date": "2026-08-02", "product_name": "Azure Kinect DK Sensor", "category": "  electronics ", "quantity": 10, "unit_price": 399.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1003", "order_date": "2026-08-03", "product_name": "Ergonomic Mesh Task Chair", "category": "Office Furniture", "quantity": 15, "unit_price": 349.50, "region": "Europe", "customer_segment": "Mid-Market", "payment_method": "Invoice Net-30"},
        {"order_id": "ORD-1004", "order_date": "2026-08-04", "product_name": "Thunderbolt 4 Dock G5", "category": "ELECTRONICS", "quantity": 25, "unit_price": 279.00, "region": "Asia Pacific", "customer_segment": "Enterprise", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1005", "order_date": "2026-08-05", "product_name": "Standing Desk Dual Motor", "category": "office furniture", "quantity": 8, "unit_price": 620.00, "region": "North America", "customer_segment": "SMB", "payment_method": "Credit Card"},
        {"order_id": "ORD-1006", "order_date": "2026-08-06", "product_name": "Power BI Premium Per User (Annual)", "category": "Software Licenses", "quantity": 50, "unit_price": 240.00, "region": "Europe", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1007", "order_date": "2026-08-07", "product_name": "Noise-Canceling Conferencing Headset", "category": "Accessories", "quantity": 30, "unit_price": 189.00, "region": "Latin America", "customer_segment": "Mid-Market", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1008", "order_date": "2026-08-08", "product_name": "4K UltraSharp 32 Monitor", "category": "Electronics", "quantity": 12, "unit_price": 749.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        # Exact duplicate of ORD-1008
        {"order_id": "ORD-1008", "order_date": "2026-08-08", "product_name": "4K UltraSharp 32 Monitor", "category": "Electronics", "quantity": 12, "unit_price": 749.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1009", "order_date": "2026-08-09", "product_name": "Rackmount UPS 3000VA", "category": "Cloud Hardware", "quantity": 3, "unit_price": 1450.00, "region": "Asia Pacific", "customer_segment": "Enterprise", "payment_method": "Invoice Net-30"},
        {"order_id": "ORD-1010", "order_date": "2026-08-10", "product_name": "Mechanical Wireless Keyboard", "category": "  accessories", "quantity": 40, "unit_price": 129.00, "region": "Europe", "customer_segment": "SMB", "payment_method": "Credit Card"},
        # Edge case: Missing customer_segment
        {"order_id": "ORD-1011", "order_date": "2026-08-11", "product_name": "GitHub Enterprise Seat Pack", "category": "software licenses", "quantity": 35, "unit_price": 252.00, "region": "North America", "customer_segment": None, "payment_method": "Wire Transfer"},
        # Edge case: Invalid date
        {"order_id": "ORD-1012", "order_date": "2026-13-45", "product_name": "Smart Room Bar Camera", "category": "Electronics", "quantity": 5, "unit_price": 999.00, "region": "Europe", "customer_segment": "Enterprise", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1013", "order_date": "2026-08-13", "product_name": "Modular Acoustic Desk Partition", "category": "Office Furniture", "quantity": 20, "unit_price": 175.00, "region": "North America", "customer_segment": "Mid-Market", "payment_method": "Invoice Net-30"},
        {"order_id": "ORD-1014", "order_date": "2026-08-14", "product_name": "10GbE Managed Switch 24-Port", "category": "cloud_hardware", "quantity": 6, "unit_price": 1120.00, "region": "Asia Pacific", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        # Edge case: Negative quantity
        {"order_id": "ORD-1015", "order_date": "2026-08-15", "product_name": "USB-C Multiport Travel Hub", "category": "Accessories", "quantity": -4, "unit_price": 79.00, "region": "North America", "customer_segment": "SMB", "payment_method": "Credit Card"},
        {"order_id": "ORD-1016", "order_date": "2026-08-16", "product_name": "Surface Laptop Studio 2", "category": "Electronics", "quantity": 7, "unit_price": 2199.00, "region": "Europe", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        # Exact duplicate of ORD-1016
        {"order_id": "ORD-1016", "order_date": "2026-08-16", "product_name": "Surface Laptop Studio 2", "category": "Electronics", "quantity": 7, "unit_price": 2199.00, "region": "Europe", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1017", "order_date": "2026-08-18", "product_name": "Azure Databricks Commit Unit Pack", "category": "Software Licenses", "quantity": 10, "unit_price": 950.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        # Edge case: Missing unit_price
        {"order_id": "ORD-1018", "order_date": "2026-08-19", "product_name": "Dual Monitor Articulating Arm", "category": "Accessories", "quantity": 18, "unit_price": None, "region": "Latin America", "customer_segment": "Mid-Market", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1019", "order_date": "2026-08-21", "product_name": "Enterprise NAS Storage Array 64TB", "category": "Cloud Hardware", "quantity": 2, "unit_price": 4200.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1020", "order_date": "2026-08-22", "product_name": "Executive Conference Table 12ft", "category": "  Office Furniture ", "quantity": 2, "unit_price": 1850.00, "region": "Europe", "customer_segment": "Enterprise", "payment_method": "Invoice Net-30"},
        # Edge case: Duplicate business key ORD-1020 with updated quantity
        {"order_id": "ORD-1020", "order_date": "2026-08-22", "product_name": "Executive Conference Table 12ft", "category": "Office Furniture", "quantity": 3, "unit_price": 1850.00, "region": "Europe", "customer_segment": "Enterprise", "payment_method": "Invoice Net-30"},
        {"order_id": "ORD-1021", "order_date": "2026-08-24", "product_name": "SQL Server Enterprise Core License", "category": "SOFTWARE LICENSES", "quantity": 8, "unit_price": 1425.00, "region": "Asia Pacific", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1022", "order_date": "2026-08-26", "product_name": "Wireless Presentation Clicker Pro", "category": "Accessories", "quantity": 45, "unit_price": 65.00, "region": "North America", "customer_segment": "SMB", "payment_method": "Credit Card"},
        # Edge case: Missing region
        {"order_id": "ORD-1023", "order_date": "2026-08-28", "product_name": "OLED Portable Field Monitor", "category": "electronics", "quantity": 9, "unit_price": 460.00, "region": None, "customer_segment": "Mid-Market", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1024", "order_date": "2026-09-01", "product_name": "AI Workstation GPU Server Node", "category": "Cloud Hardware", "quantity": 2, "unit_price": 6800.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1025", "order_date": "2026-09-03", "product_name": "Lumbar Support Ergonomic Footrest", "category": "Office Furniture", "quantity": 55, "unit_price": 58.00, "region": "Europe", "customer_segment": "SMB", "payment_method": "Credit Card"},
        {"order_id": "ORD-1026", "order_date": "2026-09-05", "product_name": "Defender for Cloud Server Plan", "category": "Software Licenses", "quantity": 40, "unit_price": 180.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1027", "order_date": "2026-09-07", "product_name": "Precision Wireless Trackpad", "category": "ACCESSORIES", "quantity": 28, "unit_price": 115.00, "region": "Asia Pacific", "customer_segment": "Mid-Market", "payment_method": "Corporate Card"},
        # Edge case: Invalid date string
        {"order_id": "ORD-1028", "order_date": "INVALID_DATE", "product_name": "Fiber Optic Patch Cable 10m Pack", "category": "Cloud Hardware", "quantity": 20, "unit_price": 95.00, "region": "Latin America", "customer_segment": "Mid-Market", "payment_method": "Invoice Net-30"},
        {"order_id": "ORD-1029", "order_date": "2026-09-10", "product_name": "Surface Hub 3 50-inch Collaboration", "category": "Electronics", "quantity": 3, "unit_price": 4499.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1030", "order_date": "2026-09-12", "product_name": "Mobile Locking Pedestal File", "category": "Office Furniture", "quantity": 16, "unit_price": 240.00, "region": "Europe", "customer_segment": "Mid-Market", "payment_method": "Invoice Net-30"},
        # Edge case: Negative unit price
        {"order_id": "ORD-1031", "order_date": "2026-09-14", "product_name": "HDMI 2.1 Braided Cable 3m", "category": "Accessories", "quantity": 60, "unit_price": -24.00, "region": "North America", "customer_segment": "SMB", "payment_method": "Credit Card"},
        {"order_id": "ORD-1032", "order_date": "2026-09-16", "product_name": "Microsoft Fabric Capacity F16 Pack", "category": "Software Licenses", "quantity": 6, "unit_price": 1920.00, "region": "Europe", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1033", "order_date": "2026-09-18", "product_name": "Edge Gateway Industrial Appliance", "category": "Cloud Hardware", "quantity": 8, "unit_price": 890.00, "region": "Asia Pacific", "customer_segment": "Enterprise", "payment_method": "Invoice Net-30"},
        {"order_id": "ORD-1034", "order_date": "2026-09-20", "product_name": "27-inch QHD IPS Designer Display", "category": "Electronics", "quantity": 14, "unit_price": 499.00, "region": "Latin America", "customer_segment": "Mid-Market", "payment_method": "Corporate Card"},
        # Exact duplicate of ORD-1034
        {"order_id": "ORD-1034", "order_date": "2026-09-20", "product_name": "27-inch QHD IPS Designer Display", "category": "Electronics", "quantity": 14, "unit_price": 499.00, "region": "Latin America", "customer_segment": "Mid-Market", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1035", "order_date": "2026-09-22", "product_name": "Acoustic Phone Booth Pod", "category": "Office Furniture", "quantity": 2, "unit_price": 3400.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        # Edge case: Missing customer_segment
        {"order_id": "ORD-1036", "order_date": "2026-09-25", "product_name": "Encrypted Hardware Security Key", "category": "Accessories", "quantity": 80, "unit_price": 55.00, "region": "Europe", "customer_segment": None, "payment_method": "Corporate Card"},
        {"order_id": "ORD-1037", "order_date": "2026-09-28", "product_name": "Azure Purview Governance Suite", "category": "Software Licenses", "quantity": 12, "unit_price": 680.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1038", "order_date": "2026-10-01", "product_name": "NVMe All-Flash Cache Blade", "category": "cloud hardware", "quantity": 4, "unit_price": 2350.00, "region": "Asia Pacific", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1039", "order_date": "2026-10-03", "product_name": "Webcam 4K AI Auto-Framing", "category": "Electronics", "quantity": 22, "unit_price": 199.00, "region": "Europe", "customer_segment": "Mid-Market", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1040", "order_date": "2026-10-05", "product_name": "Collaborative Whiteboard Easel", "category": "Office Furniture", "quantity": 11, "unit_price": 295.00, "region": "Latin America", "customer_segment": "SMB", "payment_method": "Credit Card"},
        {"order_id": "ORD-1041", "order_date": "2026-10-07", "product_name": "Visual Studio Enterprise Annual", "category": "Software Licenses", "quantity": 15, "unit_price": 1199.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1042", "order_date": "2026-10-09", "product_name": "Laptop Privacy Filter 15-inch", "category": "Accessories", "quantity": 65, "unit_price": 48.00, "region": "Europe", "customer_segment": "SMB", "payment_method": "Credit Card"},
        {"order_id": "ORD-1043", "order_date": "2026-10-11", "product_name": "Redundant Power Distribution Unit", "category": "Cloud Hardware", "quantity": 9, "unit_price": 540.00, "region": "North America", "customer_segment": "Mid-Market", "payment_method": "Invoice Net-30"},
        {"order_id": "ORD-1044", "order_date": "2026-10-13", "product_name": "Surface Pro Flex Keyboard Pen Bundle", "category": "Electronics", "quantity": 19, "unit_price": 449.00, "region": "Asia Pacific", "customer_segment": "Enterprise", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1045", "order_date": "2026-10-15", "product_name": "Height-Adjustable Drafting Stool", "category": "Office Furniture", "quantity": 14, "unit_price": 280.00, "region": "North America", "customer_segment": "Mid-Market", "payment_method": "Invoice Net-30"},
        {"order_id": "ORD-1046", "order_date": "2026-10-17", "product_name": "Azure Synapse Dedicated SQL Pack", "category": "Software Licenses", "quantity": 5, "unit_price": 2600.00, "region": "Europe", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
        {"order_id": "ORD-1047", "order_date": "2026-10-19", "product_name": "Desktop GaN Charging Station 140W", "category": "Accessories", "quantity": 32, "unit_price": 110.00, "region": "North America", "customer_segment": "Mid-Market", "payment_method": "Corporate Card"},
        {"order_id": "ORD-1048", "order_date": "2026-10-20", "product_name": "Enterprise Firewall Security Gateway", "category": "Cloud Hardware", "quantity": 5, "unit_price": 1780.00, "region": "North America", "customer_segment": "Enterprise", "payment_method": "Wire Transfer"},
    ]
    return base_records


def evaluate_data_quality(records: List[Dict[str, Any]], required_cols: List[str] = None) -> Dict[str, Any]:
    """
    Calculates transparent Data Quality metrics and returns detailed rule breakdown.
    Formula:
      Overall Score = 0.30 * Completeness + 0.25 * Validity + 0.25 * Uniqueness + 0.20 * Consistency
    """
    if required_cols is None:
        required_cols = ["order_id", "order_date", "product_name", "category", "quantity", "unit_price", "region", "customer_segment"]

    df = pd.DataFrame(records)
    total_rows = len(df)
    if total_rows == 0:
        return {"overall_score": 0, "total_rows": 0}

    # 1. Completeness: % of non-null cells across required columns
    total_cells = total_rows * len(required_cols)
    null_counts = {col: int(df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()) for col in required_cols if col in df.columns}
    total_nulls = sum(null_counts.values())
    completeness_score = round(((total_cells - total_nulls) / total_cells) * 100, 1)

    # 2. Uniqueness: % of rows that are neither exact duplicates nor business-key duplicates
    exact_dups_mask = df.duplicated(keep="first")
    key_dups_mask = df.duplicated(subset=["order_id"], keep="first") if "order_id" in df.columns else exact_dups_mask
    exact_dup_count = int(exact_dups_mask.sum())
    key_dup_count = int(key_dups_mask.sum())
    uniqueness_score = round(((total_rows - key_dup_count) / total_rows) * 100, 1)

    # 3. Validity: valid dates and positive numeric quantity/unit_price
    parsed_dates = pd.to_datetime(df["order_date"], format="%Y-%m-%d", errors="coerce")
    invalid_date_mask = parsed_dates.isna()
    qty_num = pd.to_numeric(df["quantity"], errors="coerce")
    price_num = pd.to_numeric(df["unit_price"], errors="coerce")
    invalid_num_mask = (qty_num.isna()) | (qty_num <= 0) | (price_num.isna()) | (price_num <= 0)
    invalid_row_mask = invalid_date_mask | invalid_num_mask
    invalid_row_count = int(invalid_row_mask.sum())
    validity_score = round(((total_rows - invalid_row_count) / total_rows) * 100, 1)

    # 4. Consistency: standardized category labels (Title Case, no leading/trailing spaces, no underscores)
    canonical_categories = {"Electronics", "Office Furniture", "Software Licenses", "Accessories", "Cloud Hardware"}
    inconsistent_cat_mask = ~df["category"].astype(str).isin(canonical_categories)
    inconsistent_count = int(inconsistent_cat_mask.sum())
    consistency_score = round(((total_rows - inconsistent_count) / total_rows) * 100, 1)

    overall_score = round(
        0.30 * completeness_score +
        0.25 * validity_score +
        0.25 * uniqueness_score +
        0.20 * consistency_score,
        1
    )

    any_issue_mask = df[required_cols].isna().any(axis=1) | key_dups_mask | invalid_row_mask | inconsistent_cat_mask
    valid_rows = int((~any_issue_mask).sum())

    return {
        "overall_score": overall_score,
        "completeness_score": completeness_score,
        "uniqueness_score": uniqueness_score,
        "validity_score": validity_score,
        "consistency_score": consistency_score,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_row_count,
        "duplicate_rows": key_dup_count,
        "exact_duplicate_rows": exact_dup_count,
        "inconsistent_rows": inconsistent_count,
        "null_counts": null_counts,
    }


def run_transformation_pipeline(records: List[Dict[str, Any]], config: Dict[str, Any] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Executes modular data cleaning, type conversion, feature engineering, and validation.
    Returns (transformed_records, step_metrics).
    """
    df = pd.DataFrame(records)
    steps = []

    # Step 1: Deduplication
    in_rows = len(df)
    df = df.drop_duplicates(keep="first")
    df = df.drop_duplicates(subset=["order_id"], keep="last")
    out_rows = len(df)
    steps.append({
        "step": "Deduplicate Records & Business Keys",
        "type": "DEDUPLICATE",
        "input_rows": in_rows,
        "output_rows": out_rows,
        "removed_rows": in_rows - out_rows,
    })

    # Step 2: Category & Whitespace Normalization
    in_rows = len(df)
    df["category"] = (
        df["category"]
        .astype(str)
        .str.strip()
        .str.replace("_", " ", regex=False)
        .str.title()
    )
    steps.append({
        "step": "Normalize Whitespace & Category Taxonomy",
        "type": "CLEAN",
        "input_rows": in_rows,
        "output_rows": len(df),
        "removed_rows": 0,
    })

    # Step 3: Missing Value Imputation & Date/Numeric Validation
    in_rows = len(df)
    df["customer_segment"] = df["customer_segment"].fillna("Enterprise")
    df["region"] = df["region"].fillna("North America")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").abs()
    median_price = pd.to_numeric(df["unit_price"], errors="coerce").dropna()
    median_val = float(median_price[median_price > 0].median()) if len(median_price) > 0 else 250.0
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(median_val).abs()

    # Filter out unparseable dates
    parsed_dates = pd.to_datetime(df["order_date"], format="%Y-%m-%d", errors="coerce")
    df = df[parsed_dates.notna()].copy()
    parsed_dates = pd.to_datetime(df["order_date"], format="%Y-%m-%d")
    out_rows = len(df)
    steps.append({
        "step": "Impute Nulls, Fix Numeric Signs & Filter Invalid Dates",
        "type": "VALIDATE_AND_CLEAN",
        "input_rows": in_rows,
        "output_rows": out_rows,
        "removed_rows": in_rows - out_rows,
    })

    # Step 4: Feature Engineering (Revenue, Order Tier, Year/Month/Day)
    in_rows = len(df)
    df["revenue"] = (df["quantity"] * df["unit_price"]).round(2)
    df["order_year"] = parsed_dates.dt.year
    df["order_month"] = parsed_dates.dt.strftime("%Y-%m")
    df["order_day"] = parsed_dates.dt.day
    df["deal_size_tier"] = np.where(
        df["revenue"] >= 10000, "Strategic (>= $10k)",
        np.where(df["revenue"] >= 3000, "Mid-Tier ($3k-$10k)", "Standard (< $3k)")
    )
    steps.append({
        "step": "Derive Revenue, Temporal Dimensions & Deal Size Tier",
        "type": "FEATURE_ENGINEERING",
        "input_rows": in_rows,
        "output_rows": len(df),
        "removed_rows": 0,
    })

    return df.to_dict(orient="records"), steps


if __name__ == "__main__":
    raw = generate_synthetic_ecommerce_dataset()
    raw_q = evaluate_data_quality(raw)
    curated, step_log = run_transformation_pipeline(raw)
    curated_q = evaluate_data_quality(curated)
    print("RAW QUALITY:", json.dumps(raw_q, indent=2))
    print("STEPS:", json.dumps(step_log, indent=2))
    print("CURATED QUALITY:", json.dumps(curated_q, indent=2))
    print("TOTAL CURATED REVENUE:", round(sum(r["revenue"] for r in curated), 2))
