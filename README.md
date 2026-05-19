# High-Throughput E-Commerce Catalog Ingestion & Quality Guardrail Pipeline

## Project Overview
This repository contains a high-volume data engineering pipeline designed to simulate upstream retail vendor catalog submissions. It establishes automated data quality (DQ) guardrails, explicit schema enforcement, a Dead-Letter Queue (DLQ) isolation framework, and an automated task execution tree.

The core objective of this project is to implement a production **Dead-Letter Queue (DLQ)** pattern using PySpark and PyArrow. This pattern ensures that high-velocity transaction payloads are audited and partitioned in real time—routing clean data to downstream analytical silver storage while isolating corrupt records for root-cause diagnostics, maintaining high system availability and idempotency.

---

## Architecture & Data Flow



1. **Ingress Layer (`data_generator.py`)**: Streams 500,000 synthetic e-commerce catalog transactions split into memory-efficient batches to prevent local Out-of-Memory (OOM) anomalies. It intentionally injects structural and logical defects (~5% corruption threshold).
2. **Compute & Quality Layer (`transform.py`)**: Initializes a local Spark session, enforces an explicit StructType schema, applies robust validation gates, and branches the data stream.
3. **Storage Layer (`data/`)**: Commits audited records into optimized, Snappy-compressed columnar Parquet assets via PyArrow.
4. **Orchestration & Lifecycle Control (`orchestrator.py`)**: A centralized DAG controller managing automated execution flow, sequential task dependencies, real-time process logging, and automated post-processing file movement.

---

## Data Quality Guardrails (Business Logic)
The pipeline screens incoming data against 5 explicit validation gates:
1. **Critical Identity**: `product_id` must not be null.
2. **Schema Integrity**: `price` must parse cleanly into a numeric float (catches alpha string corruptions).
3. **Inventory Validity**: `quantity` must be a positive integer (rejects negative inventory records).
4. **Temporal Consistency**: `transaction_date` cannot be a timestamp positioned in the future.
5. **Categorical Data Drift Gate (Custom Rule)**: Enforces strict allowed-list verification against standard business categories (`Electronics`, `Apparel`, `Home & Kitchen`, `Automotive`, `Books`, `Beauty`). Any unexpected categories introduced by upstream drift are automatically isolated.

---

## Local Environment & Production Edge Cases Handled
During local development and testing under **Python 3.12 on a Windows host environment**, critical platform-specific blockers were systematically resolved:

### 1. Hadoop Winutils File System Lock Bypass
* **Issue**: PySpark’s native `.parquet()` write action relies on Hadoop storage binaries, throwing a critical `FileNotFoundException` due to an unconfigured `HADOOP_HOME` and missing `winutils.exe` on local Windows filesystems.
* **Resolution**: Decoupled the storage driver layer from Java binaries by leveraging `pyarrow` as a pure Python storage adapter. Transformed rows are shuttled across Spark's memory bridge (`toPandas()`) and committed safely without OS directory-locking issues.

### 2. Python 3.12 Distutils Deprecation
* **Issue**: Invoking the Pandas memory bridge threw a `ModuleNotFoundError: No module named 'distutils'` because the legacy `distutils` library was completely stripped out of core Python 3.12 runtimes.
* **Resolution**: Environment patched by bootstrapping modern `setuptools` directly inside the virtual environment container, restoring library compatibility.

### 3. Ingress Directory Clutter & Reprocessing Prevention
* **Issue**: Leaving raw processed CSV files in the landing zone causes downstream duplicate processing runs and skews analytical metrics.
* **Resolution**: Implemented an automated `shutil` file-archiving task at the end of the DAG pipeline sequence. Processed payloads are sliced cleanly from the `ingress/` directory to the `archive/` directory upon successful compute finalization.

---

## Execution Metrics Output

```text
🚀 [DAG TRIGGER]: Initializing pipeline run sequence

============================================================
 🟢 [DAG TASK START]: Extract_And_Inject_Raw_Payload
============================================================
📦 Successfully flushed 500000/500000 records to disk.
✅ Ingestion baseline complete. 

============================================================
 ✅ [DAG TASK SUCCESS]: Extract_And_Inject_Raw_Payload completed cleanly.
============================================================

============================================================
 🟢 [DAG TASK START]: PySpark_Enforce_Schema_And_Isolate_DLQ
============================================================
📖 Ingesting raw incoming payload...
🔄 Bridging distributed datasets to secure file-system driver...        
💾 Committing verified records to Silver Layer (Parquet)
⚠️ Committing defective rows to DLQ Target (Parquet)

========================================
      PIPELINE EXECUTION METRICS
========================================
 Total Rows Ingested:      500,000
 Clean Rows Curated:       474,906
 Defective Rows Isolated:  25,094
========================================

✅ PySpark Engine shut down cleanly.
============================================================
 ✅ [DAG TASK SUCCESS]: PySpark_Enforce_Schema_And_Isolate_DLQ completed cleanly.
============================================================

============================================================
 🟢 [DAG TASK START]: Archive_Processed_Raw_Payload
============================================================
📦 Successfully moved raw landing file to Archive Layer:
   From: .../data/ingress/catalog_transactions_20260520.csv
   To:   .../data/archive/catalog_transactions_20260520.csv

============================================================
 ✅ [DAG TASK SUCCESS]: Archive_Processed_Raw_Payload completed cleanly.
============================================================

🏁 [DAG SUCCESS]: End-to-end pipeline run finalized successfully.

## Project Directory Structure

```text
amazon_rbs_pipeline/
├── .gitignore               # Excludes python caches, local virtual environments, and heavy raw datasets
├── README.md                # System documentation, validation metrics, and architecture guide
├── data/
│   ├── archive/             # Historical storage zone for raw payloads post-execution (Reprocessing protection)
│   ├── dlq_defects/         # Dead-Letter Queue housing isolated schema drift and rule violations (Parquet format)
│   ├── ingress/             # Real-time landing zone for incoming vendor catalog logs (CSV format)
│   └── silver_clean/        # Production-ready, Snappy-compressed curated catalog datasets (Parquet format)
└── scripts/
    ├── data_generator.py    # Ingress simulation layer producing 500k-row batch testing assets
    ├── orchestrator.py      # Multi-task sequential DAG manager (Simulates Apache Airflow runtime behavior)
    └── transform.py         # PySpark distributed processing core applying strict catalog DQ gates