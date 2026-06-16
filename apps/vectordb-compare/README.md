# Vector Database Benchmark Comparison

A comprehensive benchmark workflow that compares the performance of vector database engines: **ApertureDB**, **Pinecone**, **Weaviate**, **Qdrant**, and **LanceDB**.

## Overview

This benchmark provides a cVerifying deepimage96_1k with k=10...
  ✓ adb: Recall@10=0.892, Precision@10=0.892 (100 queries, 0.045s)
  ✓ pc: Recall@10=0.874, Precision@10=0.874 (100 queries, 0.038s)
  ✓ wv: Recall@10=0.901, Precision@10=0.901 (100 queries, 0.052s)

================================================================================
VERIFICATION TIMING ANALYSIS
================================================================================
Configuration             Engine     Load (s)   Calc (s)   Total (s)  File Size
--------------------------------------------------------------------------------
deepimage96_1k_k10        adb        0.023      0.022      0.045      12.3KB
                          pc         0.019      0.019      0.038      11.8KB
                          wv         0.031      0.021      0.052      13.1KB

--------------------------------------------------
TIMING SUMMARY
--------------------------------------------------
Engine     Avg Time (s)    Min Time (s)    Max Time (s)
--------------------------------------------------
adb        0.045          0.045          0.045
pc         0.038          0.038          0.038
wv         0.052          0.052          0.052

✅ All engines show good accuracy (>70% recall and precision)e pipeline for:
- **Data Ingestion** - Load vector datasets into multiple databases
- **Verification** - Validate that data was ingested correctly
- **KNN Benchmarking** - Compare search performance across engines

### Supported Vector Databases

| Engine | Code | Description |
|--------|------|-------------|
| **ApertureDB** | `adb` | High-performance multimodal database |
| **Pinecone** | `pc` | Vector database as a service |
| **Weaviate** | `wv` | Open-source vector search engine |
| **Qdrant** | `qd` | High-performance vector similarity engine |
| **LanceDB** | `ldb` | Serverless vector database built on Lance format |

### Datasets

- **`deepimage96`** - Deep learning image features (96 dimensions)
- **`yfcc100m`** - YFCC100M dataset features (4096 dimensions)

## Architecture

The benchmark uses a **modular architecture** with dynamic loading:

### Core Components

- **`ingest.py`** - Data ingestion pipeline with engine selection
- **`verify_ingestion.py`** - Verify data was loaded correctly
- **`knn.py`** - KNN performance benchmarking
- **`utils.py`** - Shared utilities with dynamic imports

### Engine-Specific Modules

Each database engine is implemented in separate modules with isolated imports:

```
├── ingest_aperturedb.py     # ApertureDB ingestion
├── ingest_pinecone.py       # Pinecone ingestion
├── ingest_weaviate.py       # Weaviate ingestion
├── ingest_qdrant.py         # Qdrant ingestion
├── ingest_lancedb.py        # LanceDB ingestion
├── knn_aperturedb.py        # ApertureDB KNN
├── knn_pinecone.py          # Pinecone KNN
├── knn_weaviate.py          # Weaviate KNN
├── knn_qdrant.py            # Qdrant KNN
├── knn_lancedb.py           # LanceDB KNN
├── verify_aperturedb.py     # ApertureDB verification
├── verify_pinecone.py       # Pinecone verification
├── verify_weaviate.py       # Weaviate verification
├── verify_qdrant.py         # Qdrant verification
└── verify_lancedb.py        # LanceDB verification
```### Key Features

#### 🔧 **Engine Selection**
Run benchmarks on specific engines using the `-engines` flag:

```bash
# Run all engines
python knn.py -engines "adb,pc,wv,qd,ldb"

# Run only ApertureDB and Pinecone
python knn.py -engines "adb,pc"

# Run single engine
python knn.py -engines "adb"
```

#### 🛡️ **Graceful Degradation**
Missing dependencies don't crash the entire benchmark:

```
Failed to import engine pc: No module named 'pinecone'
Skipping pc - required dependencies not available
✓ ADB ingestion completed
✓ QD ingestion completed
```

#### 📦 **Import Isolation**
Engine dependencies are only imported when used, preventing conflicts.

#### 🔄 **Docker Compose Support**
Easy deployment with environment-based configuration.

## Quick Start

### Using Docker Compose

1. **Set up environment**:
```bash
cp .env.example .env
# Edit .env with your credentials
```

2. **Run the benchmark**:
```bash
docker compose up --build
```

### Manual Usage

1. **Data Ingestion**:
```bash
python ingest.py -engines "adb,pc,wv,qd,ldb" -source "deepimage96"
```

2. **Verify Ingestion**:
```bash
python verify_ingestion.py -engines "adb,pc,wv,qd,ldb" -source "deepimage96"
```

3. **Run KNN Benchmark**:
```bash
python knn.py -engines "adb,pc,wv,qd,ldb" -dataset "deepimage96"
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ENGINES` | Comma-separated engine list | `"adb,pc,wv,qd,ldb"` |
| `SOURCE` | Dataset source | `"deepimage96"` |
| `MINIMAL` | Run minimal test (1k vectors) | `false` |
| `TOTAL_QUERIES` | Number of KNN queries | `100` |
| `KNN_SAMPLES` | K-nearest neighbors | `10` |
| `CONCURRENCIES` | Thread counts to test | `"16,32,64"` |
| `LOAD_BATCH_SIZE` | Ingestion batch size | `1000` |
| `LOAD_NUM_THREADS` | Ingestion threads | `32` |

### Database Credentials

**ApertureDB**:
```bash
DB_HOST=your-aperturedb-host
DB_PASS=your-password
```

**Pinecone**:
```bash
PINECONE_API_KEY=your-api-key
```

**Weaviate**:
```bash
WEAVIATE_CLUSTER_URL=https://your-cluster.weaviate.cloud
WEAVIATE_API_KEY=your-api-key
```

**Qdrant**:
```bash
QDRANT_CLUSTER_URL=https://your-cluster.qdrant.io:6333
QDRANT_API_KEY=your-api-key
```

**LanceDB**:
```bash
LANCEDB_URI=./lancedb
LANCEDB_API_KEY=your-api-key  # Optional for cloud
```

## Benchmark Methodology

### 1. Data Ingestion
- Load vector datasets into each database
- Use appropriate indexing (HNSW for most engines)
- Batch processing for optimal performance
- Progress tracking and timing

### 2. Verification
- Verify correct number of vectors ingested
- Check data accessibility across all size tiers
- Report any discrepancies

### 3. KNN Performance Testing
- Run K-nearest neighbor searches
- Test multiple concurrency levels
- Measure response times and throughput
- Generate performance statistics

### 4. Results Analysis
- Export results to CSV format
- Generate performance percentiles
- Compare engines across metrics
- Optional S3 upload and Slack notifications

## Results and Outputs

### Performance Metrics
- **Response Time**: P50, P90, P95, P99 percentiles
- **Throughput**: Queries per second at different concurrencies
- **Accuracy**: KNN result verification
- **Scalability**: Performance across data sizes (1K, 10K, 100K, 1M)

### Output Files
- `output/knn_comp.csv` - Benchmark results
- `output/results_*.txt` - Detailed result sets
- `output/knn_verification_report.json` - Ground truth verification report
- Performance logs and timing data

## KNN Result Verification

The benchmark includes **automatic result verification** against ground truth data to ensure accuracy and quality of KNN search results.

### 🎯 **Ground Truth Verification**

After each KNN benchmark run, the system automatically:

1. **Loads ground truth data** from the dataset's `.neighbors` and `.distances` arrays
2. **Compares each engine's results** against the true nearest neighbors
3. **Calculates accuracy metrics** for each engine and configuration
4. **Generates comprehensive reports** with detailed analysis

### 📊 **Accuracy Metrics**

For each engine, the verification calculates:

- **Recall@k**: Percentage of ground truth neighbors found in top-k results
  ```
  Recall@k = |intersection(predicted_k, true_k)| / k
  ```

- **Precision@k**: Percentage of predicted neighbors that are actually correct
  ```
  Precision@k = |intersection(predicted_k, true_neighbors)| / |predicted_k|
  ```

### 🔍 **Verification Process**

```bash
# KNN benchmark automatically includes verification
python knn.py -engines "adb,pc,wv" -knn_samples 10

# Example output:
Loading ground truth data...
Ground truth loaded: 10000 queries with 100 neighbors each

================================================================================
KNN RESULTS VERIFICATION AGAINST GROUND TRUTH
================================================================================

Verifying deepimage96_1k with k=10...
  ✓ adb: Recall@10=0.892, Precision@10=0.892 (100 queries)
  ✓ pc: Recall@10=0.874, Precision@10=0.874 (100 queries)
  ✓ wv: Recall@10=0.901, Precision@10=0.901 (100 queries)

✅ All engines show good accuracy (>70% recall and precision)
```

### 📋 **Verification Report**

The system generates `output/knn_verification_report.json` containing:

```json
{
  "summary": {
    "total_engine_configs": 3,
    "avg_recall_across_all": 0.889,
    "avg_precision_across_all": 0.889,
    "avg_load_time": 0.024,
    "avg_calc_time": 0.021,
    "avg_total_time": 0.045,
    "accuracy_issues": 0,
    "sanity_issues": 0
  },
  "detailed": {
    "deepimage96_1k_k10": {
      "adb": {
        "recall_at_k": 0.892,
        "precision_at_k": 0.892,
        "num_queries": 100,
        "load_time": 0.023,
        "calc_time": 0.022,
        "total_time": 0.045,
        "individual_recalls": [...],
        "individual_precisions": [...]
      }
    }
  },
  "timing": {
    "ground_truth_load_time": 1.234,
    "engine_processing": {
      "deepimage96_1k_k10": {
        "adb": {
          "load_time": 0.023,
          "calc_time": 0.022,
          "total_time": 0.045,
          "file_size_bytes": 12589
        }
      }
    }
  }
}
```

### ✅ **Quality Checks**

The verification performs multiple quality checks:

#### **Ground Truth Validation**
- Compares predicted neighbors against true nearest neighbors
- Flags engines with recall < 70% or precision < 70%
- Provides per-query accuracy breakdown

#### **Sanity Checks**
- Verifies each query returns exactly `k` results
- Checks for duplicate neighbors in results
- Validates neighbor IDs are valid integers

#### **Cross-Engine Analysis**
- Compares performance across all engines
- Identifies outliers or problematic configurations
- Provides engine ranking by accuracy

#### **Verification Timing Analysis**
- Measures time to load result files for each engine
- Tracks calculation time for accuracy metrics
- Shows file sizes and processing efficiency
- Provides timing summaries across engines and configurations
- Helps identify performance bottlenecks in verification process

### 🛠️ **Testing the Verification**

You can test the verification system independently:

```bash
# Run verification test with mock data
python test_verification.py
```

This creates mock result files with different accuracy levels and verifies the system correctly identifies good vs poor results.

## Advanced Usage

### Custom Configurations

```bash
# Minimal test with single engine
python knn.py -engines "adb" -minimal true -total_queries 10

# High-throughput test
python knn.py -engines "adb,pc" -concurrencies "32,64,128" -total_queries 1000

# Different dataset
python ingest.py -engines "adb,wv" -source "yfcc100m" -load_batch_size 500
```

### Adding New Engines

1. Create engine modules:
   - `ingest_newengine.py`
   - `knn_newengine.py`
   - `verify_newengine.py`

2. Add engine mapping in main files:
   - `ingest.py`
   - `knn.py`
   - `verify_ingestion.py`

3. Add connector function in `utils.py`

## Requirements

### Python Dependencies
- `numpy`, `h5py`, `urllib3`, `pandas`
- `aperturedb` (for ApertureDB)
- `pinecone` (for Pinecone)
- `weaviate-client` (for Weaviate)
- `qdrant-client` (for Qdrant)
- `lancedb` (for LanceDB)

### Infrastructure
- Access to target vector databases
- Sufficient memory for dataset loading
- Network connectivity for cloud services

## Troubleshooting

### Common Issues

**Missing Dependencies**:
```
Failed to import engine pc: No module named 'pinecone'
```
*Solution*: Install missing packages or exclude engine from test

**Authentication Errors**:
```
Exception: PINECONE_API_KEY not set
```
*Solution*: Set required environment variables

**Memory Issues**:
*Solution*: Use `-minimal true` for smaller datasets

### Debug Mode
```bash
# Enable verbose logging
python knn.py -engines "adb" -verbose true

# Check specific engine
python verify_ingestion.py -engines "pc" -source "deepimage96"
```

---

For detailed implementation information, see the individual module documentation and source code.
