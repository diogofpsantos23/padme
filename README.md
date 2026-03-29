# PADME

This is a distributed data retention simulator that evaluates different data selection strategies under constrained storage and gossip-based replication.
Supported strategies include baseline, random sampling, and PADME (a utility-driven policy).

## Overview

Each node in the system:

1. Ingests a partition of the input dataset
2. Applies a retention policy under a storage budget
3. Stores a bounded number of items
4. Exchanges data with other nodes via gossip replication

The objective is to compare different retention strategies in terms of:
- downstream ML performance
- network cost

## Retention Modes

The system supports three modes:

- `baseline`  
  Admits all incoming items.

- `random`  
  Retains items uniformly at random under the storage limit (each item has equal probability of being kept).

- `padme`  
  Retains items based on a utility function that favors less redundant and more representative samples.

## Configuration

All runs are controlled via a JSON configuration file.

### Example

```json
{
  "path": "data/input/creditcard_train.csv",
  "mode": "baseline",
  "nodes": 8,
  "padmeBinBalanceGamma": 0.3,
  "padmeBinBalanceMin": 0.8,
  "padmeBinBalanceMax": 1.2,
  "vectorTransform": "zscore",
  "dataKeepRatios": [0.01, 0.02, 0.05, 0.10, 0.20],
  "reportEvery": 1000,
  "ignoreColumns": ["__id", "label", "Time", "Amount"]
}
```

### Parameters

- `path` (string)  
  Path to the input CSV dataset.

- `mode` (string)  
  Retention policy: `baseline`, `random`, or `padme`.

- `nodes` (int)  
  Number of simulated nodes (1 <= value <= 36).
  
- `padmeBinBalanceGamma` (double, default=0.3)  
  Controls the strength of representativity balancing.

- `padmeBinBalanceMin` (double, default=0.8)  
  Lower bound for bin balancing factor.

- `padmeBinBalanceMax` (double, default=1.2)  
  Upper bound for bin balancing factor.

- `dataKeepRatios` (double[])  
  Storage ratios to evaluate (0 < value ≤ 1).

- `vectorTransform` (string)  
  Feature normalization method:
  - `zscore`
  - `log_zscore`
  - `robust`
  - `log_robust`

- `ignoreColumns` (string[])  
  Columns excluded from the feature vector.

- `reportEvery` (int)  
  Logging frequency during ingestion (per number of items seen).

## How to Run

### Build

```bash
mvn clean package
```

### Execute

```bash
java -cp target/classes padme.Main --config src/main/resources/config.json
```

## Output

Results are written to:

```
data/output/<dataset>/<mode>/<ratio>/
```

Each run produces:

### Per-node datasets

```
<mode>_node0.csv
<mode>_node1.csv
...
```

### Metrics

```json
{
  "keepRatio": 0.1,
  "totalBytesSent": 12345678
}
```

## Analysis

The `analysis/` directory contains Jupyter notebooks for evaluating the results produced by the simulator.

- One notebook per dataset
- Loads simulation outputs and evaluates both system-level metrics and ML performance 

Requirements:

```bash
pip install -r analysis/requirements.txt
```

## Contact

Diogo Santos (up202108747@up.pt)