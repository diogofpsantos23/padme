# PADME

This is a distributed data retention simulator that evaluates different data selection strategies under constrained storage and gossip-based replication.
The prototype studies whether nodes in a decentralized key value store can retain only a subset of the data they ingest or receive through replication, while preserving downstream machine learning performance and reducing replication payload.

## Overview

Each simulation follows the same general workflow:

1. Ingests a partition of the input dataset 
2. Applies a retention policy under a storage budget 
3. Stores a bounded number of items 
4. Exchanges data with other nodes via gossip replication

The objective is to compare retention strategies in terms of:

- downstream ML performance
- network cost

## Retention Modes

The simulator currently supports four modes:

- `baseline`  
  Stores and replicates all incoming items. This mode is used as the full data reference.

- `random`  
  Applies random retention under the same storage budget as the other non baseline modes. This is used as a lightweight probabilistic baseline.

- `kcenter`  
  Applies a utility aware retention policy adaptation of k-Center Greedy. Items farther from their nearest representative receive higher utility, favoring coverage of poorly represented regions of the feature space.

- `graphcut`  
  Applies a utility aware retention policy adaptation of Graph Cut. The policy combines coverage and redundancy estimation using the representative set and a sample of retained non representative items. This mode can achieve strong ML results, but has higher computational cost.

## Configuration

Simulation runs are controlled through a JSON configuration file.

### Example

```json
{
  "path": "data/input/creditcard_train.csv",
  "mode": "k_center",
  "nodes": 5,
  "dataKeepRatios": [0.01, 0.02, 0.05, 0.10, 0.20],
  "nonRepSampleFactor": 5,
  "reportEvery": 1000,
  "ignoreColumns": ["__id", "label"]
}