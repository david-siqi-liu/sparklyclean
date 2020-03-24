# SparklyClean

Scala-based, Spark implementation of [Distributed Data Deduplication](http://www.vldb.org/pvldb/vol9/p864-chu.pdf) that guarantees optimal data distribution strategy in distributed data deduplication tasks with minimal customization required.

## Overview

**Data deduplication** is the process of identifying tuples in a dataset that refer to the same real world entity. It is very costly to be ran on a single machine - for a dataset with n tuples, this is a O(n^2) operation since each tuple needs to be compared with every other tuple in the dataset.

A commonly used technique to avoid this quadratic complexity is **blocking**. Basically, blocking functions partition the dataset into disjoint blocks, and only tuples within the same blocks are compared. For example, suppose we possess some domain-knowledge, and introduces a blocking function (e.g., account type) that partitions the dataset into two equally-large blocks of size n/2, then the number of pair-wise comparisons becomes 2 * (n/2)^2 = n^2 / 2, half of the original. Unfortunately, this introduces false-negatives (e.g., when account type is null), so in practice, multiple blocking functions are often used together.

With the help of **distributed systems** such as Hadoop MapReduce and Spark, we can parallelize this intensivie data deduplication task. However, there are several challenges that need to be addressed:

1. In addition to **computation cost** that exists in the traditional, centralized setting, distributed algorithms also incur **communication cost**, which essentially is the network transfer and I/O cost for sending, receiving and storing data from each worker
2. It is typical to have **data skew** in a distributed task. Ideally, we want all workers to perform roughly the same amount of work so we don't encounter a "bottleneck" issue
3. The distribution strategy should be able to handle multiple blocking functions. Specifically, we need to ensure that each tuple pair is compared exactly once, not multiple times if they exists in the same block according to multiple blocking functions

## DisDedup

*DisDedup* is the distributed framework proposed by the paper. It aims to minimize the maximum cost (both computation and communication) across all workers in the network.

At a very high level, the framework does the following:

1. In the **setup** phase, compute the amount of work (i.e., number of pair-wise comparisons) within each block, produced by each blocking function. Then, assign workers to each block based on its workload
   - Larger blocks get assigned multiple workers (multi-reducer blocks)
   - Smaller blocks each gets asssigned a single worker (either deterministically or randomly)
2. In the **map** phase, multiple-reducer blocks' reducers get handled via the [Triangle Distribution Strategy]()
3. In the **partition** phase, tuples are sent to their designated worker(s)
4. In the **reduce** phase, tuples that belong to the same blocks (within each worker) are compared against one another
   - For each tuple pair, similarity scores (e.g., edit-distance) are computed for each column
5.  Similarity features are outputted for further analysis (e.g., train machine learning algorithms)

Please refer to the original paper for details and proofs on optimality.

## Project Organization

```
├── impl
│   ├── BKV       						<- Block-Key-Value class
│   ├── Compare        				<- Custom comparison functions for creating similarity features
│   ├── DisDedupMapper      	<- Mapper function
│   └── DisDedupPartitioner		<- Partitioner function
│   └── DisDedupReducer				<- Reducer function
│   └── Setup									<- Setup function
│   └── Util									<- Utility functions
├── GenerateLabeledPoints			<- Main class, conduct pairwise comparisons and output similarity features
├── TrainDupClassifier				<- Train a classification model on labeled dataset
├── ApplyDupClassifier				<- Apply the trained model on unlabeled dataset
```

