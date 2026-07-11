# GH-16021 Implementation Plan

## Goal

Add an experimental querier option that partitions requests to the store when they contain too many blocks according to the partitioner's internal counting policy.

Requirements:

- Add `-querier.max-blocks-per-store-request`.
- Default to `0`, which disables partitioning and preserves existing behavior.
- Partition requested blocks so each partition can be assigned to any eligible replica in the block's replication set.
- Count only blocks with compaction level 3 or higher toward the configured maximum.
- Allow any number of compaction level 1 and 2 blocks in each partition.
- Keep compaction-level awareness private to the partitioning implementation and its focused tests.
- Do not implement new span logging or metrics in this change. They will be added separately.

## Configuration

Add to `pkg/querier/querier.go`:

```go
MaxBlocksPerStoreRequest int `yaml:"max_blocks_per_store_request" category:"experimental"`
```

Register:

```text
-querier.max-blocks-per-store-request
```

Semantics:

- `0`: disabled.
- Positive values: maximum counted blocks in one request to one store client.
- Negative values: invalid configuration.

The flag help and generated configuration documentation must explain that compaction levels 1 and 2 do not count toward the limit. Do not expose that policy through query execution APIs, logs, metrics, or general-purpose request types.

Pass the value from `NewBlocksStoreQueryableFromConfig()` to `newBlocksStoreReplicationSet()`.

## Request representation

Change `BlocksStoreSet.GetClientsFor()` from returning one client map to returning partitions:

```go
type blocksStoreRequestPartitions []map[BlocksStoreClient][]ulid.ULID
```

Use a private named type if it reduces repeated nested iteration, but keep it generic. It must not contain compaction-level fields or methods.

Each outer slice entry is a partition. Each map groups the blocks assigned to each client in that partition. A client may occur in multiple partitions, allowing multiple RPCs to the same replica.

Required invariants:

1. Every input block appears exactly once across all partitions.
2. Every block is assigned to an eligible, non-excluded replica.
3. A client appears at most once in each partition.
4. A client may appear in multiple partitions.
5. Every client entry satisfies the configured maximum according to the private counting policy.
6. Empty partitions are not returned.
7. With the option disabled, return one partition containing the same grouping produced today.

## Partitioning implementation

Keep all compaction-level logic in `pkg/querier/blocks_store_replicated_set.go`, or in one focused private helper file next to it.

`GetClientsFor()` already has the necessary inputs:

- Bucket-index block metadata.
- Ring ownership and dynamic replication.
- Retry exclusions.
- Preferred availability zones.
- Load-balancing strategy.
- Store-gateway client pool.

Build partitions while resolving ownership rather than selecting one replica for every block and splitting afterward. Overflow partitions must be able to use another eligible replica.

For each block:

1. Resolve its replication set with the existing ring options, including dynamic replication.
2. Apply existing exclusion, randomization, and preferred-zone behavior.
3. Find an existing partition and eligible replica where the block can be added.
4. If none can accept it, create a partition and assign the block to an eligible replica.
5. Increment partition capacity only for blocks with `CompactionLevel >= 3`.
6. Blocks at compaction levels 1 and 2 can be added without consuming capacity or forcing a new partition.

Treat `CompactionLevel == 0` as not counted. The bucket-index field is omitted when zero and can represent unavailable metadata; the requirement is to cap only known level 3 and higher blocks.

When several eligible placements exist:

- Preserve preferred-zone behavior.
- Preserve random load balancing as a tie-breaker.
- Prefer a placement with available capacity.
- Prefer replicas with fewer planned requests or less counted load so overflow can spread across replicas.
- Fall back to multiple partitions on one replica when ownership or exclusions require it.

Do not require all blocks in a partition to share a replica set. The validity condition is per client entry: each assigned block must be owned by that client.

Resolve each store address through the client pool once per `GetClientsFor()` call and reuse the client for all partitions targeting that address.

## Query execution

Update `queryFunc` and the series, label, and search fetch methods to accept all partitions in one call.

All `(partition, client)` entries in one consistency attempt must execute concurrently. Do not execute outer partitions as sequential waves. The store-gateway concurrency gate is expected to provide downstream backpressure.

Existing fetch loops should change from:

```go
for client, blockIDs := range clients {
```

to nested iteration over partitions and their client maps, launching one goroutine per client entry.

This applies to:

- `fetchSeriesFromStores()`
- `fetchLabelNamesFromStore()`
- `fetchLabelValuesFromStore()`
- `fetchSearchLabelNamesFromStore()`
- `fetchSearchLabelValuesFromStore()`

Preserve existing stream lifetime, cancellation, warning aggregation, result merging, and retry behavior.

## Consistency retries

Treat all partitions returned by one `GetClientsFor()` call as one consistency attempt.

After `queryF` returns:

- Aggregate queried-block hints across every request.
- Flatten partitions when updating `attemptedBlocks`.
- Record the selected remote address for each assigned block.
- Avoid duplicate addresses in an individual block's exclusion list.
- Retry only blocks still reported missing by the consistency tracker.
- Repartition missing blocks against remaining eligible replicas on the next attempt.
- Keep retry bounds based on the dynamic replication factor, not partition count.

Keep `storesHit` semantics unchanged: count distinct remote addresses, not requests or partitions.

## Tests

### Configuration tests

Add tests for:

- Default value is `0`.
- Positive values are accepted.
- Negative values fail validation.

### Replication-set and partitioner tests

Extend `pkg/querier/blocks_store_replicated_set_test.go` with focused cases:

- Disabled option returns one partition and preserves current assignment behavior.
- Exactly the configured number of counted blocks remains in one request.
- One additional counted block creates another partition.
- Any number of level 1 and level 2 blocks does not create partitions.
- Mixed level 1, 2, and 3+ blocks observes the limit only for level 3+ blocks.
- Compaction level 0 does not count.
- Every input block appears exactly once.
- The same client can occur in multiple partitions.
- Overflow is distributed to another eligible replica when possible.
- A single eligible replica receives multiple partitions.
- Excluded replicas are never selected.
- Preferred availability zones are still honored.
- Random load balancing remains supported.
- Dynamic replication factors are honored.
- Ring buffer reuse remains safe.
- Client lookup is reused for repeated partitions targeting the same address.

Tests outside the focused partitioning tests should not inspect compaction levels.

### Queryable tests

Update mocks and existing expectations for the partitioned return type, then add coverage for:

- Multiple partitions execute within one consistency attempt.
- Multiple requests can target the same client.
- Queried-block hints are aggregated across partitions.
- Only missing blocks are retried.
- Retry exclusions apply per block.
- Distinct-store accounting is unchanged when one store receives multiple requests.
- Errors cancel sibling requests and clean up streams as before.

Cover all request families:

- Series.
- Label names.
- Label values.
- Streaming label-name search.
- Streaming label-value search.

For search tests, verify that multiple streams targeting the same client survive header setup, merge correctly, and are all closed on failure.

## Documentation and generated files

After implementing the flag:

1. Run `make reference-help doc` and include generated configuration changes.
2. Add `-querier.max-blocks-per-store-request` to the Querier section of `docs/sources/mimir/configure/about-versioning.md`.
3. Add a changelog entry once the PR number is known.

## Validation

Run at least:

```text
go test ./pkg/querier/...
make format
make reference-help doc
```

Then rerun affected tests after formatting and generation.

Use `testing.T.Context()` in new tests where a test-scoped context is needed. Do not add dependencies or unrelated refactors.

## Deferred work

Do not add span logging or metrics in this implementation. A follow-up will define how partitioning is represented in debug spans and observability without exposing the internal compaction-level policy.
