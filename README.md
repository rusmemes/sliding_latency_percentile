# LatencyTracker

A high-performance, lock-free latency percentile tracker for Rust.

`LatencyTracker` is designed for extremely high write throughput in multi-threaded services. It maintains a rolling time window of latency observations and provides fast percentile queries (P50, P90, P95, P99) without storing individual samples.

## Features

- Lock-free recording path using atomics
- Thread-local batching to minimize contention
- Sliding time window (10 seconds by default)
- Percentile estimation from a fixed-size histogram
- Multi-threaded friendly
- Service isolation (multiple independent trackers)
- Constant memory usage
- High write throughput
- Compile-time configuration via environment variables

## How It Works

### Histogram-based aggregation

Latency values are mapped into histogram bins:

- Linear resolution: 10 ms by default
- Range: 0–5000 ms by default
- Values above the configured maximum latency are clamped into the final bucket

### Thread-local batching

Each thread accumulates observations locally and periodically flushes them into shared striped histograms.

Benefits:

- Fewer atomic operations
- Reduced cache contention
- Better scalability under heavy load

### Striped architecture

The tracker uses 64 independent stripes by default. Each thread is assigned a stripe based on its thread ID hash.

This significantly reduces contention during updates.

The number of stripes can be configured at compile time using the `LATENCY_TRACKER_STRIPES` environment variable. The value must be a power of two. Each thread is assigned a stripe based on its thread ID hash.

This significantly reduces contention during updates.

### Sliding Window

Data is stored in per-second buckets across a rolling 10-second window by default.

Only buckets belonging to the active window are used when computing percentiles.

The window size can be configured at compile time using the `LATENCY_TRACKER_WINDOW` environment variable.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
latency_tracker = "0.1"
```

## Usage

```rust
use latency_tracker::LatencyTracker;

let metrics = LatencyTracker::new();

metrics.record_latency_ms(120);
metrics.record_latency_ms(250);
metrics.record_latency_ms(80);

metrics.flush_thread_local();

println!("P50: {} ms", metrics.latency_ms(50));
println!("P90: {} ms", metrics.latency_ms(90));
println!("P99: {} ms", metrics.latency_ms(99));
```

## API

### Create tracker

```rust
let tracker = LatencyTracker::new();
```

### Record latency

```rust
tracker.record_latency_ms(150);
```

### Flush thread-local data

```rust
tracker.flush_thread_local();
```

Call this before reading percentiles if recent observations are still buffered in the current thread.

### Read percentiles

```rust
tracker.latency_ms(50); // P50
tracker.latency_ms(90); // P90
tracker.latency_ms(95); // P95
tracker.latency_ms(99); // P99
```

Percentiles above 99 return `0`.

## Performance Characteristics

### Write path

- O(1)
- Lock-free
- Thread-local batching

### Read path

Percentile calculation scans the histogram:

- O(number_of_bins)
- Constant memory
- Suitable for frequent monitoring

## Default Configuration

The library uses compile-time constants for its internal data structures. By default, the configuration is:

| Parameter | Environment variable | Default value |
|------------|----------------------|---------------|
| Window | `LATENCY_TRACKER_WINDOW` | `10` seconds |
| Stripes | `LATENCY_TRACKER_STRIPES` | `64` |
| Flush threshold | `LATENCY_TRACKER_FLUSH_EVERY` | `512` samples |
| Histogram step | `LATENCY_TRACKER_LINEAR_STEP` | `10` ms |
| Maximum tracked latency | `LATENCY_TRACKER_LINEAR_MAX_MS` | `5000` ms |

These values can be overridden at compile time using environment variables.

Example:
```bash
LATENCY_TRACKER_WINDOW=30
LATENCY_TRACKER_STRIPES=128
LATENCY_TRACKER_FLUSH_EVERY=1024
LATENCY_TRACKER_LINEAR_STEP=20
LATENCY_TRACKER_LINEAR_MAX_MS=10000
cargo build
```

For tests:
```bash
LATENCY_TRACKER_WINDOW=30 LATENCY_TRACKER_STRIPES=128 cargo test
````

### Configuration constraints

The following constraints are validated during compilation:

- `LATENCY_TRACKER_WINDOW` must be greater than `0`
- `LATENCY_TRACKER_STRIPES` must be greater than `0`
- `LATENCY_TRACKER_STRIPES` must be a power of two
- `LATENCY_TRACKER_FLUSH_EVERY` must be greater than `0`
- `LATENCY_TRACKER_LINEAR_STEP` must be greater than `0`
- `LATENCY_TRACKER_LINEAR_MAX_MS` must be greater than `0`
- `LATENCY_TRACKER_LINEAR_MAX_MS` must be greater than or equal to `LATENCY_TRACKER_LINEAR_STEP`
- `LATENCY_TRACKER_LINEAR_MAX_MS` must be divisible by `LATENCY_TRACKER_LINEAR_STEP`

Because these values affect array sizes and internal memory layout, they are compile-time settings, not runtime settings.

## Testing

The library includes tests covering:

- Percentile ordering
- Uniform distributions
- Heavy-tail distributions
- Multi-threaded recording
- Accuracy validation
- Stress testing with millions of samples
- Service isolation

Run tests:

```bash
cargo test
```

## License

MIT
