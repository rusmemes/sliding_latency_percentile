use std::cell::Cell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

const WINDOW: u64 = 10;
const BUCKETS: usize = WINDOW as usize + 1;
const STRIPES: usize = 64;
const FLUSH_EVERY: u32 = 64;

const HISTOGRAM_BINS: usize = 152;
const LINEAR_MAX_MS: u64 = 1000;
const LINEAR_STEP: u64 = 10;
const LINEAR_BINS: usize = (LINEAR_MAX_MS / LINEAR_STEP) as usize;
const MAX_LATENCY_MS: u64 = 5000;

#[repr(align(64))]
struct Bucket {
    epoch: AtomicU64,
    hist: [AtomicU64; HISTOGRAM_BINS],
}

impl Bucket {
    fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            hist: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

static METRICS: OnceLock<Vec<[Bucket; BUCKETS]>> = OnceLock::new();

fn get_metrics() -> &'static Vec<[Bucket; BUCKETS]> {
    METRICS.get_or_init(|| {
        let mut v = Vec::with_capacity(STRIPES);
        for _ in 0..STRIPES {
            v.push(std::array::from_fn(|_| Bucket::new()));
        }
        v
    })
}

fn get_bucket(stripe: usize, b_idx: usize) -> &'static Bucket {
    &get_metrics()[stripe][b_idx]
}

thread_local! {
    static STRIPE: Cell<usize> = const { Cell::new(usize::MAX) };
    static LAST_SEC: Cell<u64> = const { Cell::new(0) };

    static BATCH_HIST: Cell<[u64; HISTOGRAM_BINS]> = const { Cell::new([0; HISTOGRAM_BINS]) };
    static BATCH_COUNT: Cell<u32> = const { Cell::new(0) };
    static BATCH_START_SEC: Cell<u64> = const { Cell::new(0) };
}

static START: OnceLock<Instant> = OnceLock::new();

#[inline(always)]
fn now_sec() -> u64 {
    let start = START.get_or_init(Instant::now);
    let secs = start.elapsed().as_secs();
    LAST_SEC.with(|c| {
        let last = c.get();
        if secs != last { c.set(secs); }
        secs
    })
}

#[inline(always)]
fn get_stripe() -> usize {
    STRIPE.with(|c| {
        let mut s = c.get();
        if s == usize::MAX {
            let mut h = DefaultHasher::new();
            std::thread::current().id().hash(&mut h);
            s = (h.finish() as usize) & (STRIPES - 1);
            c.set(s);
        }
        s
    })
}

#[inline(always)]
fn latency_to_bin(lat_ms: u64) -> usize {
    if lat_ms <= LINEAR_MAX_MS {
        (lat_ms / LINEAR_STEP) as usize
    } else if lat_ms >= MAX_LATENCY_MS {
        HISTOGRAM_BINS - 1
    } else {
        let log_bins_start = LINEAR_BINS;
        let remaining_bins = HISTOGRAM_BINS - log_bins_start - 1;
        let log_val = ((lat_ms as f64).log2() - 10.0).max(0.0);
        let scale = (log_val * remaining_bins as f64 / 3.0) as usize;
        log_bins_start + scale.min(remaining_bins)
    }
}

#[inline]
fn bin_to_latency_ms(bin: usize) -> u64 {
    if bin < LINEAR_BINS {
        (bin as u64) * LINEAR_STEP
    } else if bin == HISTOGRAM_BINS - 1 {
        MAX_LATENCY_MS
    } else {
        let log_bins_start = LINEAR_BINS;
        let offset = bin - log_bins_start;
        let remaining = (HISTOGRAM_BINS - log_bins_start - 1).max(1);
        let exp = 10.0 + (offset as f64) * 3.0 / remaining as f64;
        (2u64.pow(exp as u32)).min(MAX_LATENCY_MS)
    }
}

#[inline(always)]
pub fn record_latency(lat_ms: u64) {
    let now = now_sec();
    let bin = latency_to_bin(lat_ms);
    let s = get_stripe();
    let b = (now as usize) % BUCKETS;

    BATCH_HIST.with(|hist_cell| {
        BATCH_COUNT.with(|cnt_cell| {
            BATCH_START_SEC.with(|start_cell| {
                let mut hist = hist_cell.get();
                let mut cnt = cnt_cell.get();
                let mut batch_start = start_cell.get();

                if batch_start == 0 || batch_start != now {
                    if cnt > 0 {
                        flush(&hist, batch_start, s, b);
                    }
                    hist = [0; HISTOGRAM_BINS];
                    cnt = 0;
                    batch_start = now;
                }

                hist[bin] += 1;
                cnt += 1;

                if cnt >= FLUSH_EVERY {
                    flush(&hist, batch_start, s, b);
                    hist = [0; HISTOGRAM_BINS];
                    cnt = 0;
                }

                hist_cell.set(hist);
                cnt_cell.set(cnt);
                start_cell.set(batch_start);
            });
        });
    });
}

#[inline(always)]
fn flush(hist: &[u64; HISTOGRAM_BINS], epoch: u64, stripe: usize, b_idx: usize) {
    let bucket = get_bucket(stripe, b_idx);

    let prev = bucket.epoch.load(Ordering::Relaxed);

    if prev != epoch {
        if bucket.epoch.compare_exchange(prev, epoch, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            for (i, &val) in hist.iter().enumerate() {
                if val > 0 {
                    bucket.hist[i].store(val, Ordering::Relaxed);
                }
            }
            return;
        }
    }

    for (i, &val) in hist.iter().enumerate() {
        if val > 0 {
            bucket.hist[i].fetch_add(val, Ordering::Relaxed);
        }
    }
}

pub fn flush_thread_local() {
    BATCH_HIST.with(|hist_cell| {
        BATCH_COUNT.with(|cnt_cell| {
            BATCH_START_SEC.with(|start_cell| {
                let hist = hist_cell.get();
                let cnt = cnt_cell.get();
                let start = start_cell.get();
                if cnt > 0 {
                    let s = get_stripe();
                    let b = (start as usize) % BUCKETS;
                    flush(&hist, start, s, b);
                    hist_cell.set([0; HISTOGRAM_BINS]);
                    cnt_cell.set(0);
                    start_cell.set(0);
                }
            });
        });
    });
}

pub fn latency_ms(percentile: u8) -> u64 {
    if percentile > 99 {
        return 0;
    }
    let now = now_sec();
    let mut total_hist = [0u64; HISTOGRAM_BINS];
    let mut total_count = 0u64;

    let metrics = get_metrics();

    for stripe in metrics.iter() {
        for bucket in stripe.iter() {
            let epoch = bucket.epoch.load(Ordering::Acquire);
            if now.saturating_sub(epoch) > WINDOW {
                continue;
            }

            for (i, bin) in bucket.hist.iter().enumerate() {
                let val = bin.load(Ordering::Relaxed);
                total_hist[i] += val;
                total_count += val;
            }
        }
    }

    if total_count == 0 {
        return 0;
    }

    let target = ((total_count as f64) * (percentile as f64 / 100.0)) as u64 + 1;
    let mut cumulative = 0u64;

    for (i, &count) in total_hist.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            return bin_to_latency_ms(i);
        }
    }

    MAX_LATENCY_MS
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::thread;

    fn clear_metrics() {
        let metrics = get_metrics();

        for stripe in metrics {
            for bucket in stripe {
                bucket.epoch.store(0, Ordering::Relaxed);

                for bin in &bucket.hist {
                    bin.store(0, Ordering::Relaxed);
                }
            }
        }
    }

    fn reset() {
        clear_metrics();

        BATCH_HIST.with(|c| c.set([0; HISTOGRAM_BINS]));
        BATCH_COUNT.with(|c| c.set(0));
        BATCH_START_SEC.with(|c| c.set(0));
    }

    #[test]
    fn latency_to_bin_linear() {
        reset();
        assert_eq!(latency_to_bin(0), 0);
        assert_eq!(latency_to_bin(10), 1);
        assert_eq!(latency_to_bin(100), 10);
        assert_eq!(latency_to_bin(1000), 100);
    }

    #[test]
    fn latency_to_bin_clamps_max() {
        reset();
        assert_eq!(
            latency_to_bin(MAX_LATENCY_MS),
            HISTOGRAM_BINS - 1
        );

        assert_eq!(
            latency_to_bin(MAX_LATENCY_MS * 10),
            HISTOGRAM_BINS - 1
        );
    }

    #[test]
    fn latency_to_bin_monotonic() {
        reset();
        let mut prev = latency_to_bin(0);

        for value in 1..10_000 {
            let current = latency_to_bin(value);

            assert!(
                current >= prev,
                "value={value} current={current} prev={prev}"
            );

            prev = current;
        }
    }

    #[test]
    fn bin_to_latency_monotonic() {
        reset();
        let mut prev = 0;

        for bin in 0..HISTOGRAM_BINS {
            let value = bin_to_latency_ms(bin);

            assert!(
                value >= prev,
                "bin={bin} value={value} prev={prev}"
            );

            prev = value;
        }
    }

    #[test]
    fn last_bin_maps_to_max_latency() {
        reset();
        assert_eq!(
            bin_to_latency_ms(HISTOGRAM_BINS - 1),
            MAX_LATENCY_MS
        );
    }

    #[test]
    fn invalid_percentile_returns_zero() {
        reset();
        assert_eq!(latency_ms(100), 0);
        assert_eq!(latency_ms(255), 0);
    }

    #[test]
    fn percentile_ordering() {
        reset();
        for value in 1..=1000 {
            record_latency(value);
        }

        flush_thread_local();

        let p50 = latency_ms(50);
        let p90 = latency_ms(90);
        let p95 = latency_ms(95);
        let p99 = latency_ms(99);

        assert!(p50 <= p90);
        assert!(p90 <= p95);
        assert!(p95 <= p99);
    }

    #[test]
    fn constant_distribution() {
        reset();
        for _ in 0..20_000 {
            record_latency(500);
        }

        flush_thread_local();

        let p50 = latency_ms(50);
        let p90 = latency_ms(90);
        let p99 = latency_ms(99);

        assert!(p50 >= 490);
        assert!(p90 >= 490);
        assert!(p99 >= 490);
    }

    #[test]
    fn uniform_distribution() {
        reset();
        for value in 1..=1000 {
            for _ in 0..100 {
                record_latency(value);
            }
        }

        flush_thread_local();

        let p50 = latency_ms(50);
        let p90 = latency_ms(90);
        let p99 = latency_ms(99);

        assert!(
            (400..=600).contains(&p50),
            "p50={p50}"
        );

        assert!(
            p90 > 800,
            "p90={p90}"
        );

        assert!(
            p99 > 950,
            "p99={p99}"
        );
    }

    #[test]
    fn heavy_tail_distribution() {
        reset();
        for _ in 0..99_000 {
            record_latency(10);
        }

        for _ in 0..1_000 {
            record_latency(1000);
        }

        flush_thread_local();

        let p50 = latency_ms(50);
        let p90 = latency_ms(90);
        let p99 = latency_ms(99);

        println!("p50={p50} p90={p90} p99={p99}");
        assert!(p50 < 100, "p50={p50}");
        assert!(p90 < 100, "p90={p90}");
        assert!(p99 >= 900, "p99={p99}");
    }

    #[test]
    fn flush_thread_local_makes_data_visible() {
        reset();
        for _ in 0..10 {
            record_latency(500);
        }

        flush_thread_local();

        assert!(latency_ms(99) > 0);
    }

    #[test]
    fn multi_threaded_recording() {
        reset();
        let mut handles = Vec::new();

        for _ in 0..16 {
            handles.push(thread::spawn(|| {
                for _ in 0..10_000 {
                    record_latency(500);
                }

                flush_thread_local();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let p99 = latency_ms(99);

        assert!(
            p99 >= 490,
            "p99={p99}"
        );
    }

    #[test]
    fn percentile_close_to_real_distribution() {
        reset();
        let mut values = Vec::new();

        for _ in 0..100_000 {
            let value = rand::random_range(1..1000);

            values.push(value);

            record_latency(value);
        }

        flush_thread_local();

        values.sort_unstable();

        let real_p99 =
            values[(values.len() * 99) / 100];

        let measured =
            latency_ms(99);

        let error =
            (measured as i64 - real_p99 as i64).abs();

        assert!(
            error < 100,
            "real_p99={real_p99}, measured={measured}, error={error}"
        );
    }

    #[test]
    fn million_samples_stress() {
        reset();
        for i in 0..1_000_000 {
            record_latency(
                (i % 1000 + 1) as u64
            );
        }

        flush_thread_local();

        let p99 = latency_ms(99);

        assert!(
            p99 > 900,
            "p99={p99}"
        );
    }
}