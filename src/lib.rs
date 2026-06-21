use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

const WINDOW: u64 = 10;
const BUCKETS: usize = WINDOW as usize + 1;
const STRIPES: usize = 64;
const FLUSH_EVERY: u32 = 512;

const LINEAR_MAX_MS: u64 = 5000;
const LINEAR_STEP: u64 = 10;
const LINEAR_BINS: usize = (LINEAR_MAX_MS / LINEAR_STEP) as usize;
const HISTOGRAM_BINS: usize = LINEAR_BINS + 1;

static NEXT_SERVICE_ID: AtomicUsize = AtomicUsize::new(1);

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

struct MetricsState {
    stripes: Vec<[Bucket; BUCKETS]>,
    start: Instant,
}

impl MetricsState {
    fn new() -> Self {
        let mut stripes = Vec::with_capacity(STRIPES);

        for _ in 0..STRIPES {
            stripes.push(std::array::from_fn(|_| Bucket::new()));
        }

        Self {
            stripes,
            start: Instant::now(),
        }
    }

    #[inline(always)]
    fn now_sec(&self) -> u64 {
        self.start.elapsed().as_secs()
    }

    #[inline(always)]
    fn bucket(&self, stripe: usize, b_idx: usize) -> &Bucket {
        &self.stripes[stripe][b_idx]
    }
}

#[derive(Clone)]
pub struct MetricsService {
    id: usize,
    state: Arc<MetricsState>,
}

impl MetricsService {
    pub fn new() -> Self {
        Self {
            id: NEXT_SERVICE_ID.fetch_add(1, Ordering::Relaxed),
            state: Arc::new(MetricsState::new()),
        }
    }

    #[inline(always)]
    pub fn record_latency(&self, lat_ms: u64) {
        let now = self.state.now_sec();
        let bin = latency_to_bin(lat_ms);
        let stripe = get_stripe();

        BATCHES.with(|batches| {
            let mut batches = batches.borrow_mut();
            let batch = batches.entry(self.id).or_insert_with(Batch::new);

            if batch.start_sec == 0 || batch.start_sec != now {
                if batch.count > 0 {
                    self.flush_batch(batch, stripe);
                }

                batch.hist = [0; HISTOGRAM_BINS];
                batch.count = 0;
                batch.start_sec = now;
            }

            batch.hist[bin] += 1;
            batch.count += 1;

            if batch.count >= FLUSH_EVERY {
                self.flush_batch(batch, stripe);

                batch.hist = [0; HISTOGRAM_BINS];
                batch.count = 0;
            }
        });
    }

    pub fn flush_thread_local(&self) {
        BATCHES.with(|batches| {
            let mut batches = batches.borrow_mut();

            if let Some(batch) = batches.get_mut(&self.id) {
                if batch.count > 0 {
                    let stripe = get_stripe();

                    self.flush_batch(batch, stripe);

                    batch.hist = [0; HISTOGRAM_BINS];
                    batch.count = 0;
                    batch.start_sec = 0;
                }
            }
        });
    }

    pub fn latency_ms(&self, percentile: u8) -> u64 {
        if percentile > 99 {
            return 0;
        }

        let now = self.state.now_sec();
        let mut total_hist = [0u64; HISTOGRAM_BINS];
        let mut total_count = 0u64;

        for stripe in &self.state.stripes {
            for bucket in stripe {
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

        LINEAR_MAX_MS
    }

    #[inline(always)]
    fn flush_batch(&self, batch: &Batch, stripe: usize) {
        let b_idx = (batch.start_sec as usize) % BUCKETS;

        flush(
            &self.state,
            &batch.hist,
            batch.start_sec,
            stripe,
            b_idx,
        );
    }
}

impl Default for MetricsService {
    fn default() -> Self {
        Self::new()
    }
}

struct Batch {
    hist: [u64; HISTOGRAM_BINS],
    count: u32,
    start_sec: u64,
}

impl Batch {
    fn new() -> Self {
        Self {
            hist: [0; HISTOGRAM_BINS],
            count: 0,
            start_sec: 0,
        }
    }
}

thread_local! {
    static STRIPE: RefCell<Option<usize>> = const { RefCell::new(None) };
    static BATCHES: RefCell<HashMap<usize, Batch>> = RefCell::new(HashMap::new());
}

#[inline(always)]
fn get_stripe() -> usize {
    STRIPE.with(|cell| {
        let mut stripe = cell.borrow_mut();

        match *stripe {
            Some(value) => value,
            None => {
                let mut hasher = DefaultHasher::new();

                std::thread::current().id().hash(&mut hasher);

                let value = (hasher.finish() as usize) & (STRIPES - 1);

                *stripe = Some(value);

                value
            }
        }
    })
}

#[inline(always)]
fn latency_to_bin(lat_ms: u64) -> usize {
    if lat_ms <= LINEAR_MAX_MS {
        (lat_ms / LINEAR_STEP) as usize
    } else {
        HISTOGRAM_BINS - 1
    }
}

#[inline]
fn bin_to_latency_ms(bin: usize) -> u64 {
    if bin < LINEAR_BINS {
        (bin as u64) * LINEAR_STEP
    } else {
        LINEAR_MAX_MS
    }
}

#[inline(always)]
fn flush(
    state: &MetricsState,
    hist: &[u64; HISTOGRAM_BINS],
    epoch: u64,
    stripe: usize,
    b_idx: usize,
) {
    let bucket = state.bucket(stripe, b_idx);

    let prev = bucket.epoch.load(Ordering::Relaxed);

    if prev != epoch {
        if bucket
            .epoch
            .compare_exchange(prev, epoch, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn latency_to_bin_linear() {
        assert_eq!(latency_to_bin(0), 0);
        assert_eq!(latency_to_bin(10), 1);
        assert_eq!(latency_to_bin(100), 10);
        assert_eq!(latency_to_bin(1000), 100);
        assert_eq!(latency_to_bin(5000), 500);
    }

    #[test]
    fn latency_to_bin_clamps_max() {
        assert_eq!(
            latency_to_bin(LINEAR_MAX_MS),
            HISTOGRAM_BINS - 1
        );

        assert_eq!(
            latency_to_bin(LINEAR_MAX_MS * 10),
            HISTOGRAM_BINS - 1
        );
    }

    #[test]
    fn latency_to_bin_monotonic() {
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
        assert_eq!(
            bin_to_latency_ms(HISTOGRAM_BINS - 1),
            LINEAR_MAX_MS
        );
    }

    #[test]
    fn invalid_percentile_returns_zero() {
        let metrics = MetricsService::new();

        assert_eq!(metrics.latency_ms(100), 0);
        assert_eq!(metrics.latency_ms(255), 0);
    }

    #[test]
    fn percentile_ordering() {
        let metrics = MetricsService::new();

        for value in 1..=1000 {
            metrics.record_latency(value);
        }

        metrics.flush_thread_local();

        let p50 = metrics.latency_ms(50);
        let p90 = metrics.latency_ms(90);
        let p95 = metrics.latency_ms(95);
        let p99 = metrics.latency_ms(99);

        assert!(p50 <= p90);
        assert!(p90 <= p95);
        assert!(p95 <= p99);
    }

    #[test]
    fn constant_distribution() {
        let metrics = MetricsService::new();

        for _ in 0..20_000 {
            metrics.record_latency(500);
        }

        metrics.flush_thread_local();

        let p50 = metrics.latency_ms(50);
        let p90 = metrics.latency_ms(90);
        let p99 = metrics.latency_ms(99);

        assert!(p50 >= 490);
        assert!(p90 >= 490);
        assert!(p99 >= 490);
    }

    #[test]
    fn uniform_distribution() {
        let metrics = MetricsService::new();

        for value in 1..=1000 {
            for _ in 0..100 {
                metrics.record_latency(value);
            }
        }

        metrics.flush_thread_local();

        let p50 = metrics.latency_ms(50);
        let p90 = metrics.latency_ms(90);
        let p99 = metrics.latency_ms(99);

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
        let metrics = MetricsService::new();

        for _ in 0..99_000 {
            metrics.record_latency(100);
        }

        for _ in 0..1_000 {
            metrics.record_latency(5000);
        }

        metrics.flush_thread_local();

        let p50 = metrics.latency_ms(50);
        let p90 = metrics.latency_ms(90);
        let p99 = metrics.latency_ms(99);

        println!("p50={p50} p90={p90} p99={p99}");

        assert!(p50 < 1000, "p50={p50}");
        assert!(p90 < 1000, "p90={p90}");
        assert!(p99 >= 4900, "p99={p99}");
    }

    #[test]
    fn flush_thread_local_makes_data_visible() {
        let metrics = MetricsService::new();

        for _ in 0..10 {
            metrics.record_latency(500);
        }

        metrics.flush_thread_local();

        assert!(metrics.latency_ms(99) > 0);
    }

    #[test]
    fn multi_threaded_recording() {
        let metrics = MetricsService::new();
        let mut handles = Vec::new();

        for _ in 0..16 {
            let metrics = metrics.clone();

            handles.push(thread::spawn(move || {
                for _ in 0..10_000 {
                    metrics.record_latency(500);
                }

                metrics.flush_thread_local();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let p99 = metrics.latency_ms(99);

        assert!(
            p99 >= 490,
            "p99={p99}"
        );
    }

    #[test]
    fn percentile_close_to_real_distribution() {
        let metrics = MetricsService::new();
        let mut values = Vec::new();

        for _ in 0..100_000 {
            let value = rand::random_range(1..1000);

            values.push(value);

            metrics.record_latency(value);
        }

        metrics.flush_thread_local();

        values.sort_unstable();

        let real_p99 = values[(values.len() * 99) / 100];

        let measured = metrics.latency_ms(99);

        let error = (measured as i64 - real_p99 as i64).abs();

        assert!(
            error < 100,
            "real_p99={real_p99}, measured={measured}, error={error}"
        );
    }

    #[test]
    fn million_samples_stress() {
        let metrics = MetricsService::new();

        for i in 0..1_000_000 {
            metrics.record_latency((i % 1000 + 1) as u64);
        }

        metrics.flush_thread_local();

        let p99 = metrics.latency_ms(99);

        assert!(
            p99 > 900,
            "p99={p99}"
        );
    }

    #[test]
    fn services_are_isolated() {
        let first = MetricsService::new();
        let second = MetricsService::new();

        for _ in 0..10_000 {
            first.record_latency(10);
        }

        for _ in 0..10_000 {
            second.record_latency(1000);
        }

        first.flush_thread_local();
        second.flush_thread_local();

        let first_p99 = first.latency_ms(99);
        let second_p99 = second.latency_ms(99);

        assert!(first_p99 < 100, "first_p99={first_p99}");
        assert!(second_p99 >= 900, "second_p99={second_p99}");
    }

    #[test]
    fn services_are_isolated_in_same_thread_before_flush() {
        let first = MetricsService::new();
        let second = MetricsService::new();

        first.record_latency(10);
        second.record_latency(1000);

        first.flush_thread_local();
        second.flush_thread_local();

        let first_p99 = first.latency_ms(99);
        let second_p99 = second.latency_ms(99);

        assert!(first_p99 < 100, "first_p99={first_p99}");
        assert!(second_p99 >= 900, "second_p99={second_p99}");
    }

    #[test]
    fn write_requests_per_second() {
        let metrics_service = Arc::new(MetricsService::new());
        const THREADS: usize = 16;

        let stop = Arc::new(AtomicBool::new(false));
        let ops = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();

        for _ in 0..THREADS {
            let stop = stop.clone();
            let ops = ops.clone();
            let service_clone = metrics_service.clone();
            handles.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    service_clone.record_latency(500);
                    ops.fetch_add(1, Ordering::Relaxed);
                }
                service_clone.flush_thread_local();
            }));
        }

        let start = Instant::now();

        thread::sleep(Duration::from_secs(10));

        stop.store(true, Ordering::Relaxed);

        for h in handles {
            h.join().unwrap();
        }

        println!("{}", metrics_service.latency_ms(99));

        let total = ops.load(Ordering::Relaxed);

        println!("ops={}", total);
        println!("ops/sec={}", total as f64 / start.elapsed().as_secs_f64());
    }
}