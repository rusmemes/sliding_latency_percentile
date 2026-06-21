use std::env;
use std::fs;
use std::path::PathBuf;

const DEFAULT_WINDOW: u64 = 10;
const DEFAULT_STRIPES: usize = 64;
const DEFAULT_FLUSH_EVERY: u32 = 512;
const DEFAULT_LINEAR_MAX_MS: u64 = 5000;
const DEFAULT_LINEAR_STEP: u64 = 10;

fn read_u64(name: &str, default: u64) -> u64 {
    println!("cargo:rerun-if-env-changed={name}");

    match env::var(name) {
        Ok(value) => value.parse::<u64>().unwrap_or_else(|_| {
            panic!("{name} must be a positive integer, got `{value}`");
        }),
        Err(_) => default,
    }
}

fn read_usize(name: &str, default: usize) -> usize {
    println!("cargo:rerun-if-env-changed={name}");

    match env::var(name) {
        Ok(value) => value.parse::<usize>().unwrap_or_else(|_| {
            panic!("{name} must be a positive integer, got `{value}`");
        }),
        Err(_) => default,
    }
}

fn read_u32(name: &str, default: u32) -> u32 {
    println!("cargo:rerun-if-env-changed={name}");

    match env::var(name) {
        Ok(value) => value.parse::<u32>().unwrap_or_else(|_| {
            panic!("{name} must be a positive integer, got `{value}`");
        }),
        Err(_) => default,
    }
}

fn main() {
    let window = read_u64("LATENCY_TRACKER_WINDOW", DEFAULT_WINDOW);
    let stripes = read_usize("LATENCY_TRACKER_STRIPES", DEFAULT_STRIPES);
    let flush_every = read_u32("LATENCY_TRACKER_FLUSH_EVERY", DEFAULT_FLUSH_EVERY);
    let linear_max_ms = read_u64("LATENCY_TRACKER_LINEAR_MAX_MS", DEFAULT_LINEAR_MAX_MS);
    let linear_step = read_u64("LATENCY_TRACKER_LINEAR_STEP", DEFAULT_LINEAR_STEP);

    assert!(window > 0, "LATENCY_TRACKER_WINDOW must be greater than 0");

    assert!(
        stripes > 0,
        "LATENCY_TRACKER_STRIPES must be greater than 0"
    );

    assert!(
        stripes.is_power_of_two(),
        "LATENCY_TRACKER_STRIPES must be a power of two because stripe selection uses a bit mask"
    );

    assert!(
        flush_every > 0,
        "LATENCY_TRACKER_FLUSH_EVERY must be greater than 0"
    );

    assert!(
        linear_max_ms > 0,
        "LATENCY_TRACKER_LINEAR_MAX_MS must be greater than 0"
    );

    assert!(
        linear_step > 0,
        "LATENCY_TRACKER_LINEAR_STEP must be greater than 0"
    );

    assert!(
        linear_max_ms >= linear_step,
        "LATENCY_TRACKER_LINEAR_MAX_MS must be greater than or equal to LATENCY_TRACKER_LINEAR_STEP"
    );

    assert_eq!(
        linear_max_ms % linear_step,
        0,
        "LATENCY_TRACKER_LINEAR_MAX_MS must be divisible by LATENCY_TRACKER_LINEAR_STEP"
    );

    let config = format!(
        r#"const WINDOW: u64 = {window};
        const BUCKETS: usize = WINDOW as usize + 1;
        const STRIPES: usize = {stripes};
        const FLUSH_EVERY: u32 = {flush_every};

        const LINEAR_MAX_MS: u64 = {linear_max_ms};
        const LINEAR_STEP: u64 = {linear_step};
        const LINEAR_BINS: usize = (LINEAR_MAX_MS / LINEAR_STEP) as usize;
        const HISTOGRAM_BINS: usize = LINEAR_BINS + 1;
        "#
    );

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR must be set"));

    fs::write(out_dir.join("config.rs"), config).expect("failed to write generated config");
}
