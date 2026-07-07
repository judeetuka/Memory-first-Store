use mfs_core::s3fifo::{S3FifoAdmissionExperiment, S3FifoCache, S3FifoConfig};
use std::time::Instant;

const WORKING_SET: u64 = 100_000;
const OPS: usize = 1_000_000;

#[derive(Clone, Copy)]
struct Workload {
    zipf_alpha: f64,
    cap_percent: f64,
}

#[derive(Clone, Copy)]
struct Variant {
    name: &'static str,
    shards: Option<usize>,
    hot_ratio_percent: Option<usize>,
    ghost_ratio_percent: Option<usize>,
    small_to_main_threshold: Option<u8>,
    admission_filter: Option<bool>,
    admission_experiment: Option<S3FifoAdmissionExperiment>,
}

const WORKLOADS: [Workload; 3] = [
    Workload {
        zipf_alpha: 1.00,
        cap_percent: 0.05,
    },
    Workload {
        zipf_alpha: 1.10,
        cap_percent: 0.10,
    },
    Workload {
        zipf_alpha: 1.50,
        cap_percent: 0.25,
    },
];

const VARIANTS: [Variant; 18] = [
    Variant::new("default"),
    Variant::new("single_shard").with_shards(1),
    Variant::new("threshold2").with_small_to_main_threshold(2),
    Variant::new("single_shard_threshold2")
        .with_shards(1)
        .with_small_to_main_threshold(2),
    Variant::new("hot80").with_hot_ratio_percent(80),
    Variant::new("hot95").with_hot_ratio_percent(95),
    Variant::new("ghost25").with_ghost_ratio_percent(25),
    Variant::new("ghost100").with_ghost_ratio_percent(100),
    Variant::new("hot80_ghost100")
        .with_hot_ratio_percent(80)
        .with_ghost_ratio_percent(100),
    Variant::new("admission")
        .with_shards(1)
        .with_admission_filter(true),
    Variant::new("admission_ghost0")
        .with_shards(1)
        .with_ghost_ratio_percent(0)
        .with_admission_filter(true),
    Variant::new("admission_ghost25")
        .with_shards(1)
        .with_ghost_ratio_percent(25)
        .with_admission_filter(true),
    Variant::new("exp_adm_gate0_ghost25")
        .with_shards(1)
        .with_ghost_ratio_percent(25)
        .with_admission_filter(true)
        .with_admission_experiment(S3FifoAdmissionExperiment::CapacityGate {
            maximum_frequency: 0,
        }),
    Variant::new("exp_adm_gate1_ghost25")
        .with_shards(1)
        .with_ghost_ratio_percent(25)
        .with_admission_filter(true)
        .with_admission_experiment(S3FifoAdmissionExperiment::CapacityGate {
            maximum_frequency: 1,
        }),
    Variant::new("exp_adm_wide_floor4096_ghost25")
        .with_shards(1)
        .with_ghost_ratio_percent(25)
        .with_admission_filter(true)
        .with_admission_experiment(S3FifoAdmissionExperiment::WideSketch {
            min_width: 4096,
            sample_size_floor: 4096,
        }),
    Variant::new("exp_adm_packed4_floor4096_ghost25")
        .with_shards(1)
        .with_ghost_ratio_percent(25)
        .with_admission_filter(true)
        .with_admission_experiment(S3FifoAdmissionExperiment::Packed4Bit {
            min_width: 4096,
            sample_size_floor: 4096,
        }),
    Variant::new("exp_adm_doorkeeper_floor4096_ghost25")
        .with_shards(1)
        .with_ghost_ratio_percent(25)
        .with_admission_filter(true)
        .with_admission_experiment(S3FifoAdmissionExperiment::Doorkeeper {
            min_width: 4096,
            sample_size_floor: 4096,
        }),
    Variant::new("exp_adm_two_counter_ghost25")
        .with_shards(1)
        .with_ghost_ratio_percent(25)
        .with_admission_filter(true)
        .with_admission_experiment(S3FifoAdmissionExperiment::TwoCounterDecay {
            min_width: 4096,
            sample_size_floor: 4096,
        }),
];

impl Variant {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            shards: None,
            hot_ratio_percent: None,
            ghost_ratio_percent: None,
            small_to_main_threshold: None,
            admission_filter: None,
            admission_experiment: None,
        }
    }

    const fn with_shards(mut self, shards: usize) -> Self {
        self.shards = Some(shards);
        self
    }

    const fn with_hot_ratio_percent(mut self, percent: usize) -> Self {
        self.hot_ratio_percent = Some(percent);
        self
    }

    const fn with_ghost_ratio_percent(mut self, percent: usize) -> Self {
        self.ghost_ratio_percent = Some(percent);
        self
    }

    const fn with_small_to_main_threshold(mut self, threshold: u8) -> Self {
        self.small_to_main_threshold = Some(threshold);
        self
    }

    const fn with_admission_filter(mut self, enabled: bool) -> Self {
        self.admission_filter = Some(enabled);
        self
    }

    const fn with_admission_experiment(mut self, experiment: S3FifoAdmissionExperiment) -> Self {
        self.admission_experiment = Some(experiment);
        self
    }

    fn build_cache(self, capacity: usize) -> S3FifoCache<u64, ()> {
        let mut config = S3FifoConfig::new(capacity);
        if let Some(shards) = self.shards {
            config = config.with_shards(shards);
        }
        if let Some(percent) = self.hot_ratio_percent {
            config = config.with_hot_ratio_percent(percent);
        }
        if let Some(percent) = self.ghost_ratio_percent {
            config = config.with_ghost_ratio_percent(percent);
        }
        if let Some(threshold) = self.small_to_main_threshold {
            config = config.with_small_to_main_threshold(threshold);
        }
        if let Some(enabled) = self.admission_filter {
            config = config.with_admission_filter(enabled);
        }
        if let Some(experiment) = self.admission_experiment {
            config = config.with_admission_experiment(experiment);
        }
        S3FifoCache::with_config(config)
    }
}

fn main() {
    println!(
        "mfs_s3fifo_tuning working_set={WORKING_SET} ops={OPS} variants={}",
        VARIANTS.len()
    );
    for workload in WORKLOADS {
        let capacity = ((WORKING_SET as f64) * workload.cap_percent) as usize;
        let keys = zipfian_sequence(OPS, WORKING_SET, workload.zipf_alpha);
        println!(
            "workload zipf={:.2} cap={:.0}% capacity={capacity}",
            workload.zipf_alpha,
            workload.cap_percent * 100.0,
        );
        println!(
            "{:<40} {:>10} {:>10} {:>14}",
            "variant", "hit_ratio", "misses", "ops/s"
        );
        for variant in VARIANTS {
            let (hits, elapsed) = measure_variant(variant, capacity, &keys);
            let misses = OPS - hits;
            let hit_ratio = 100.0 * hits as f64 / OPS as f64;
            let ops_per_second = OPS as f64 / elapsed.as_secs_f64();
            println!(
                "{:<40} {:>9.3}% {:>10} {:>14.0}",
                variant.name, hit_ratio, misses, ops_per_second
            );
        }
    }
}

fn measure_variant(
    variant: Variant,
    capacity: usize,
    keys: &[u64],
) -> (usize, std::time::Duration) {
    let cache = variant.build_cache(capacity);
    let mut hits = 0usize;
    let start = Instant::now();
    for &key in keys {
        if cache.read_with(&key, |_| ()).is_some() {
            hits += 1;
        } else {
            cache.insert(key, ());
        }
    }
    (hits, start.elapsed())
}

fn zipfian_sequence(n: usize, support: u64, alpha: f64) -> Vec<u64> {
    let support_usize = support as usize;
    let mut cumulative = Vec::with_capacity(support_usize);
    let mut sum = 0.0f64;
    for i in 1..=support {
        sum += 1.0 / (i as f64).powf(alpha);
        cumulative.push(sum);
    }

    let mut rng = 0x9E37_79B9_7F4A_7C1Fu64 ^ alpha.to_bits();
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let u = (rng as f64 / u64::MAX as f64).clamp(1e-12, 1.0 - 1e-12);
        let target = u * sum;
        let idx = cumulative
            .partition_point(|&weight| weight < target)
            .min(support_usize - 1);
        out.push(idx as u64);
    }
    out
}
