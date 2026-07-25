use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
#[derive(Debug, Default)]
pub struct Metrics {
    pub commands: AtomicU64,
    pub errors: AtomicU64,
    pub timeouts: AtomicU64,
    pub total_latency_micros: AtomicU64,
}
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub commands: u64,
    pub errors: u64,
    pub timeouts: u64,
    pub total_latency_micros: u64,
}
impl Metrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            commands: self.commands.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            total_latency_micros: self.total_latency_micros.load(Ordering::Relaxed),
        }
    }
}
pub struct Backoff {
    pub base: Duration,
    pub max: Duration,
}
impl Default for Backoff {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(25),
            max: Duration::from_secs(2),
        }
    }
}
impl Backoff {
    pub fn delay(&self, attempt: usize) -> Duration {
        self.base
            .saturating_mul(1u32.checked_shl(attempt.min(16) as u32).unwrap_or(u32::MAX))
            .min(self.max)
    }
}
pub struct CircuitBreaker {
    failures: AtomicU64,
    opened_at: std::sync::Mutex<Option<Instant>>,
    threshold: u64,
    cooldown: Duration,
}
impl CircuitBreaker {
    pub fn new(threshold: u64, cooldown: Duration) -> Self {
        Self {
            failures: AtomicU64::new(0),
            opened_at: std::sync::Mutex::new(None),
            threshold: threshold.max(1),
            cooldown,
        }
    }
    pub fn allow(&self) -> bool {
        let mut o = self.opened_at.lock().unwrap();
        if let Some(t) = *o {
            if t.elapsed() < self.cooldown {
                return false;
            }
            *o = None;
            self.failures.store(0, Ordering::Relaxed)
        }
        true
    }
    pub fn success(&self) {
        self.failures.store(0, Ordering::Relaxed);
        *self.opened_at.lock().unwrap() = None
    }
    pub fn failure(&self) {
        if self.failures.fetch_add(1, Ordering::Relaxed) + 1 >= self.threshold {
            *self.opened_at.lock().unwrap() = Some(Instant::now())
        }
    }
}
pub fn elapsed_micros(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}
