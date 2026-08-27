use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HybridTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
}

impl HybridTimestamp {
    pub fn new(physical_ms: u64, logical: u32) -> Self {
        Self {
            physical_ms,
            logical,
        }
    }

    pub fn zero() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct HybridClock {
    last: HybridTimestamp,
}

impl HybridClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn now(&self) -> HybridTimestamp {
        self.last
    }

    pub fn tick(&mut self) -> HybridTimestamp {
        let physical_ms = current_physical_ms();
        if physical_ms > self.last.physical_ms {
            self.last = HybridTimestamp::new(physical_ms, 0);
        } else {
            self.last.logical = self.last.logical.saturating_add(1);
        }
        self.last
    }

    pub fn observe(&mut self, remote: HybridTimestamp) -> HybridTimestamp {
        let physical_ms = current_physical_ms();
        let max_physical = physical_ms
            .max(self.last.physical_ms)
            .max(remote.physical_ms);
        let logical = if max_physical == self.last.physical_ms && max_physical == remote.physical_ms
        {
            self.last.logical.max(remote.logical).saturating_add(1)
        } else if max_physical == self.last.physical_ms {
            self.last.logical.saturating_add(1)
        } else if max_physical == remote.physical_ms {
            remote.logical.saturating_add(1)
        } else {
            0
        };

        self.last = HybridTimestamp::new(max_physical, logical);
        self.last
    }
}

fn current_physical_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_monotonically_advances() {
        let mut clock = HybridClock::new();

        let first = clock.tick();
        let second = clock.tick();

        assert!(second >= first);
    }

    #[test]
    fn observe_remote_timestamp_advances_local_clock() {
        let mut clock = HybridClock::new();
        let local = clock.tick();
        let remote = HybridTimestamp::new(local.physical_ms.saturating_add(10), 7);

        let observed = clock.observe(remote);

        assert!(observed > remote);
        assert_eq!(observed.physical_ms, remote.physical_ms);
        assert_eq!(observed.logical, 8);
    }
}
