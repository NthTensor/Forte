//! Architecture-specific timing functions, taken from
//! <https://github.com/spence/tach>
//!
//! [`ticks`] reads a hardware counter. The tick rate depends on the
//! architecture and machine (for example, a fixed-frequency system counter on
//! Arm, and the nominal CPU frequency on x86), so ticks are only comparable
//! with other ticks read on the same machine. [`ticks_per_second`] measures the rate, and
//! [`duration_to_ticks`] and [`ticks_to_duration`] convert with it.

use core::time::Duration;
use std::sync::OnceLock;
use std::time::Instant;

/// How often a worker shares the work it has queued with idle siblings.
///
/// Work that finishes within one heartbeat is never shared, so it runs without
/// any synchronization; longer work is offered to other workers about this
/// often.
pub const HEARTBEAT: Duration = Duration::from_micros(10);

/// Returns the rate of [`ticks`], in ticks per second.
///
/// The rate is measured against [`Instant`] the first time this is called,
/// which takes about 2 milliseconds, and is cached after that.
pub fn ticks_per_second() -> u64 {
    static RATE: OnceLock<u64> = OnceLock::new();
    *RATE.get_or_init(|| {
        let start_time = Instant::now();
        let start_ticks = ticks();
        std::thread::sleep(Duration::from_millis(2));
        let elapsed_ticks = ticks().wrapping_sub(start_ticks);
        let elapsed_nanos = start_time.elapsed().as_nanos().max(1);
        let rate = u128::from(elapsed_ticks) * 1_000_000_000 / elapsed_nanos;
        u64::try_from(rate).unwrap_or(u64::MAX).max(1)
    })
}

/// Converts a duration to a number of [`ticks`].
pub fn duration_to_ticks(duration: Duration) -> u64 {
    let ticks =
        duration.as_nanos() * u128::from(ticks_per_second()) / 1_000_000_000;
    u64::try_from(ticks).unwrap_or(u64::MAX)
}

/// Converts a number of [`ticks`] to a duration.
pub fn ticks_to_duration(ticks: u64) -> Duration {
    let nanos =
        u128::from(ticks) * 1_000_000_000 / u128::from(ticks_per_second());
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// Read from the `cntvct_el0` register on Arm `AArch64`.
///
/// Returns a counter in hardware-specific units, not nanoseconds. Only compare
/// it against other values from this function on the same machine, or convert
/// it with [`ticks_to_duration`].
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn ticks() -> u64 {
    use core::arch::asm;
    let cnt: u64;
    // SAFETY: `mrs cntvct_el0` only reads the architectural virtual counter
    // register: it touches neither memory (`nomem`) nor the stack (`nostack`),
    // and writes no condition flags (`preserves_flags`).
    unsafe {
        asm!(
            "mrs {}, cntvct_el0",
            out(reg) cnt,
            options(nostack, nomem, preserves_flags)
        );
    }
    cnt
}

/// Read from rdtime on RISC-V
///
/// Returns a counter in hardware-specific units, not nanoseconds. Only compare
/// it against other values from this function on the same machine, or convert
/// it with [`ticks_to_duration`].
#[cfg(target_arch = "riscv64")]
#[inline(always)]
pub fn ticks() -> u64 {
    use core::arch::asm;
    let cnt: u64;
    // SAFETY: `rdtime` reads a timer CSR into its destination register: it
    // touches neither memory (`nomem`) nor the stack (`nostack`), and writes no
    // other register, leaving `fflags` and vector state untouched
    // (`preserves_flags`).
    unsafe {
        asm!(
            "rdtime {}",
            out(reg) cnt,
            options(nostack, nomem, preserves_flags)
        );
    }
    cnt
}

/// Read from the real-time stamp counter on x86 / x86-64.
///
/// Returns a counter in hardware-specific units, not nanoseconds. Only compare
/// it against other values from this function on the same machine, or convert
/// it with [`ticks_to_duration`].
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline(always)]
pub fn ticks() -> u64 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::_rdtsc;
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::_rdtsc;
    // SAFETY: the `rdtsc` instruction is available on every `x86`/`x86_64`
    // target Rust supports (baseline since i586), which is `_rdtsc`'s only
    // requirement.
    unsafe { _rdtsc() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions_round_trip() {
        assert!(ticks_per_second() > 0);
        assert!(duration_to_ticks(HEARTBEAT) > 0);

        let duration = Duration::from_millis(3);
        let round_trip = ticks_to_duration(duration_to_ticks(duration));
        // Converting to ticks rounds down by less than one tick.
        let one_tick = ticks_to_duration(1).max(Duration::from_nanos(1));
        assert!(duration - round_trip <= one_tick);
    }
}
