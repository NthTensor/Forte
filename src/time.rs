//! Architecture-specific timing functions, taken from
//! <https://github.com/spence/tach>

/// Read from the `cntvct_el0` register on Arm `AArch64`.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn ticks() -> u64 {
    use core::arch::asm;
    let cnt: u64;
    // SAFETY: `mrs cntvct_el0` only reads the architectural virtual counter
    // register and does not touch memory or the stack.
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
