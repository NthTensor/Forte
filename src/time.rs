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
    // SAFETY: `rdtime` reads a timer CSR into a general-purpose register and does not access
    // Rust memory.
    unsafe {
        asm!(
            "rdtime {}",
            out(reg) cnt,
            options(nostack, nomem, preserves_flags)
        );
    }
    cnt
}

/// Read from the real-time stamp counter on windows
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline(always)]
pub fn ticks() -> u64 {
    // SAFETY: `_rdtsc` emits the CPU counter read instruction and has no Rust memory safety
    // preconditions.
    unsafe { core::arch::x86_64::_rdtsc() }
}
