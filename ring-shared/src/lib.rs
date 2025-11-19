#![allow(clippy::missing_safety_doc)]
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering, compiler_fence};
use std::{fs::OpenOptions, io, os::unix::io::AsRawFd, path::Path};

use libc::{
    FUTEX_PRIVATE_FLAG, FUTEX_WAIT, FUTEX_WAKE, MAP_SHARED, PROT_READ, PROT_WRITE, SYS_futex,
    c_int, c_void, ftruncate, mmap, munmap, off_t, timespec,
};

#[repr(C, align(64))]
#[derive(Debug)]
pub struct RingHeader {
    pub magic: u32,            // "MMAP" 0x4D4D4150
    pub version: u32,          // 1
    pub capacity: u64,         // bytes usable for record area
    pub head: AtomicU64,       // write pos (producer)
    pub tail: AtomicU64,       // read pos (consumer)
    pub dropped: AtomicU64,    // number of record drop
    pub notify_seq: AtomicU32, // seq increase when publish (futex wake)
    pub _pad: [u8; 28],        // pad to 64 bytes: 4+4+8+8+8+8+4+28=64
}

#[repr(C, align(16))]
#[derive(Clone, Copy, Debug)]
pub struct RecHdr {
    pub len: u32,   // total bytes = 16 (header) + payload
    pub kind: u16,  // 1=Account 2=Slot ...
    pub flags: u16, // reserved
    pub seq: u64,   // sequence number for resync
}

#[derive(Debug)]
pub struct MmapRing {
    pub hdr: *mut RingHeader,
    pub data: *mut u8,
    pub total_len: usize,
}

unsafe impl Send for MmapRing {}
unsafe impl Sync for MmapRing {}

impl MmapRing {
    pub fn create_or_open(path: &Path, total_len: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        if total_len > 0 {
            unsafe {
                let rc = ftruncate(file.as_raw_fd(), total_len as off_t);
                if rc != 0 {
                    return Err(io::Error::last_os_error());
                }
            }
        }
        let meta = file.metadata()?;
        let map_len = if total_len > 0 {
            total_len
        } else {
            meta.len() as usize
        };

        unsafe {
            let ptr = mmap(
                std::ptr::null_mut(),
                map_len,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                file.as_raw_fd(),
                0,
            );
            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            let hdr = ptr as *mut RingHeader;
            let data = (ptr as *mut u8).add(std::mem::size_of::<RingHeader>());
            Ok(Self {
                hdr,
                data,
                total_len: map_len,
            })
        }
    }

    pub fn init(&self, capacity: u64) {
        assert!(capacity % 16 == 0, "capacity must be multiple of 16");
        unsafe {
            (*self.hdr).magic = 0x4D4D4150;
            (*self.hdr).version = 1;
            (*self.hdr).capacity = capacity;
            (*self.hdr).head.store(0, Ordering::Relaxed);
            (*self.hdr).tail.store(0, Ordering::Relaxed);
            (*self.hdr).dropped.store(0, Ordering::Relaxed);
            (*self.hdr).notify_seq.store(0, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn capacity(&self) -> u64 {
        unsafe { (*self.hdr).capacity }
    }

    #[inline]
    fn read_u32_at(&self, off: u64) -> u32 {
        let cap = self.capacity() as usize;
        let start = off as usize;
        let mut b = [0u8; 4];
        unsafe {
            if start + 4 <= cap {
                std::ptr::copy_nonoverlapping(self.data.add(start), b.as_mut_ptr(), 4);
            } else {
                let first = cap - start;
                std::ptr::copy_nonoverlapping(self.data.add(start), b.as_mut_ptr(), first);
                std::ptr::copy_nonoverlapping(self.data, b.as_mut_ptr().add(first), 4 - first);
            }
        }
        u32::from_le_bytes(b)
    }

    #[inline]
    fn write_bytes(&self, off: u64, src: &[u8]) {
        let cap = self.capacity() as usize;
        let start = off as usize;
        unsafe {
            if start + src.len() <= cap {
                std::ptr::copy_nonoverlapping(src.as_ptr(), self.data.add(start), src.len());
            } else {
                let first = cap - start;
                std::ptr::copy_nonoverlapping(src.as_ptr(), self.data.add(start), first);
                std::ptr::copy_nonoverlapping(
                    src.as_ptr().add(first),
                    self.data,
                    src.len() - first,
                );
            }
        }
    }

    #[inline]
    fn align16(x: u64) -> u64 {
        (x + 15) & !15
    }

    fn advance_tail_one_record(&self) -> bool {
        use Ordering::*;
        let cap = self.capacity();

        loop {
            let tail = unsafe { (*self.hdr).tail.load(Acquire) };
            let head = unsafe { (*self.hdr).head.load(Acquire) };

            if head <= tail {
                return false;
            }

            let tmod = tail % cap;
            let len = self.read_u32_at(tmod) as u64;

            // Validate header
            if len < 16 || len > cap {
                // Invalid header, force sync to head
                unsafe {
                    (*self.hdr).tail.store(head, Release);
                }
                return false;
            }

            let new_tail = tail + Self::align16(len);

            // CAS to advance tail
            match unsafe {
                (*self.hdr)
                    .tail
                    .compare_exchange_weak(tail, new_tail, Release, Acquire)
            } {
                Ok(_) => {
                    unsafe {
                        (*self.hdr).dropped.fetch_add(1, Relaxed);
                    }
                    return true;
                }
                Err(_) => continue, // retry
            }
        }
    }

    pub fn write_record(&self, hdr: RecHdr, payload: &[u8]) -> bool {
        use Ordering::*;
        let cap = self.capacity();

        // Validate inputs
        let expected_total = 16 + payload.len();
        if hdr.len as usize != expected_total {
            return false;
        }

        let total = Self::align16(hdr.len as u64);
        if total > cap {
            unsafe {
                (*self.hdr).dropped.fetch_add(1, Relaxed);
            }
            return false;
        }

        loop {
            // Load current head
            let head = unsafe { (*self.hdr).head.load(Acquire) };
            let tail = unsafe { (*self.hdr).tail.load(Acquire) };
            let mut used = head - tail;

            // Drop records if out of cap
            while used + total > cap {
                if !self.advance_tail_one_record() {
                    unsafe {
                        (*self.hdr).dropped.fetch_add(1, Relaxed);
                    }
                    return false;
                }
                let new_tail = unsafe { (*self.hdr).tail.load(Acquire) };
                used = head - new_tail;
            }

            let off = head % cap;

            // Write payload before increasing head
            let mut hb = [0u8; 16];
            hb[0..4].copy_from_slice(&hdr.len.to_le_bytes());
            hb[4..6].copy_from_slice(&hdr.kind.to_le_bytes());
            hb[6..8].copy_from_slice(&hdr.flags.to_le_bytes());
            hb[8..16].copy_from_slice(&hdr.seq.to_le_bytes());
            self.write_bytes(off, &hb);

            let poff = (off + 16) % cap;
            self.write_bytes(poff, payload);

            compiler_fence(Release);

            match unsafe {
                (*self.hdr)
                    .head
                    .compare_exchange_weak(head, head + total, AcqRel, Acquire)
            } {
                Ok(_) => {
                    unsafe {
                        (*self.hdr).notify_seq.fetch_add(1, Release);
                    }
                    futex_wake(&unsafe { &*self.hdr }.notify_seq);
                    return true;
                }
                Err(_actual_head) => {
                    continue;
                }
            }
        }
    }
}

impl Drop for MmapRing {
    fn drop(&mut self) {
        unsafe {
            munmap(self.hdr as *mut _, self.total_len);
        }
    }
}

#[inline]
pub fn futex_wait(word: &AtomicU32, expected: u32, timeout_ns: i64) {
    // timeout_ns < 0 → wait indefinitely
    unsafe {
        let uaddr = word as *const AtomicU32 as *const u32 as *mut c_void;
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let ts_ptr: *const timespec = if timeout_ns >= 0 {
            ts.tv_sec = (timeout_ns / 1_000_000_000) as i64;
            ts.tv_nsec = (timeout_ns % 1_000_000_000) as i64;
            &ts
        } else {
            std::ptr::null()
        };
        libc::syscall(
            SYS_futex,
            uaddr,
            FUTEX_WAIT | FUTEX_PRIVATE_FLAG,
            expected as c_int,
            ts_ptr,
            0 as *mut c_void,
            0,
        );
    }
}

#[inline]
pub fn futex_wake(word: &AtomicU32) {
    unsafe {
        let uaddr = word as *const AtomicU32 as *const u32 as *mut c_void;
        libc::syscall(
            SYS_futex,
            uaddr,
            FUTEX_WAKE | FUTEX_PRIVATE_FLAG,
            1i32,
            0 as *mut c_void,
            0 as *mut c_void,
            0,
        );
    }
}

#[inline]
pub fn align16_u64(x: u64) -> u64 {
    (x + 15) & !15
}
