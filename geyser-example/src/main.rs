use anchor_client::solana_sdk::pubkey::Pubkey;
use clap::Parser;
use core::sync::atomic::Ordering::*;
use ring_shared::{MmapRing, RecHdr, futex_wait};
use std::io::Write;
use std::net::TcpStream;
use std::path::Path;
use tokio::task;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "/dev/shm/geyser.ring")]
    shm_path: String,

    /// ns; -1 = infinite
    #[arg(long, default_value_t = 200_000)] // 200µs
    empty_wait_ns: i64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let ring = MmapRing::create_or_open(Path::new(&args.shm_path), 0)?;
    let cap = ring.capacity() as usize;

    let hdr = unsafe { &*ring.hdr };
    let mut last_seq = hdr.notify_seq.load(Relaxed);

    add_accounts(vec!["SysvarC1ock11111111111111111111111111111111"]);

    loop {
        let head = hdr.head.load(Acquire);
        let mut tail = hdr.tail.load(Relaxed);
        if head == tail {
            futex_wait(&hdr.notify_seq, last_seq, args.empty_wait_ns);
            last_seq = hdr.notify_seq.load(Relaxed);
            continue;
        }

        let mut tmod = (tail % ring.capacity()) as usize;
        let mut hbuf = [0u8; 16];
        read_bytes(&ring, tmod, &mut hbuf, cap);

        let len = u32::from_le_bytes(hbuf[0..4].try_into().unwrap()) as usize;
        let kind = u16::from_le_bytes(hbuf[4..6].try_into().unwrap());
        // flags = hbuf[6..8]
        let seq = u64::from_le_bytes(hbuf[8..16].try_into().unwrap());

        if len < 16 || len as u64 > ring.capacity() {
            let head_now = hdr.head.load(Acquire);
            hdr.tail.store(head_now, Release);
            tail = head_now;
            last_seq = hdr.notify_seq.load(Relaxed);
            continue;
        }

        let plen = len - 16;
        let mut payload = vec![0u8; plen];
        let poff = (tmod + 16) % cap;
        read_bytes(&ring, poff, &mut payload, cap);

        let kind_dispatch = kind;
        task::spawn_blocking(move || match kind_dispatch {
            1 => handle_account(&payload),
            2 => handle_slot(&payload),
            _ => {}
        });

        // advance tail (fetch_max semantics)
        let adv = (tail + ring_shared::align16_u64(len as u64)) as u64;
        loop {
            let cur = hdr.tail.load(Acquire);
            if cur >= adv {
                break;
            }
            if hdr
                .tail
                .compare_exchange(cur, adv, Release, Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

#[inline]
fn read_bytes(r: &MmapRing, start: usize, dst: &mut [u8], cap: usize) {
    unsafe {
        if start + dst.len() <= cap {
            std::ptr::copy_nonoverlapping(r.data.add(start), dst.as_mut_ptr(), dst.len());
        } else {
            let first = cap - start;
            std::ptr::copy_nonoverlapping(r.data.add(start), dst.as_mut_ptr(), first);
            std::ptr::copy_nonoverlapping(r.data, dst.as_mut_ptr().add(first), dst.len() - first);
        }
    }
}

fn handle_account(p: &[u8]) {
    // pubkey(32) | owner(32) | lamports(8) | slot(8) | exec(1) | data_len(4) | data[..]
    if p.len() < 32 + 32 + 8 + 8 + 1 + 4 {
        return;
    }

    let pubkey = &p[0..32];
    let owner = &p[32..64];
    let lamports = u64::from_le_bytes(p[64..72].try_into().unwrap());
    let slot = u64::from_le_bytes(p[72..80].try_into().unwrap());
    let exec = p[80];
    let data_len = u32::from_le_bytes(p[81..85].try_into().unwrap()) as usize;

    if p.len() < 85 + data_len {
        println!("data_len {} out of buffer size {}", data_len, p.len());
        return;
    }
    let data = &p[85..85 + data_len];

    fn to_pubkey(bytes: &[u8]) -> Pubkey {
        let pubkey: [u8; 32] = unsafe { *(bytes.as_ptr() as *const [u8; 32]) };
        Pubkey::new_from_array(pubkey)
    }

    println!("===== account =====");
    println!("pubkey:   {}", to_pubkey(pubkey));
    println!("owner:    {}", to_pubkey(owner));
    println!("lamports: {}", lamports);
    println!("slot:     {}", slot);
    println!("exec:     {}", exec);
    println!("data_len: {}", data_len);
}

fn handle_slot(p: &[u8]) {
    // slot(8) | has_parent(1) | status(1) | parent[..]
    if p.len() < 10 {
        return;
    }
    let slot = u64::from_le_bytes(p[0..8].try_into().unwrap());
    let status = p[9];
    println!("===== slot =====");
    println!("slot:   {}", slot);
    println!("status: {}", status);
}

fn add_accounts<S: AsRef<str>>(accounts: Vec<S>) -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:19999")?;

    let acc: Vec<String> = accounts.iter().map(|s| s.as_ref().to_string()).collect();
    let cmd = format!(
        r#"{{"cmd":"add_accounts","accounts":{}}}"#,
        serde_json::to_string(&acc).unwrap()
    );

    stream.write_all(cmd.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    Ok(())
}
