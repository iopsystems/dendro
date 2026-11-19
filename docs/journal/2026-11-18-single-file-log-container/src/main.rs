//! GO/NO-GO probe for docs/journal/2026-11-18-single-file-log-container.md.
//!
//! g1: per-tick commit time, dendro's SQLite tick commit vs append + sync.
//! g2: rolling buffer kept by hole punching; allocated vs live bytes, and a
//!     reader following the file that must never see a foreign byte.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use dendro::archive::{ArchiveMut, SourceMeta, WalRow};

const STREAMS: usize = 26;
const ROW_BYTES: usize = 1925;
const MAGIC: u32 = 0x444c_4f47; // "DLOG"
const KIND_TICK: u32 = 1;
const KIND_SEAL: u32 = 2;

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

fn stream_name(i: usize) -> String {
    format!("stream-{i:02}")
}

fn tick_rows(seq: u64) -> Vec<WalRow> {
    let mut rng = Rng::new(seq);
    (0..STREAMS)
        .map(|i| {
            let mut row = vec![0u8; ROW_BYTES];
            rng.fill(&mut row);
            WalRow {
                stream: stream_name(i),
                ts: (seq as i64 + 1) * 1_000_000_000,
                wall_offset: 0,
                row,
            }
        })
        .collect()
}

/// One record: magic, kind, payload length, payload, crc32 of all before it.
fn encode(seq: u64, kind: u32, seal_bytes: usize) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&seq.to_le_bytes());
    if kind == KIND_TICK {
        for r in tick_rows(seq) {
            payload.extend_from_slice(&(r.stream.len() as u16).to_le_bytes());
            payload.extend_from_slice(r.stream.as_bytes());
            payload.extend_from_slice(&r.ts.to_le_bytes());
            payload.extend_from_slice(&r.wall_offset.to_le_bytes());
            payload.extend_from_slice(&(r.row.len() as u32).to_le_bytes());
            payload.extend_from_slice(&r.row);
        }
    } else {
        let mut body = vec![0u8; seal_bytes];
        Rng::new(seq ^ 0xABCD).fill(&mut body);
        payload.extend_from_slice(&body);
    }
    let mut rec = Vec::with_capacity(16 + payload.len() + 4);
    rec.extend_from_slice(&MAGIC.to_le_bytes());
    rec.extend_from_slice(&kind.to_le_bytes());
    rec.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    rec.extend_from_slice(&payload);
    let crc = crc32fast::hash(&rec);
    rec.extend_from_slice(&crc.to_le_bytes());
    rec
}

fn verify(rec: &[u8]) -> bool {
    if rec.len() < 20 {
        return false;
    }
    let (body, crc) = rec.split_at(rec.len() - 4);
    u32::from_le_bytes(crc.try_into().unwrap()) == crc32fast::hash(body)
}

#[derive(Clone, Copy, PartialEq)]
enum Sync {
    Data,
    Full,
}

fn sync(f: &File, mode: Sync) {
    let fd = f.as_raw_fd();
    let rc = unsafe {
        match mode {
            #[cfg(target_os = "macos")]
            Sync::Data => libc::fsync(fd),
            #[cfg(not(target_os = "macos"))]
            Sync::Data => libc::fdatasync(fd),
            #[cfg(target_os = "macos")]
            Sync::Full => libc::fcntl(fd, libc::F_FULLFSYNC),
            #[cfg(not(target_os = "macos"))]
            Sync::Full => libc::fsync(fd),
        }
    };
    assert_eq!(rc, 0, "sync failed: {}", std::io::Error::last_os_error());
}

fn flock(f: &File) {
    let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(rc, 0, "flock: {}", std::io::Error::last_os_error());
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn remove_archive(path: &Path) {
    for suffix in ["", "-wal", "-shm", "-lock", "-journal"] {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(p));
    }
}

/// Today's tick commit: the writer thread's connection, one transaction per
/// tick, a PASSIVE checkpoint every 10 s as writer_loop does.
fn run_sqlite(path: &Path, warm: usize, ticks: usize, mode: Sync) -> Vec<f64> {
    remove_archive(path);
    let source_id = {
        let mut a = ArchiveMut::create(path).expect("create");
        a.insert_source(&SourceMeta {
            labels: Default::default(),
            metadata: Default::default(),
            clock_anchor_wall_ns: 0,
        })
        .expect("source")
    };
    let mut a = ArchiveMut::open_for_write(path).expect("open_for_write");
    if mode == Sync::Full {
        a.probe_pragma("fullfsync", "ON").unwrap();
        a.probe_pragma("checkpoint_fullfsync", "ON").unwrap();
    }
    let mut out = Vec::with_capacity(ticks);
    let mut last_ckpt = Instant::now();
    for seq in 0..(warm + ticks) as u64 {
        let rows = tick_rows(seq);
        let t = Instant::now();
        a.insert_wal_rows_batch(&[(source_id, rows)]).expect("commit");
        let dt = t.elapsed();
        if seq as usize >= warm {
            out.push(dt.as_secs_f64() * 1e6);
        }
        if last_ckpt.elapsed() >= Duration::from_secs(10) {
            let _ = a.checkpoint_passive();
            last_ckpt = Instant::now();
        }
    }
    drop(a);
    remove_archive(path);
    out
}

/// Preallocation in fixed chunks ahead of the write position, without moving
/// EOF, so O_APPEND still appends after the last record.
struct Prealloc {
    chunk: u64,
    allocated_to: u64,
}

impl Prealloc {
    fn new(chunk: u64) -> Self {
        Prealloc { chunk, allocated_to: 0 }
    }
    fn ensure(&mut self, f: &File, upto: u64) {
        if self.chunk == 0 {
            return;
        }
        while self.allocated_to < upto {
            fallocate_keep_size(f, self.allocated_to, self.chunk);
            self.allocated_to += self.chunk;
        }
    }
}

#[cfg(target_os = "linux")]
fn fallocate_keep_size(f: &File, off: u64, len: u64) {
    let rc = unsafe { libc::fallocate(f.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, off as _, len as _) };
    assert_eq!(rc, 0, "fallocate: {}", std::io::Error::last_os_error());
}

#[cfg(target_os = "macos")]
fn fallocate_keep_size(f: &File, off: u64, len: u64) {
    // F_PREALLOCATE allocates relative to the physical end of file
    // (F_PEOFPOSMODE); off is implied by what was allocated before.
    let _ = off;
    let mut st = libc::fstore_t {
        fst_flags: libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len as libc::off_t,
        fst_bytesalloc: 0,
    };
    let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_PREALLOCATE, &mut st) };
    assert_ne!(rc, -1, "F_PREALLOCATE: {}", std::io::Error::last_os_error());
}

/// xfs extent size hint: FS_IOC_FSSETXATTR with FS_XFLAG_EXTSIZE. Must be set
/// while the file has no data. Prints the value read back.
#[cfg(target_os = "linux")]
fn set_extsize(f: &File, bytes: u32) {
    #[repr(C)]
    #[derive(Default, Debug)]
    struct FsXattr {
        fsx_xflags: u32,
        fsx_extsize: u32,
        fsx_nextents: u32,
        fsx_projid: u32,
        fsx_cowextsize: u32,
        fsx_pad: [u8; 8],
    }
    const FS_IOC_FSGETXATTR: libc::c_ulong = 0x801c_581f;
    const FS_IOC_FSSETXATTR: libc::c_ulong = 0x401c_5820;
    const FS_XFLAG_EXTSIZE: u32 = 0x0000_0800;
    let fd = f.as_raw_fd();
    let mut x = FsXattr::default();
    assert_eq!(unsafe { libc::ioctl(fd, FS_IOC_FSGETXATTR, &mut x) }, 0, "FSGETXATTR: {}", std::io::Error::last_os_error());
    x.fsx_xflags |= FS_XFLAG_EXTSIZE;
    x.fsx_extsize = bytes;
    let rc = unsafe { libc::ioctl(fd, FS_IOC_FSSETXATTR, &x) };
    assert_eq!(rc, 0, "FSSETXATTR: {}", std::io::Error::last_os_error());
    let mut y = FsXattr::default();
    assert_eq!(unsafe { libc::ioctl(fd, FS_IOC_FSGETXATTR, &mut y) }, 0);
    println!("extsize hint read back: xflags {:#x} extsize {}", y.fsx_xflags, y.fsx_extsize);
}

#[cfg(target_os = "macos")]
fn set_extsize(_f: &File, _bytes: u32) {
    panic!("extent size hints are xfs only");
}

fn run_log(path: &Path, warm: usize, ticks: usize, mode: Sync, prealloc: u64, extsize: u32) -> Vec<f64> {
    let _ = std::fs::remove_file(path);
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .unwrap();
    flock(&f);
    if extsize > 0 {
        set_extsize(&f, extsize);
    }
    let mut pa = Prealloc::new(prealloc);
    let mut end = 0u64;
    let mut out = Vec::with_capacity(ticks);
    for seq in 0..(warm + ticks) as u64 {
        let t = Instant::now();
        let rec = encode(seq, KIND_TICK, 0);
        end += rec.len() as u64;
        pa.ensure(&f, end);
        f.write_all(&rec).unwrap();
        sync(&f, mode);
        let dt = t.elapsed();
        if seq as usize >= warm {
            out.push(dt.as_secs_f64() * 1e6);
        }
    }
    drop(f);
    let _ = std::fs::remove_file(path);
    out
}

fn g1(dir: &Path, trials: usize, warm: usize, ticks: usize, full: bool, arms_arg: Option<String>) {
    let mut arms: Vec<String> = match arms_arg {
        Some(a) => a.split(',').map(str::to_string).collect(),
        None => ["sqlite_a", "sqlite_b", "log_a", "log_b"].iter().map(|s| s.to_string()).collect(),
    };
    if full {
        arms.extend(["sqlite_full".to_string(), "log_full".to_string()]);
    }
    let mut medians: Vec<Vec<f64>> = vec![Vec::new(); arms.len()];
    let mut p99s: Vec<Vec<f64>> = vec![Vec::new(); arms.len()];
    println!("trial,arm,median_us,p99_us,mean_us");
    for trial in 0..trials {
        for k in 0..arms.len() {
            let i = (k + trial) % arms.len();
            let arm = arms[i].as_str();
            let mode = if arm.ends_with("full") { Sync::Full } else { Sync::Data };
            let mut v = if arm.starts_with("sqlite") {
                run_sqlite(&dir.join("probe.dendro"), warm, ticks, mode)
            } else {
                let prealloc = if arm.contains("prealloc") { 16 << 20 } else { 0 };
                let extsize = if arm.contains("extsize") { 1 << 20 } else { 0 };
                run_log(&dir.join("probe.dlog"), warm, ticks, mode, prealloc, extsize)
            };
            let mean = v.iter().sum::<f64>() / v.len() as f64;
            let p99 = pct(&mut v, 0.99);
            let med = median(&mut v);
            println!("{trial},{arm},{med:.1},{p99:.1},{mean:.1}");
            medians[i].push(med);
            p99s[i].push(p99);
        }
    }
    println!("\nsummary over {trials} trials (per-tick us; median of per-trial medians, min..max)");
    for (i, arm) in arms.iter().enumerate() {
        let mut m = medians[i].clone();
        let lo = m.iter().cloned().fold(f64::INFINITY, f64::min);
        let hi = m.iter().cloned().fold(0.0, f64::max);
        let mut p = p99s[i].clone();
        println!(
            "{arm:12} median {:8.1}  [{lo:.1}..{hi:.1}]  median p99 {:8.1}",
            median(&mut m),
            median(&mut p)
        );
    }
}

fn allocated(f: &File) -> (u64, u64, u64) {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::fstat(f.as_raw_fd(), &mut st) }, 0);
    (st.st_blocks as u64 * 512, st.st_size as u64, st.st_blksize as u64)
}

#[cfg(target_os = "macos")]
fn punch(f: &File, off: u64, len: u64) {
    #[repr(C)]
    struct FPunchhole {
        fp_flags: u32,
        reserved: u32,
        fp_offset: libc::off_t,
        fp_length: libc::off_t,
    }
    const F_PUNCHHOLE: libc::c_int = 99;
    let arg = FPunchhole { fp_flags: 0, reserved: 0, fp_offset: off as _, fp_length: len as _ };
    let rc = unsafe { libc::fcntl(f.as_raw_fd(), F_PUNCHHOLE, &arg) };
    assert_eq!(rc, 0, "F_PUNCHHOLE: {}", std::io::Error::last_os_error());
}

#[cfg(target_os = "linux")]
fn punch(f: &File, off: u64, len: u64) {
    let rc = unsafe {
        libc::fallocate(
            f.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            off as _,
            len as _,
        )
    };
    assert_eq!(rc, 0, "PUNCH_HOLE: {}", std::io::Error::last_os_error());
}

struct Sent {
    seq: u64,
    kind: u32,
    start: u64,
    len: u64,
}

fn reader(path: PathBuf, rx: mpsc::Receiver<Sent>, punched: Arc<AtomicU64>, seal_bytes: usize) -> (u64, u64, u64, u64) {
    let f = File::open(&path).unwrap();
    let (mut ok, mut evicted, mut partial, mut violations) = (0u64, 0u64, 0u64, 0u64);
    let mut n = 0u64;
    for s in rx {
        n += 1;
        // Lag behind the writer in bursts so punches land under the reader.
        if n % 20_000 == 0 {
            std::thread::sleep(Duration::from_secs(4));
        }
        let mut buf = vec![0u8; s.len as usize];
        f.read_exact_at(&mut buf, s.start).unwrap();
        if verify(&buf) {
            ok += 1;
            continue;
        }
        let pt = punched.load(Ordering::SeqCst);
        let expect = encode(s.seq, s.kind, seal_bytes);
        let mut bad = false;
        let mut zeros = 0u64;
        for (i, (&got, &want)) in buf.iter().zip(expect.iter()).enumerate() {
            let off = s.start + i as u64;
            if got == want {
                continue;
            }
            if got == 0 && off < pt {
                zeros += 1;
                continue;
            }
            bad = true;
            break;
        }
        if bad {
            violations += 1;
            eprintln!("VIOLATION seq {} at {} len {} punched_to {pt}", s.seq, s.start, s.len);
        } else if zeros as usize == buf.iter().zip(expect.iter()).filter(|(g, w)| g != w).count() {
            if s.start + s.len <= pt {
                evicted += 1;
            } else {
                partial += 1;
            }
        }
    }
    (ok, evicted, partial, violations)
}

fn g2(dir: &Path, ws_mib: u64, total_mult: u64, seal_every: u64, seal_mib: u64, with_sync: bool, prealloc_mib: u64, extsize_kib: u32) {
    let path = dir.join("probe-g2.dlog");
    let _ = std::fs::remove_file(&path);
    let mut f = OpenOptions::new().create(true).append(true).read(true).open(&path).unwrap();
    flock(&f);
    if extsize_kib > 0 {
        set_extsize(&f, extsize_kib << 10);
    }
    let mut pa = Prealloc::new(prealloc_mib << 20);
    println!("prealloc chunk {prealloc_mib} MiB; extsize hint {extsize_kib} KiB");
    let (_, _, blk) = allocated(&f);
    let ws = ws_mib << 20;
    let total = ws * total_mult;
    let seal_bytes = (seal_mib << 20) as usize;
    let punched = Arc::new(AtomicU64::new(0));
    let (tx, rx) = mpsc::channel::<Sent>();
    let rpath = path.clone();
    let rp = punched.clone();
    let rh = std::thread::spawn(move || reader(rpath, rx, rp, seal_bytes));

    let mut live: VecDeque<(u64, u64)> = VecDeque::new();
    let mut end = 0u64;
    let mut punched_to = 0u64;
    let mut punch_us: Vec<f64> = Vec::new();
    let mut next_report = ws;
    let mut seq = 0u64;
    let mut tick_n = 0u64;
    println!("block size {blk}; working set {ws_mib} MiB; total {} MiB", total >> 20);
    println!("appended_mib,live_mib,allocated_mib,apparent_mib,alloc_over_live");
    let t0 = Instant::now();
    while end < total {
        let kind = if tick_n > 0 && tick_n % seal_every == 0 { KIND_SEAL } else { KIND_TICK };
        let rec = encode(seq, kind, seal_bytes);
        pa.ensure(&f, end + rec.len() as u64);
        f.write_all(&rec).unwrap();
        if with_sync {
            sync(&f, Sync::Data);
        }
        let start = end;
        end += rec.len() as u64;
        live.push_back((start, rec.len() as u64));
        tx.send(Sent { seq, kind, start, len: rec.len() as u64 }).unwrap();
        if std::env::var("PROBE_CORRUPT").is_ok() && seq == 26_000 {
            let w = OpenOptions::new().write(true).open(&path).unwrap();
            let (cs, _) = live[live.len() - 1000];
            w.write_all_at(&[0xFFu8; 16], cs + 100).unwrap();
            eprintln!("corrupted record at {cs}");
        }
        seq += 1;
        if kind == KIND_TICK {
            tick_n += 1;
        } else {
            tick_n += 1;
        }
        while let Some(&(s, _)) = live.front() {
            if end - s > ws {
                live.pop_front();
            } else {
                break;
            }
        }
        let live_start = live.front().map(|&(s, _)| s).unwrap_or(end);
        let target = live_start / blk * blk;
        if target >= punched_to + (1 << 20) {
            let t = Instant::now();
            punch(&f, punched_to, target - punched_to);
            punch_us.push(t.elapsed().as_secs_f64() * 1e6);
            punched_to = target;
            punched.store(punched_to, Ordering::SeqCst);
        }
        if end >= next_report {
            let (alloc, apparent, _) = allocated(&f);
            let livebytes = end - live_start;
            println!(
                "{:.0},{:.1},{:.1},{:.0},{:.4}",
                end as f64 / 1048576.0,
                livebytes as f64 / 1048576.0,
                alloc as f64 / 1048576.0,
                apparent as f64 / 1048576.0,
                alloc as f64 / livebytes as f64
            );
            next_report += ws;
        }
    }
    let wall = t0.elapsed();
    drop(tx);
    let (ok, evicted, partial, violations) = rh.join().unwrap();
    let n = punch_us.len();
    let p99 = if n > 0 { pct(&mut punch_us, 0.99) } else { 0.0 };
    let med = if n > 0 { median(&mut punch_us) } else { 0.0 };
    println!("wall {:.1}s; {seq} records; punches {n}: median {med:.0} us, p99 {p99:.0} us", wall.as_secs_f64());
    println!("reader: ok {ok}, evicted (all zeros) {evicted}, partially punched {partial}, VIOLATIONS {violations}");
    #[cfg(target_os = "linux")]
    {
        let o = std::process::Command::new("filefrag").arg(&path).output();
        if let Ok(o) = o {
            print!("{}", String::from_utf8_lossy(&o.stdout));
        }
    }
    drop(f);
    if std::env::var("PROBE_KEEP").is_err() {
        let _ = std::fs::remove_file(&path);
    } else {
        println!("kept {} live_start {}", path.display(), live.front().map(|&(s, _)| s).unwrap_or(end));
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |k: &str, d: &str| -> String {
        args.iter()
            .position(|a| a == k)
            .map(|i| args[i + 1].clone())
            .unwrap_or_else(|| d.to_string())
    };
    let dir = PathBuf::from(get("--dir", "."));
    match args.get(1).map(String::as_str) {
        Some("g1") => g1(
            &dir,
            get("--trials", "15").parse().unwrap(),
            get("--warm", "200").parse().unwrap(),
            get("--ticks", "1000").parse().unwrap(),
            args.iter().any(|a| a == "--full"),
            args.iter().position(|a| a == "--arms").map(|i| args[i + 1].clone()),
        ),
        Some("g2") => g2(
            &dir,
            get("--ws-mib", "256").parse().unwrap(),
            get("--mult", "20").parse().unwrap(),
            get("--seal-every", "1000").parse().unwrap(),
            get("--seal-mib", "4").parse().unwrap(),
            !args.iter().any(|a| a == "--no-sync"),
            get("--prealloc-mib", "0").parse().unwrap(),
            get("--extsize-kib", "0").parse().unwrap(),
        ),
        _ => eprintln!("usage: logprobe g1|g2 --dir D ..."),
    }
}
