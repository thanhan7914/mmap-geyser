use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, Result as GeyserResult, SlotStatus,
};
use arc_swap::ArcSwap;
use ring_shared::{MmapRing, RecHdr};
use solana_program::pubkey::Pubkey;
use std::{
    collections::HashSet,
    io::{BufRead, BufReader},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

#[derive(serde::Deserialize)]
struct Cfg {
    libpath: Option<String>,
    shm_path: String,
    ring_bytes: u64,
    max_record_bytes: Option<u64>,
    bind_address: Option<String>,
}

#[inline]
fn encode_slot_status(st: &SlotStatus) -> u8 {
    match st {
        SlotStatus::Processed => 0,
        SlotStatus::Confirmed => 1,
        SlotStatus::Rooted => 2,
        _ => 3,
    }
}

fn parse_pubkeys(values: &[String]) -> Vec<[u8; 32]> {
    values
        .iter()
        .filter_map(|value| match Pubkey::from_str(value) {
            Ok(pk) => Some(pk.to_bytes()),
            Err(err) => None,
        })
        .collect()
}

fn spawn_filter_socket(addr: String, filters: Arc<ArcSwap<FilterState>>) {
    if let Err(err) = thread::Builder::new()
        .name("filter-socket".into())
        .spawn(move || filter_listener(addr, filters))
    {
        eprintln!("Failed to spawn listener thread: {}", err);
    }
}

fn filter_listener(addr: String, filters: Arc<ArcSwap<FilterState>>) {
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => {
            println!("mmap-geyser plugin listening on {}", addr);
            l
        }
        Err(err) => {
            eprintln!("Failed to bind {}: {}", addr, err);
            return;
        }
    };

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let filters = Arc::clone(&filters);
                if let Err(err) = thread::Builder::new()
                    .name("filter-client".into())
                    .spawn(move || handle_filter_client(stream, filters))
                {
                    eprintln!("[mmap-geyser] failed to spawn client handler: {}", err);
                }
            }
            Err(err) => {
                eprintln!("[mmap-geyser] error accepting connection: {}", err);
            }
        }
    }
}

fn handle_filter_client(stream: TcpStream, filters: Arc<ArcSwap<FilterState>>) {
    let peer = stream.peer_addr().ok();
    let reader = BufReader::new(stream);

    for line in reader.lines() {
        match line {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<FilterCommand>(trimmed) {
                    Ok(cmd) => apply_filter_command(&filters, cmd),
                    Err(err) => eprintln!("[mmap-geyser] invalid command {:?}: {}", trimmed, err),
                }
            }
            Err(err) => {
                eprintln!("[mmap-geyser] client {:?} read error: {}", peer, err);
                break;
            }
        }
    }
}

fn apply_filter_command(filters: &Arc<ArcSwap<FilterState>>, cmd: FilterCommand) {
    let mut next = (*filters.load_full()).clone();
    match cmd {
        FilterCommand::ReplaceAccounts { accounts } => {
            let parsed = parse_pubkeys(&accounts);
            next.allow_accounts = parsed.into_iter().collect::<HashSet<_>>();
        }
        FilterCommand::AddAccounts { accounts } => {
            let parsed = parse_pubkeys(&accounts);
            for pk in parsed {
                next.allow_accounts.insert(pk);
            }
        }
        FilterCommand::RemoveAccounts { accounts } => {
            let parsed = parse_pubkeys(&accounts);
            for pk in parsed {
                next.allow_accounts.remove(&pk);
            }
        }
        FilterCommand::ClearAccounts => {
            next.allow_accounts.clear();
        }
        FilterCommand::UpdateSlot { enabled } => {
            next.allow_slot = enabled;
        }
    }
    filters.store(Arc::new(next));
}

#[derive(Clone, Debug)]
struct FilterState {
    allow_accounts: HashSet<[u8; 32]>,
    allow_slot: bool,
}

impl Default for FilterState {
    fn default() -> Self {
        Self {
            allow_accounts: HashSet::new(),
            allow_slot: false,
        }
    }
}

impl FilterState {
    #[inline]
    fn allows(&self, pubkey: &[u8]) -> bool {
        if pubkey.len() != 32 {
            return false;
        }
        let pk = unsafe { &*(pubkey.as_ptr() as *const [u8; 32]) };
        self.allow_accounts.contains(pk)
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum FilterCommand {
    ReplaceAccounts { accounts: Vec<String> },
    AddAccounts { accounts: Vec<String> },
    RemoveAccounts { accounts: Vec<String> },
    UpdateSlot { enabled: bool },
    ClearAccounts,
}

#[derive(Debug)]
pub struct MmapPlugin {
    ring: OnceLock<MmapRing>,
    seq: AtomicU64,
    max_rec: u64,
    filters: Arc<ArcSwap<FilterState>>,
}

impl Default for MmapPlugin {
    fn default() -> Self {
        Self {
            ring: OnceLock::new(),
            seq: AtomicU64::new(1),
            max_rec: 8 << 20,
            filters: Arc::new(ArcSwap::from_pointee(FilterState::default())),
        }
    }
}

impl MmapPlugin {
    fn put(&self, kind: u16, payload: &[u8]) {
        if let Some(ring) = self.ring.get() {
            if (payload.len() as u64 + 16) > ring.capacity()
                || (payload.len() as u64) > self.max_rec
            {
                unsafe {
                    (*ring.hdr)
                        .dropped
                        .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                }
                return;
            }

            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let hdr = RecHdr {
                len: (16 + payload.len()) as u32,
                kind,
                flags: 0,
                seq,
            };
            let _ok = ring.write_record(hdr, payload);
        }
    }

    fn should_emit_account(&self, pubkey: &[u8]) -> bool {
        let state = self.filters.load();
        state.allows(pubkey)
    }

    fn should_emit_slot(&self) -> bool {
        let state = self.filters.load();
        state.allow_slot
    }
}

impl GeyserPlugin for MmapPlugin {
    fn name(&self) -> &'static str {
        "geyser-mmap-plugin"
    }

    fn on_load(&mut self, config_file: &str, _is_restart: bool) -> GeyserResult<()> {
        let cfg_s = std::fs::read_to_string(config_file)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        let cfg: Cfg =
            serde_json::from_str(&cfg_s).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;

        let size = cfg.ring_bytes as usize;
        let ring = MmapRing::create_or_open(PathBuf::from(&cfg.shm_path).as_path(), size)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        let cap = (size - std::mem::size_of::<ring_shared::RingHeader>()) as u64;
        ring.init(cap);
        self.max_rec = cfg.max_record_bytes.unwrap_or(8 << 20);
        let _ = self.ring.set(ring);
        if let Some(addr) = cfg.bind_address {
            spawn_filter_socket(addr.clone(), Arc::clone(&self.filters));
        }

        Ok(())
    }

    fn on_unload(&mut self) {}

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        _is_startup: bool,
    ) -> GeyserResult<()> {
        match account {
            ReplicaAccountInfoVersions::V0_0_1(_info) => {
                unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
            }
            ReplicaAccountInfoVersions::V0_0_2(_info) => {
                unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
            }
            ReplicaAccountInfoVersions::V0_0_3(info) => {
                if !self.should_emit_account(info.pubkey) {
                    return Ok(());
                }
                let data = info.data;
                let mut p = Vec::with_capacity(32 + 32 + 8 + 8 + 1 + 4 + data.len());
                p.extend_from_slice(info.pubkey);
                p.extend_from_slice(info.owner);
                p.extend_from_slice(&info.lamports.to_le_bytes());
                p.extend_from_slice(&slot.to_le_bytes());
                p.push(info.executable as u8);
                p.extend_from_slice(&(data.len() as u32).to_le_bytes());
                p.extend_from_slice(data);
                self.put(1, &p);
            }
        }

        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> GeyserResult<()> {
        if !self.should_emit_slot() {
            return Ok(());
        }

        let has_parent = parent.is_some() as u8;
        let base_len = 8 + 1 + 1;
        let total_len = if has_parent == 1 {
            base_len + 8
        } else {
            base_len
        };

        let mut p = Vec::with_capacity(total_len);
        p.extend_from_slice(&slot.to_le_bytes());
        p.push(has_parent);
        if let Some(par) = parent {
            p.extend_from_slice(&par.to_le_bytes());
        }
        p.push(encode_slot_status(status));

        self.put(2, &p);
        Ok(())
    }

    fn notify_end_of_startup(&self) -> GeyserResult<()> {
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        false
    }
}

#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    Box::into_raw(Box::new(MmapPlugin::default()))
}
