use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, Result as GeyserResult, SlotStatus,
};
use ring_shared::{MmapRing, RecHdr, align16_u64};
use std::{
    path::PathBuf,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(serde::Deserialize)]
struct Cfg {
    libpath: Option<String>,
    shm_path: String,
    ring_bytes: u64,
    max_record_bytes: Option<u64>,
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

#[derive(Debug)]
pub struct MmapPlugin {
    ring: OnceLock<MmapRing>,
    seq: AtomicU64,
    max_rec: u64,
}

impl Default for MmapPlugin {
    fn default() -> Self {
        Self {
            ring: OnceLock::new(),
            seq: AtomicU64::new(1),
            max_rec: 8 << 20,
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
