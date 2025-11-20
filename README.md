## mmap-geyser

A minimal Solana Geyser pipeline that streams account and slot updates via an mmap-backed ring buffer.

### Layout
- `geyser-mmap-plugin`: Solana plugin that writes updates into a shared-memory
  ring (`/dev/shm/geyser.ring`) using futex wakes.
- `geyser-example`: tiny consumer that reads the ring, decodes records and dumps
  them to stdout.
- `ring-shared`: common ring+futex helpers used by both sides.

### Build
```bash
cargo build --release -p geyser-mmap-plugin
```

### Run
1. Start a validator with the plugin:
   ```bash
   solana-test-validator -r --geyser-plugin-config config.json
   ```
2. In another shell, run the reader:
   ```bash
   cargo run -p geyser-example
   ```

### Config
`config.json` controls the plugin. Example:
```json
{
  "libpath": "target/release/libgeyser_mmap_plugin.so",
  "shm_path": "/dev/shm/geyser.ring",
  "ring_bytes": 67108864,
  "max_record_bytes": 8388608,
  "bind_address": "0.0.0.0:19999"
}
```

### References
- https://github.com/0xNineteen/simple-geyser
- https://github.com/blockworks-foundation/quic_geyser_plugin
