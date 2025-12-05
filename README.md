# eventhub-demo2-MAX

EventHub Throughput Tester with Persistent Advice ID Counter

## Features
- Tests Azure EventHub throughput (precise or maximum)
- Generates realistic PatientAdmissionAdvice JSON payloads
- **Persistent advice ID counter** across runs (saved to `advice_id_counter.json`)
- Tracks Advice ID range: `700000 → 705999` (example)
- Multiple test modes: `precise`, `max_throughput`, `discover_max`

## Usage
