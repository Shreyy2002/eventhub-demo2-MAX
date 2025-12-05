---

# Event Hub Producer – Patient Admission Events

## Dependencies

Install required Python packages:

```bash
pip install azure-eventhub azure-eventhub-checkpointstoreblob-aio faker
```

Or using `requirements.txt`:

```
azure-eventhub>=5.11.0
azure-eventhub-checkpointstoreblob-aio>=1.1.4
faker>=20.0.0
```

## How to Run

```bash
# Send 1000 events (default)
python max_throughout_new.py

# Send 100 events
python max_throughout_new.py -e 100

# Send 5000 events with batch size 300
python max_throughout_new.py -e 5000 -b 300

# Send 2000 events with batch size 100 and concurrency of 10
python max_throughout_new.py -e 2000 -b 100 -c 10
```

---