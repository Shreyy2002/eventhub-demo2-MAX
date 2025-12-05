# batch_consumer_patient_admission_sdk_batching.py
import asyncio
import sys
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.storage.blob.aio import BlobServiceClient
import json
from datetime import datetime


# ---------------------------
# FIXED CONFIGURATION VALUES (no os.getenv)
# IMPORTANT: This connection string must NOT contain EntityPath
EVENTHUB_CONNECTION_STR = "Endpoint=sb://ns-for-testing.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EwI67+VG8kY7hlhq8qXqSMwT1TEL/ZP15+AEhIWpOpo="

EVENTHUB_NAME = "triall"

STORAGE_CONNECTION_STR = "DefaultEndpointsProtocol=https;AccountName=safortestingg;AccountKey=MFgv2/Bk8WBRh8LR02ejzDV6VDP3GoBVHEBLDuVpD269pGUMwmCxmRrJTSXphRDFqHHkYH61yonC+AStZa9Oog==;EndpointSuffix=core.windows.net"

CHECKPOINT_CONTAINER = "testt"
# ---------------------------


class PatientAdmissionBatchConsumer:
    def __init__(self, consumer_group, batch_size=200, max_wait_time=10, prefetch=None):
        self.consumer_group = consumer_group or "$Default"
        self.batch_size = int(batch_size)
        self.max_wait_time = int(max_wait_time)
        self.prefetch = int(prefetch) if prefetch else self.batch_size

        self.total_events = 0
        self.total_batches = 0

    def extract_patient_info(self, event):
        """Extract patient info from JSON payload."""
        try:
            payload = json.loads(event.body_as_str())
            patient_data = payload["JSONData"]["D_PATIENTADMISSIONADVICE"][0]

            return {
                "adt_id": patient_data.get("D_PatientAdmissionAdviceId_ADT"),
                "patient_name": patient_data.get("PatientName"),
                "contact": patient_data.get("ContactNumber"),
                "speciality_id": patient_data.get("SpecialityId"),
                "doctor_id": patient_data.get("TreatingDoctorId"),
            }

        except Exception as e:
            return {
                "adt_id": None,
                "patient_name": "ERROR",
                "contact": "N/A",
                "speciality_id": None,
                "doctor_id": None,
                "error": str(e),
            }

    async def on_event_batch(self, partition_context, events):
        if not events:
            return

        self.total_batches += 1
        self.total_events += len(events)

        adt_ids = []
        patients = []

        for e in events:
            info = self.extract_patient_info(e)
            adt_ids.append(info["adt_id"])
            patients.append(info)

        # Simulate processing
        await asyncio.sleep(0.03 * len(events))

        # Checkpointing on last event
        await partition_context.update_checkpoint(events[-1])

        last = events[-1]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(
            f"[{timestamp}]  Batch #{self.total_batches} | Partition {partition_context.partition_id} | "
            f"{len(events)} events | Total: {self.total_events} | "
            f"Checkpoint: offset={last.offset}, seq={last.sequence_number} | "
            f"ADT IDs: {adt_ids[:5]}..."
        )

    async def start(self):
        print(
            f"\n Starting SDK Consumer\n"
            f"   Consumer Group = {self.consumer_group}\n"
            f"   max_batch_size = {self.batch_size}\n"
            f"   prefetch       = {self.prefetch}\n"
            f"   max_wait_time  = {self.max_wait_time}s\n"
        )

        # Checkpoint store
        checkpoint_store = BlobCheckpointStore.from_connection_string(
            STORAGE_CONNECTION_STR, CHECKPOINT_CONTAINER
        )

        # Ensure container exists
        try:
            blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STR)
            container = blob_service.get_container_client(CHECKPOINT_CONTAINER)
            if not await container.exists():
                await container.create_container()
                print(f" Created checkpoint container: {CHECKPOINT_CONTAINER}")
        except Exception:
            pass  # ignore create errors

        # Create SDK consumer
        client = EventHubConsumerClient.from_connection_string(
            conn_str=EVENTHUB_CONNECTION_STR,
            consumer_group=self.consumer_group,
            eventhub_name=EVENTHUB_NAME,
            checkpoint_store=checkpoint_store,
        )

        try:
            async with client:
                await client.receive_batch(
                    on_event_batch=self.on_event_batch,
                    starting_position="-1",       # read from EARLIEST if no checkpoint
                    max_batch_size=self.batch_size,
                    max_wait_time=self.max_wait_time,
                    prefetch=self.prefetch,       # REAL batching via link credit
                )
        except KeyboardInterrupt:
            print("\n Consumer stopped by user.")
        except Exception as ex:
            print(f" Consumer error: {ex}")
            raise


async def main():
    """
    CLI mapping (positional):
      1) If first arg is numeric:
           python script.py <batch_size> [max_wait_time] [prefetch]
         Example: python script.py 400 5 2000
      2) If first arg is non-numeric:
           python script.py <consumer_group> [batch_size] [max_wait_time] [prefetch]
         Example: python script.py '$Default' 400 5 2000
    """

    argv = sys.argv[1:]
    consumer_group = None
    batch = 50
    wait = 10
    prefetch = None

    if not argv:
        # no args -> use defaults
        pass
    else:
        # If first arg is numeric -> treat as batch_size
        if argv[0].lstrip('-').isdigit():
            batch = int(argv[0])
            if len(argv) >= 2 and argv[1].lstrip('-').isdigit():
                wait = int(argv[1])
            if len(argv) >= 3 and argv[2].lstrip('-').isdigit():
                prefetch = int(argv[2])
        else:
            # first arg is non-numeric -> consumer_group
            consumer_group = argv[0]
            if len(argv) >= 2 and argv[1].lstrip('-').isdigit():
                batch = int(argv[1])
            if len(argv) >= 3 and argv[2].lstrip('-').isdigit():
                wait = int(argv[2])
            if len(argv) >= 4 and argv[3].lstrip('-').isdigit():
                prefetch = int(argv[3])

    if not consumer_group:
        consumer_group = "$Default"

    # If prefetch not provided, set it to 2x batch as a sensible default
    if prefetch is None:
        prefetch = max(batch * 2, batch)

    # Print what we will use for clarity
    print(
        f"\n Starting SDK Consumer\n"
        f"   Consumer Group = {consumer_group}\n"
        f"   max_batch_size = {batch}\n"
        f"   prefetch       = {prefetch}\n"
        f"   max_wait_time  = {wait}s\n"
    )

    consumer = PatientAdmissionBatchConsumer(consumer_group, batch_size=batch, max_wait_time=wait, prefetch=prefetch)
    await consumer.start()



if __name__ == "__main__":
    asyncio.run(main())
