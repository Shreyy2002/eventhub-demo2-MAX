// Program.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Primitives;    // <-- ADD THIS
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace EventHubBatchConsumer
{
    // Subclass EventProcessorClient to override the batch callback
    class PatientAdmissionProcessor : EventProcessorClient
    {
        private readonly int _batchSize;
        private readonly int _maxWaitSeconds;

        private long _totalEvents = 0;
        private long _totalBatches = 0;

        public PatientAdmissionProcessor(BlobContainerClient checkpointStore,
                                         string consumerGroup,
                                         string fullyQualifiedNamespaceOrConnectionString,
                                         string eventHubName,
                                         int batchSize,
                                         int maxWaitSeconds,
                                         EventProcessorClientOptions? options = null)
            : base(checkpointStore, consumerGroup, fullyQualifiedNamespaceOrConnectionString, eventHubName, options)
        {
            _batchSize = batchSize;
            _maxWaitSeconds = maxWaitSeconds;
        }

        // JSON extractor matching your Python: returns dictionary-like data
        private static Dictionary<string, object?> ExtractPatientInfo(EventData evt)
        {
            try
            {
                string body = evt.EventBody.ToString() ?? "";
                using var doc = JsonDocument.Parse(body);
                if (doc.RootElement.TryGetProperty("JSONData", out var jsonData) &&
                    jsonData.TryGetProperty("D_PATIENTADMISSIONADVICE", out var dPatient) &&
                    dPatient.ValueKind == JsonValueKind.Array &&
                    dPatient.GetArrayLength() > 0)
                {
                    var patient = dPatient[0];
                    string? adtId = patient.TryGetProperty("D_PatientAdmissionAdviceId_ADT", out var x) && x.ValueKind != JsonValueKind.Null ? x.GetString() : null;
                    string? name = patient.TryGetProperty("PatientName", out var n) && n.ValueKind != JsonValueKind.Null ? n.GetString() : null;
                    string? contact = patient.TryGetProperty("ContactNumber", out var c) && c.ValueKind != JsonValueKind.Null ? c.GetString() : null;
                    string? speciality = patient.TryGetProperty("SpecialityId", out var s) && s.ValueKind != JsonValueKind.Null ? s.GetString() : null;
                    string? doctor = patient.TryGetProperty("TreatingDoctorId", out var d) && d.ValueKind != JsonValueKind.Null ? d.GetString() : null;

                    return new Dictionary<string, object?> {
                        ["adt_id"] = adtId,
                        ["patient_name"] = name,
                        ["contact"] = contact,
                        ["speciality_id"] = speciality,
                        ["doctor_id"] = doctor
                    };
                }

                return new Dictionary<string, object?> {
                    ["adt_id"] = null,
                    ["patient_name"] = "ERROR",
                    ["contact"] = "N/A",
                    ["speciality_id"] = null,
                    ["doctor_id"] = null,
                    ["error"] = "Missing expected JSON structure"
                };
            }
            catch (Exception ex)
            {
                return new Dictionary<string, object?> {
                    ["adt_id"] = null,
                    ["patient_name"] = "ERROR",
                    ["contact"] = "N/A",
                    ["speciality_id"] = null,
                    ["doctor_id"] = null,
                    ["error"] = ex.Message
                };
            }
        }

        // Protected batch override used by the SDK
        protected override async Task OnProcessingEventBatchAsync(IEnumerable<EventData> events,
                                                                 EventProcessorPartition partition,
                                                                 CancellationToken cancellationToken)
        {
            var eventList = events?.ToList() ?? new List<EventData>();
            if (eventList.Count == 0)
            {
                // nothing to do
                return;
            }

            _totalBatches++;
            _totalEvents += eventList.Count;

            var adtIds = new List<string?>();
            var patients = new List<Dictionary<string, object?>>();

            foreach (var e in eventList)
            {
                var patientInfo = ExtractPatientInfo(e);
                patients.Add(patientInfo);
                adtIds.Add(patientInfo.ContainsKey("adt_id") ? (string?)patientInfo["adt_id"] : null);
            }

            // Simulate processing time exactly like Python: 0.03s * number of events
            int delayMs = 30 * eventList.Count;
            try { await Task.Delay(delayMs, cancellationToken); } catch (TaskCanceledException) { /* ignore */ }

            // Checkpoint: use CheckpointPosition.FromEvent on the last event
            var lastEvent = eventList[eventList.Count - 1];
            var checkpointPos = CheckpointPosition.FromEvent(lastEvent);

            // Persist checkpoint for the partition (protected helper)
            await UpdateCheckpointAsync(partition.PartitionId, checkpointPos, cancellationToken);

            // Logging in same format as Python version
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            string adtPreview = adtIds.Count > 5 ? string.Join(", ", adtIds.Take(5)) + "..." : string.Join(", ", adtIds);

            // Use OffsetString instead of Offset (Offset is obsolete)
            Console.WriteLine($"[{timestamp}]✅ Batch #{_totalBatches} | Partition {partition.PartitionId} | " +
                              $"{eventList.Count} events | Total: {_totalEvents} | " +
                              $"Checkpoint: offset={lastEvent.OffsetString}, seq={lastEvent.SequenceNumber} | " +
                              $"ADT IDs: {adtPreview}");

            if (patients.Count > 0 && patients[0].ContainsKey("adt_id") && patients[0]["adt_id"] != null)
            {
                var fp = patients[0];
                Console.WriteLine($"    └─ First: ADT={fp["adt_id"]}, Name={fp["patient_name"]}, Contact={fp["contact"]}");
            }
        }

        protected override Task OnProcessingErrorAsync(Exception exception,
                                                       EventProcessorPartition partition,
                                                       string operation,
                                                       CancellationToken cancellationToken)
        {
            Console.WriteLine($"[Error] Partition '{partition?.PartitionId ?? "<none>"}' - {exception.Message} (operation: {operation})");
            return Task.CompletedTask;
        }
    }

    class Program
    {
        // Replace with environment / secret injection for production
        private const string EVENTHUB_CONNECTION_STR = "Endpoint=sb://ns-for-testing.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=bQ4ATBDxRUaumilXUVCFqjRFTQG4tqN2p+AEhCAQE7w=;EntityPath=triall";
        private const string EVENTHUB_NAME = "triall";
        private const string STORAGE_CONNECTION_STR = "DefaultEndpointsProtocol=https;AccountName=safortestingg;AccountKey=MFgv2/Bk8WBRh8LR02ejzDV6VDP3GoBVHEBLDuVpD269pGUMwmCxmRrJTSXphRDFqHHkYH61yonC+AStZa9Oog==;EndpointSuffix=core.windows.net";
        private const string CHECKPOINT_CONTAINER = "testt";

        static async Task<int> Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: dotnet run -- <consumerGroup> [batchSize] [maxWaitTimeSeconds]");
                Console.WriteLine("Example: dotnet run -- cgfortest 50 10");
                return 1;
            }

            string consumerGroup = args[0];
            int batchSize = args.Length > 1 ? int.Parse(args[1]) : 50;
            int maxWait = args.Length > 2 ? int.Parse(args[2]) : 10;

            var blobClient = new BlobContainerClient(STORAGE_CONNECTION_STR, CHECKPOINT_CONTAINER);
            await blobClient.CreateIfNotExistsAsync();

            // Configure EventProcessorClientOptions to tune maximum wait time between batches
            var options = new EventProcessorClientOptions
            {
                MaximumWaitTime = TimeSpan.FromSeconds(maxWait)
            };

            var processor = new PatientAdmissionProcessor(blobClient, consumerGroup, EVENTHUB_CONNECTION_STR, EVENTHUB_NAME, batchSize, maxWait, options);

            // --- SDK quirk: StartProcessingAsync requires at least one public ProcessEventAsync handler to be registered.
            // Register a no-op ProcessEventAsync handler so StartProcessingAsync() does not throw.
            processor.ProcessEventAsync += async (ProcessEventArgs args) =>
            {
                // no-op by design: the real batch processing is done in the protected override OnProcessingEventBatchAsync.
                await Task.CompletedTask;
            };

            // Also register a public ProcessErrorAsync (useful for error logging)
            processor.ProcessErrorAsync += async (ProcessErrorEventArgs e) =>
            {
                Console.WriteLine($"[Error] Partition '{e.PartitionId ?? "<none>"}' - {e.Exception.Message}");
                await Task.CompletedTask;
            };

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                Console.WriteLine("\n⚠️ Stopped | Ctrl+C received - shutting down...");
                cts.Cancel();
                e.Cancel = true;
            };

            try
            {
                await processor.StartProcessingAsync(cts.Token);
                // run until cancelled
                try { await Task.Delay(Timeout.Infinite, cts.Token); } catch (TaskCanceledException) { }
                await processor.StopProcessingAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal: {ex.Message}");
            }

            return 0;
        }
    }
}
