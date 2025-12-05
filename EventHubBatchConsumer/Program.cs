using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace PatientAdmissionConsumer
{
    public class PatientInfo
    {
        public string AdtId { get; set; }
        public string PatientName { get; set; }
        public string Contact { get; set; }
        public string SpecialityId { get; set; }
        public string DoctorId { get; set; }
        public string Error { get; set; }
    }

    public class PatientAdmissionBatchConsumer
    {
        // FIXED CONFIGURATION VALUES
        private const string EVENTHUB_CONNECTION_STR = "Endpoint=sb://ns-for-testing.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EwI67+VG8kY7hlhq8qXqSMwT1TEL/ZP15+AEhIWpOpo=";
        private const string EVENTHUB_NAME = "triall";
        private const string STORAGE_CONNECTION_STR = "DefaultEndpointsProtocol=https;AccountName=safortestingg;AccountKey=MFgv2/Bk8WBRh8LR02ejzDV6VDP3GoBVHEBLDuVpD269pGUMwmCxmRrJTSXphRDFqHHkYH61yonC+AStZa9Oog==;EndpointSuffix=core.windows.net";
        private const string CHECKPOINT_CONTAINER = "testt";

        private readonly string _consumerGroup;
        private readonly int _batchSize;
        private readonly int _maxWaitTime;
        private readonly int _prefetch;

        private int _totalEvents = 0;
        private int _totalBatches = 0;
        private readonly object _statsLock = new object();

        public PatientAdmissionBatchConsumer(string consumerGroup, int batchSize = 200, int maxWaitTime = 10, int? prefetch = null)
        {
            _consumerGroup = consumerGroup ?? EventHubConsumerClient.DefaultConsumerGroupName;
            _batchSize = batchSize;
            _maxWaitTime = maxWaitTime;
            _prefetch = prefetch ?? Math.Max(batchSize * 2, batchSize);
        }

        private PatientInfo ExtractPatientInfo(EventData eventData)
        {
            try
            {
                var bodyText = Encoding.UTF8.GetString(eventData.Body.ToArray());
                var doc = JsonDocument.Parse(bodyText);
                
                var patientData = doc.RootElement
                    .GetProperty("JSONData")
                    .GetProperty("D_PATIENTADMISSIONADVICE")[0];

                return new PatientInfo
                {
                    AdtId = GetStringProperty(patientData, "D_PatientAdmissionAdviceId_ADT"),
                    PatientName = GetStringProperty(patientData, "PatientName"),
                    Contact = GetStringProperty(patientData, "ContactNumber"),
                    SpecialityId = GetStringProperty(patientData, "SpecialityId"),
                    DoctorId = GetStringProperty(patientData, "TreatingDoctorId")
                };
            }
            catch (Exception ex)
            {
                return new PatientInfo
                {
                    PatientName = "ERROR",
                    Contact = "N/A",
                    Error = ex.Message
                };
            }
        }

        private string GetStringProperty(JsonElement element, string propertyName)
        {
            if (element.TryGetProperty(propertyName, out var prop))
            {
                return prop.ValueKind == JsonValueKind.String ? prop.GetString() : prop.ToString();
            }
            return null;
        }

        private async Task ProcessEventBatchAsync(string partitionId, List<EventData> events)
        {
            if (events == null || events.Count == 0)
                return;

            int batchNum;
            int totalEvts;
            
            lock (_statsLock)
            {
                _totalBatches++;
                _totalEvents += events.Count;
                batchNum = _totalBatches;
                totalEvts = _totalEvents;
            }

            var adtIds = new List<string>();
            var patients = new List<PatientInfo>();

            foreach (var evt in events)
            {
                var info = ExtractPatientInfo(evt);
                adtIds.Add(info.AdtId);
                patients.Add(info);
            }

            // Simulate processing
            await Task.Delay(30 * events.Count);

            var lastEvent = events.Last();
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            var displayIds = string.Join(", ", adtIds.Take(5));

            Console.WriteLine(
                $"[{timestamp}]  Batch #{batchNum} | Partition {partitionId} | " +
                $"{events.Count} events | Total: {totalEvts} | " +
                $"Checkpoint: offset={lastEvent.Offset}, seq={lastEvent.SequenceNumber} | " +
                $"ADT IDs: {displayIds}..."
            );
        }

        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            Console.WriteLine(
                $"\n🚀 Starting SDK Consumer\n" +
                $"   Consumer Group = {_consumerGroup}\n" +
                $"   max_batch_size = {_batchSize}\n" +
                $"   prefetch       = {_prefetch}\n" +
                $"   max_wait_time  = {_maxWaitTime}s\n"
            );

            // Ensure checkpoint container exists
            try
            {
                var blobServiceClient = new BlobServiceClient(STORAGE_CONNECTION_STR);
                var containerClient = blobServiceClient.GetBlobContainerClient(CHECKPOINT_CONTAINER);
                await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not create checkpoint container: {ex.Message}");
            }

            // Create event processor client
            var storageClient = new BlobContainerClient(STORAGE_CONNECTION_STR, CHECKPOINT_CONTAINER);
            var processor = new EventProcessorClient(
                storageClient,
                _consumerGroup,
                EVENTHUB_CONNECTION_STR,
                EVENTHUB_NAME,
                new EventProcessorClientOptions
                {
                    PrefetchCount = _prefetch,
                    MaximumWaitTime = TimeSpan.FromSeconds(_maxWaitTime)
                }
            );

            var partitionBatches = new Dictionary<string, List<EventData>>();
            var partitionLocks = new Dictionary<string, SemaphoreSlim>();

            processor.ProcessEventAsync += async args =>
            {
                if (args.CancellationToken.IsCancellationRequested)
                    return;

                var partitionId = args.Partition.PartitionId;

                if (!partitionLocks.ContainsKey(partitionId))
                {
                    lock (partitionLocks)
                    {
                        if (!partitionLocks.ContainsKey(partitionId))
                        {
                            partitionLocks[partitionId] = new SemaphoreSlim(1, 1);
                            partitionBatches[partitionId] = new List<EventData>();
                        }
                    }
                }

                await partitionLocks[partitionId].WaitAsync(args.CancellationToken);
                try
                {
                    partitionBatches[partitionId].Add(args.Data);

                    if (partitionBatches[partitionId].Count >= _batchSize)
                    {
                        var batch = new List<EventData>(partitionBatches[partitionId]);
                        partitionBatches[partitionId].Clear();

                        await ProcessEventBatchAsync(partitionId, batch);
                        await args.UpdateCheckpointAsync(args.CancellationToken);
                    }
                }
                finally
                {
                    partitionLocks[partitionId].Release();
                }
            };

            processor.ProcessErrorAsync += args =>
            {
                Console.WriteLine($"❌ Error on partition {args.PartitionId}: {args.Exception.Message}");
                return Task.CompletedTask;
            };

            try
            {
                await processor.StartProcessingAsync(cancellationToken);
                Console.WriteLine("✅ Event processor started. Press Ctrl+C to stop...");

                // Keep running until cancelled
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\n🛑 Consumer stopped by user.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Consumer error: {ex.Message}");
                throw;
            }
            finally
            {
                await processor.StopProcessingAsync();
            }
        }

        static async Task Main(string[] args)
        {
            string consumerGroup = null;
            int batchSize = 50;
            int maxWaitTime = 10;
            int? prefetch = null;

            // Parse command line arguments
            if (args.Length > 0)
            {
                if (int.TryParse(args[0], out var firstNum))
                {
                    // First arg is numeric -> batch size
                    batchSize = firstNum;
                    if (args.Length >= 2 && int.TryParse(args[1], out var waitNum))
                        maxWaitTime = waitNum;
                    if (args.Length >= 3 && int.TryParse(args[2], out var prefetchNum))
                        prefetch = prefetchNum;
                }
                else
                {
                    // First arg is consumer group
                    consumerGroup = args[0];
                    if (args.Length >= 2 && int.TryParse(args[1], out var batchNum))
                        batchSize = batchNum;
                    if (args.Length >= 3 && int.TryParse(args[2], out var waitNum))
                        maxWaitTime = waitNum;
                    if (args.Length >= 4 && int.TryParse(args[3], out var prefetchNum))
                        prefetch = prefetchNum;
                }
            }

            consumerGroup ??= EventHubConsumerClient.DefaultConsumerGroupName;
            prefetch ??= Math.Max(batchSize * 2, batchSize);

            var consumer = new PatientAdmissionBatchConsumer(consumerGroup, batchSize, maxWaitTime, prefetch);

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            await consumer.StartAsync(cts.Token);
        }
    }
}