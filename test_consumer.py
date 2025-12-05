using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// -------------------- STUBS & PAYLOAD CLASSES (Kept from your original code) --------------------
public interface IADT { Task<string> TransferToHISDB(string eventBody); Task<List<string>> TransferToHISDBWithTableType(string eventBody); }
public class DefaultADT : IADT { public Task<string> TransferToHISDB(string eventBody) => Task.FromResult("{\"result\":\"ok\"}"); public Task<List<string>> TransferToHISDBWithTableType(string eventBody) => Task.FromResult(new List<string>()); }
public class ConsumerConfig { public string? ConsumerGroup { get; set; } public int PrefetchCount { get; set; } = 500; } // Renamed BatchSize to PrefetchCount and increased default
public class PayloadRoot { [JsonPropertyName("JSONData")] public JSONData? JSONData { get; set; } }
public class JSONData { [JsonPropertyName("D_PATIENTADMISSIONADVICE")] public PatientAdmissionAdvice[]? D_PATIENTADMISSIONADVICE { get; set; } }
public class PatientAdmissionAdvice { /* Properties remain the same */ }
// ---------------------------------------------------------------------------------------------

// KafkaProducer (Kept from your original code)
public sealed class KafkaProducer : IAsyncDisposable { /* Implementation remains the same */ 
    private static readonly Lazy<KafkaProducer> _lazyInstance = new(() => new KafkaProducer());
    public static KafkaProducer Instance => _lazyInstance.Value;
    private EventHubProducerClient? _eventHubClient; private bool _initialized = false;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions { PropertyNamingPolicy = null, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };
    private KafkaProducer() { }
    public async Task InitializeAsync(string connectionStringWithoutEntityPath, string eventHubName) { /* ... */ }
    public async Task SendMessageToEventHubAsync(string json) { /* ... */ }
    public async Task<EventDataBatch> CreateBatchAsync() { /* ... */ }
    public async Task SendBatchAsync(EventDataBatch batch) { /* ... */ }
    public async ValueTask DisposeAsync() { /* ... */ }
}

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        string? cliConsumerGroup = args.Length > 0 ? args[0] : null;
        int cliPrefetchCount = args.Length > 1 ? (int.TryParse(args[1], out var p) ? p : 500) : 500;

        var consumerConfig = new ConsumerConfig
        {
            ConsumerGroup = cliConsumerGroup,
            PrefetchCount = cliPrefetchCount,
        };

        using IHost host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                services.AddSingleton<IADT, DefaultADT>();
                services.AddSingleton<KafkaProducer>(_ => KafkaProducer.Instance);
                services.AddSingleton(consumerConfig);
                services.AddHostedService<EventHubProcessorWorker>(); // Renamed the worker class
            })
            .ConfigureLogging(logging => {
                logging.ClearProviders();
                logging.AddConsole();
            })
            .Build();

        await host.RunAsync();
        return 0;
    }
}

// -------------------- NEW WORKER (using EventProcessorClient) --------------------
public class EventHubProcessorWorker : BackgroundService
{
    private readonly ILogger<EventHubProcessorWorker> _logger;
    private readonly IADT _ADT;
    private readonly ConsumerConfig _config;
    private EventProcessorClient? _processor;

    // Use environment variables for connection strings in production
    private const string EVENTHUB_CONNECTION_STR = "Endpoint=sb://ns-for-testing.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EwI67+VG8kY7hlhq8qXqSMwT1TEL/ZP15+AEhIWpOpo=";
    private const string EVENTHUB_NAME = "triall";
    private const string STORAGE_CONNECTION_STR = "DefaultEndpointsProtocol=https;AccountName=safortestingg;AccountKey=MFgv2/Bk8WBRh8LR02ejzDV6VDP3GoBVHEBLDuVpD269pGUMwmCxmRrJTSXphRDFqHHkYH61yonC+AStZa9Oog==;EndpointSuffix=core.windows.net";
    private const string STORAGE_CONTAINER_NAME = "testt"

    public EventHubProcessorWorker(ILogger<EventHubProcessorWorker> logger, IADT adt, ConsumerConfig config)
    {
        _logger = logger;
        _ADT = adt;
        _config = config;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Event Hub Processor Worker running at: {Time}", DateTimeOffset.Now);

        var blobContainerClient = new BlobContainerClient(STORAGE_CONNECTION_STR, STORAGE_CONTAINER_NAME);

        // Configure options to increase potential batch ingestion
        var options = new EventProcessorClientOptions
        {
            PrefetchCount = _config.PrefetchCount, // Use the configured (larger) value
            MaximumWaitTime = TimeSpan.FromSeconds(30), // You can tune the max wait time here
        };

        _processor = new EventProcessorClient(
            blobContainerClient,
            _config.ConsumerGroup ?? EventHubConsumerClient.DefaultConsumerGroupName,
            EVENTHUB_CONNECTION_STR,
            EVENTHUB_NAME,
            options);
        
        // Register the handlers
        _processor.ProcessEventAsync += ProcessEventHandler;
        _processor.ProcessErrorAsync += ProcessErrorHandler;

        // Start processing in the background
        await _processor.StartProcessingAsync(stoppingToken);

        _logger.LogInformation("EventProcessorClient started. Waiting for events...");

        try
        {
            // Keep the service running until the host shuts down
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (TaskCanceledException)
        {
            _logger.LogInformation("Processing is stopping.");
            await _processor.StopProcessingAsync();
        }
    }

    private async Task ProcessEventHandler(ProcessEventArgs eventArgs)
    {
        try
        {
            // eventArgs.Data gives you the *single* event data, but the internal client 
            // uses PrefetchCount to optimize how many events it pulls from Azure in bulk.
            // Your application logic inside this handler deals with one event at a time *after* fetching.

            string eventBody = eventArgs.Data.EventBody.ToString();
            _logger.LogInformation("Processing event sequence number: {SequenceNumber}", eventArgs.Data.SequenceNumber);

            // Deserialize and call business logic
            var payloadRoot = JsonSerializer.Deserialize<PayloadRoot>(eventBody);
            if (payloadRoot?.JSONData?.D_PATIENTADMISSIONADVICE != null)
            {
                foreach (var advice in payloadRoot.JSONData.D_PATIENTADMISSIONADVICE)
                {
                    // Pass specific patient advice to your ADT system
                    await _ADT.TransferToHISDB(JsonSerializer.Serialize(advice));
                }
            }

            // Checkpoint (crucial for resume/load balancing)
            // The processor automatically manages batches for efficiency, you just checkpoint when an event (or a logical group) is done.
            await eventArgs.UpdateCheckpointAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing event from partition {PartitionId}", eventArgs.Partition.PartitionId);
            // The ProcessError handler will also catch underlying library errors
        }
    }

    private Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
    {
        _logger.LogError(eventArgs.Exception, "Error in the EventProcessorClient operation: {Operation}", eventArgs.Operation);
        return Task.CompletedTask;
    }
}