// Program.cs - Event Hub throughput tester (C# .NET 8)
// Requires NuGet: Azure.Messaging.EventHubs, Azure.Messaging.EventHubs.Producer, Bogus, Polly

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Bogus;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Polly;
using Polly.Retry;

#nullable enable

// ---------------- CONFIG ----------------
public static class Config
{
    // IMPORTANT: Provide namespace-level connection string WITHOUT EntityPath.
    // Example: "Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=..."
    public const string CONNECTION_STR = "Endpoint=sb://ns-for-testing.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=bQ4ATBDxRUaumilXUVCFqjRFTQG4tqN2p+AEhCAQE7w=";
    public const string EVENTHUB_NAME = "triall";

    public const int DEFAULT_EVENT_COUNT = 1000;
    public const int DEFAULT_BATCH_SIZE = 200;
    public const int DEFAULT_CONCURRENT_BATCHES = 5;

    public const long CONTACT_NUMBER_MIN = 9000000000;
    public const long CONTACT_NUMBER_MAX = 9999999999;
    public const string ADT_ID_FILE = "adt_id_counter.txt";
    public const int ADT_ID_START = 613600;

    // Retry settings
    public const int SEND_MAX_RETRIES = 4;
    public static readonly TimeSpan SEND_RETRY_BASE_DELAY = TimeSpan.FromMilliseconds(200);
    public static readonly TimeSpan SEND_TIMEOUT = TimeSpan.FromSeconds(30);
}
// -----------------------------------------

// ---------------- Typed payload models ----------------
public class PayloadRoot
{
    [JsonPropertyName("JSONData")]
    public JSONData JSONData { get; set; } = new JSONData();
}

public class JSONData
{
    [JsonPropertyName("Table1")]
    public object[] Table1 { get; set; } = new object[] { new { Name = "PatientAdmissionAdvice" } };

    [JsonPropertyName("D_PATIENTADMISSIONADVICE")]
    public PatientAdmissionAdvice[] D_PATIENTADMISSIONADVICE { get; set; } = Array.Empty<PatientAdmissionAdvice>();

    [JsonPropertyName("M_IPWAITINGLIST")]
    public object[] M_IPWAITINGLIST { get; set; } = Array.Empty<object>();

    [JsonPropertyName("D_PATIENTREGISTRATIONLOG")]
    public object[] D_PATIENTREGISTRATIONLOG { get; set; } = Array.Empty<object>();

    [JsonPropertyName("D_PATIENTADMISSIONREQUEST")]
    public object[] D_PATIENTADMISSIONREQUEST { get; set; } = Array.Empty<object>();

    [JsonPropertyName("D_PATIENTADMISSIONADVICE_UPDATE")]
    public object[] D_PATIENTADMISSIONADVICE_UPDATE { get; set; } = Array.Empty<object>();
}

public class PatientAdmissionAdvice
{
    [JsonPropertyName("PatientName")]
    public string? PatientName { get; set; }

    [JsonPropertyName("ContactNumber")]
    public string? ContactNumber { get; set; }

    [JsonPropertyName("D_PatientAdmissionAdviceId_ADT")]
    public int D_PatientAdmissionAdviceId_ADT { get; set; }

    [JsonPropertyName("KafkaGUID")]
    public string? KafkaGUID { get; set; }
}
// -------------------------------------------------------

// ------------------ EventHub producer singleton ------------------
public sealed class EventHubProducerSingleton : IAsyncDisposable
{
    private static readonly Lazy<EventHubProducerSingleton> _lazy = new(() => new EventHubProducerSingleton());
    public static EventHubProducerSingleton Instance => _lazy.Value;

    private EventHubProducerClient? _client;
    private readonly AsyncRetryPolicy _sendRetryPolicy;
    private readonly JsonSerializerOptions _jsonOptions;

    private EventHubProducerSingleton()
    {
        // Configure retry policy (exponential backoff) for sending batches
        _sendRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(
                retryCount: Config.SEND_MAX_RETRIES,
                sleepDurationProvider: retryAttempt => TimeSpan.FromMilliseconds(Config.SEND_RETRY_BASE_DELAY.TotalMilliseconds * Math.Pow(2, retryAttempt - 1)),
                onRetry: (ex, ts, retryCount, ctx) =>
                {
                    Console.WriteLine($"[Producer] Retry {retryCount} after {ts.TotalMilliseconds}ms due to: {ex.Message}");
                });

        _jsonOptions = new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNamingPolicy = null
        };
    }

    // Initialize the client (idempotent). Use connectionString WITHOUT EntityPath and pass eventHubName explicitly.
    public void Initialize(string connectionStringWithoutEntityPath, string eventHubName)
    {
        if (_client != null) return;
        _client = new EventHubProducerClient(connectionStringWithoutEntityPath, eventHubName);
    }

    public async Task<EventDataBatch> CreateBatchAsync(CreateBatchOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (_client == null) throw new InvalidOperationException("Producer not initialized.");
        return await _client.CreateBatchAsync(options, cancellationToken);
    }

    // Send batch with retry policy and overall timeout
    public async Task SendAsync(EventDataBatch batch, CancellationToken cancellationToken = default)
    {
        if (_client == null) throw new InvalidOperationException("Producer not initialized.");

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(Config.SEND_TIMEOUT);

        await _sendRetryPolicy.ExecuteAsync(async ct =>
        {
            await _client.SendAsync(batch, ct).ConfigureAwait(false);
        }, cts.Token).ConfigureAwait(false);
    }

    // Convenience: send a single message (wraps in a small batch)
    public async Task SendSingleAsync(string json, CancellationToken cancellationToken = default)
    {
        using var batch = await CreateBatchAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!batch.TryAdd(new EventData(json)))
            throw new InvalidOperationException("Single event too large for an empty batch.");
        await SendAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_client != null)
        {
            await _client.DisposeAsync().ConfigureAwait(false);
            _client = null;
        }
    }
}
// ------------------------------------------------------------------

// ---------------- Payload generator ----------------
public class PayloadGenerator
{
    // Thread-local Faker for safer concurrent generation without locks
    private static readonly ThreadLocal<Faker> _threadFaker = new(() => new Faker());
    private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = null
    };

    public (string json, string name, string contact) Generate(int adtId)
    {
        var faker = _threadFaker.Value!;

        string patientName = $"{faker.Name.FirstName()} {faker.Name.LastName()}";
        long contactLong = Random.Shared.NextInt64(Config.CONTACT_NUMBER_MIN, Config.CONTACT_NUMBER_MAX);
        string contact = contactLong.ToString();

        var payload = new PayloadRoot
        {
            JSONData = new JSONData
            {
                D_PATIENTADMISSIONADVICE = new[]
                {
                    new PatientAdmissionAdvice
                    {
                        PatientName = patientName,
                        ContactNumber = contact,
                        D_PatientAdmissionAdviceId_ADT = adtId,
                        KafkaGUID = "A2716FC5-374E-4F4F-8A72-C46CA14E8B41"
                    }
                }
            }
        };

        string json = JsonSerializer.Serialize(payload, _jsonOptions);
        return (json, patientName, contact);
    }
}
// --------------------------------------------------

// ---------------- EventHubTester (keeps core logic) ----------------
public class EventHubTester : IAsyncDisposable
{
    private readonly PayloadGenerator _payloadGen = new();
    private int _adtId;
    public int InitialAdt { get; }
    public int AdtId => _adtId;
    public int SentCount { get; private set; }
    private readonly SemaphoreSlim _adtLock = new(1, 1);

    private readonly List<(string Name, string Contact, int AdtId)> _samples = new();
    public IReadOnlyList<(string Name, string Contact, int AdtId)> Samples => _samples.AsReadOnly();

    public int BatchCounter { get; private set; } = 0;
    private double _startTimeSeconds;

    public EventHubTester(int startAdt)
    {
        _adtId = startAdt;
        InitialAdt = startAdt;
        SentCount = 0;
    }

    public async Task<int> GetAdtIdAsync()
    {
        await _adtLock.WaitAsync().ConfigureAwait(false);
        try
        {
            int current = _adtId;
            _adtId++;
            return current;
        }
        finally
        {
            _adtLock.Release();
        }
    }

    // Correct batch-processing loop with mutable batch and robust send
    public async Task<bool> SendBatchAsync(int batchSize, CancellationToken cancellationToken)
    {
        try
        {
            int batchStartAdt = _adtId;
            // start with a fresh batch
            EventDataBatch currentBatch = await EventHubProducerSingleton.Instance.CreateBatchAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            for (int i = 0; i < batchSize; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int adt = await GetAdtIdAsync().ConfigureAwait(false);
                var (payloadJson, name, contact) = _payloadGen.Generate(adt);

                lock (_samples)
                {
                    if (_samples.Count < 3) _samples.Add((name, contact, adt));
                }

                var eventData = new EventData(payloadJson);

                if (!currentBatch.TryAdd(eventData))
                {
                    // send the full current batch
                    if (currentBatch.Count > 0)
                    {
                        await EventHubProducerSingleton.Instance.SendAsync(currentBatch, cancellationToken).ConfigureAwait(false);
                        SentCount += currentBatch.Count;
                        BatchCounter++;
                    }

                    // create a fresh batch and add current event
                    currentBatch = await EventHubProducerSingleton.Instance.CreateBatchAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    if (!currentBatch.TryAdd(eventData))
                    {
                        // single event too large
                        throw new InvalidOperationException("Single event too large for an empty batch.");
                    }
                }
            }

            // send any remaining events
            if (currentBatch.Count > 0)
            {
                await EventHubProducerSingleton.Instance.SendAsync(currentBatch, cancellationToken).ConfigureAwait(false);
                SentCount += currentBatch.Count;
                BatchCounter++;

                int batchEndAdt = _adtId - 1;
                double elapsed = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0) - _startTimeSeconds;
                string timestamp = DateTime.Now.ToString("HH:mm:ss");

                Console.WriteLine($"  [{timestamp}] Batch #{BatchCounter,3} | Size: {currentBatch.Count,3} events | ADT: {batchStartAdt}-{batchEndAdt} | Total: {SentCount,5} | Elapsed: {elapsed:F2}s");
            }

            return true;
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("  ✗ Batch cancelled due to shutdown.");
            return false;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ Error in batch #{BatchCounter + 1}: {ex.Message}");
            return false;
        }
    }

    public async Task<double> RunTestAsync(int targetCount, int batchSize, int concurrentBatches, CancellationToken cancellationToken)
    {
        _startTimeSeconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;
        int totalBatches = (targetCount + batchSize - 1) / batchSize;
        int batchesScheduled = 0;

        while (batchesScheduled < totalBatches && SentCount < targetCount)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var tasks = new List<Task<bool>>();
            int canSchedule = Math.Min(concurrentBatches, totalBatches - batchesScheduled);

            for (int i = 0; i < canSchedule; i++)
            {
                int remaining = targetCount - SentCount;
                int currentBatchSize = Math.Min(batchSize, Math.Max(0, remaining));
                if (currentBatchSize > 0)
                {
                    tasks.Add(SendBatchAsync(currentBatchSize, cancellationToken));
                    batchesScheduled++;
                }
            }

            if (tasks.Count > 0)
            {
                // Wait for all scheduled tasks but allow them to run concurrently
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }

            if (SentCount >= targetCount) break;

            // small yield to allow cancellations and avoid tight loop
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }

        double runSeconds = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0) - _startTimeSeconds;
        return runSeconds;
    }

    public Task CloseAsync() => Task.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
// --------------------------------------------------------------------

// ------------------ Program entry & utilities (CLI friendly) ------------------
static class Program
{
    static async Task<int> Main(string[] args)
    {
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (s, e) =>
        {
            Console.WriteLine("Shutdown requested (Ctrl+C)...");
            e.Cancel = true;
            cts.Cancel();
        };

        var (eventCount, batchSize, concurrent) = ParseArguments(args);

        Console.WriteLine();
        Console.WriteLine(new string('=', 70));
        Console.WriteLine("AZURE EVENT HUB - PATIENT ADMISSION THROUGHPUT TEST");
        Console.WriteLine(new string('=', 70));

        int startAdt = LoadAdtId();
        Log("Test Configuration:");
        Console.WriteLine($"  • Target Events: {eventCount:N0}");
        Console.WriteLine($"  • Batch Size: {batchSize}");
        Console.WriteLine($"  • Concurrent Batches: {concurrent}");
        Console.WriteLine($"  • Starting ADT ID: {startAdt:N0}");

        // Initialize producer singleton (namespace-level connection string without EntityPath)
        EventHubProducerSingleton.Instance.Initialize(Config.CONNECTION_STR, Config.EVENTHUB_NAME);

        var tester = new EventHubTester(startAdt);

        Log("Sending events to Event Hub...");
        var sw = Stopwatch.StartNew();
        try
        {
            double testTime = await tester.RunTestAsync(eventCount, batchSize, concurrent, cts.Token);
            double throughput = tester.SentCount > 0 ? tester.SentCount / testTime : 0;

            Console.WriteLine();
            Console.WriteLine(new string('=', 70));
            Console.WriteLine("TEST RESULTS");
            Console.WriteLine(new string('=', 70));
            Log("Test Completed");
            Console.WriteLine($"✓ Events Sent: {tester.SentCount:N0}");
            Console.WriteLine($"✓ Throughput: {throughput:F1} events/second");
            Console.WriteLine($"✓ Test Duration: {testTime:F2} seconds");
            Console.WriteLine($"✓ ADT ID Range: {tester.InitialAdt:N0} to {tester.AdtId - 1:N0}");

            if (tester.Samples.Count > 0)
            {
                Console.WriteLine("\nSample Patient Records (Verification):");
                for (int i = 0; i < Math.Min(3, tester.Samples.Count); i++)
                {
                    var s = tester.Samples[i];
                    Console.WriteLine($"  {i + 1}. {s.Name,-25} | Contact: {s.Contact} | ADT ID: {s.AdtId}");
                }
            }

            SaveAdtId(tester.AdtId - 1);

            Console.WriteLine();
            Console.WriteLine(new string('=', 70));
            Log("Script completed successfully");
            Console.WriteLine(new string('=', 70));
        }
        catch (OperationCanceledException)
        {
            Log("Operation cancelled by user.");
        }
        catch (Exception ex)
        {
            Log($"Error: {ex}");
        }
        finally
        {
            // Dispose producer singleton
            await EventHubProducerSingleton.Instance.DisposeAsync().ConfigureAwait(false);
        }

        return 0;
    }

    static void Log(string message)
    {
        var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        Console.WriteLine($"[{ts}] {message}");
    }

    // ---------------- ADT ID management
    static int LoadAdtId()
    {
        if (File.Exists(Config.ADT_ID_FILE))
        {
            try
            {
                var txt = File.ReadAllText(Config.ADT_ID_FILE).Trim();
                if (int.TryParse(txt, out var v)) return v + 1;
                return Config.ADT_ID_START;
            }
            catch
            {
                return Config.ADT_ID_START;
            }
        }
        return Config.ADT_ID_START;
    }

    static void SaveAdtId(int lastId)
    {
        try
        {
            File.WriteAllText(Config.ADT_ID_FILE, lastId.ToString());
        }
        catch
        {
            // ignore write failures
        }
    }

    // ---------------- CLI parsing (manual)
    static (int events, int batch, int concurrent) ParseArguments(string[] args)
    {
        int eventCount = Config.DEFAULT_EVENT_COUNT;
        int batchSize = Config.DEFAULT_BATCH_SIZE;
        int concurrent = Config.DEFAULT_CONCURRENT_BATCHES;

        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            if (a == "-e" || a == "--events")
            {
                if (i + 1 < args.Length && int.TryParse(args[i + 1], out var v)) { eventCount = v; i++; }
                else { Console.WriteLine($"Error: Invalid event count '{(i + 1 < args.Length ? args[i + 1] : "")}'"); PrintUsageAndExit(); }
            }
            else if (a == "-b" || a == "--batch")
            {
                if (i + 1 < args.Length && int.TryParse(args[i + 1], out var v)) { batchSize = v; i++; }
                else { Console.WriteLine($"Error: Invalid batch size '{(i + 1 < args.Length ? args[i + 1] : "")}'"); PrintUsageAndExit(); }
            }
            else if (a == "-c" || a == "--concurrent")
            {
                if (i + 1 < args.Length && int.TryParse(args[i + 1], out var v)) { concurrent = v; i++; }
                else { Console.WriteLine($"Error: Invalid concurrent batches '{(i + 1 < args.Length ? args[i + 1] : "")}'"); PrintUsageAndExit(); }
            }
            else if (a == "-h" || a == "--help")
            {
                PrintUsageAndExit();
            }
            else
            {
                Console.WriteLine($"Error: Unknown argument '{a}'");
                PrintUsageAndExit();
            }
        }

        return (eventCount, batchSize, concurrent);
    }

    static void PrintUsageAndExit()
    {
        Console.WriteLine();
        Console.WriteLine("Usage: dotnet run -- [OPTIONS]");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  -e, --events <number>       Number of events to send (default: 1000)");
        Console.WriteLine("  -b, --batch <number>        Batch size (default: 200)");
        Console.WriteLine("  -c, --concurrent <number>   Concurrent batches (default: 5)");
        Console.WriteLine("  -h, --help                  Show this help message");
        Console.WriteLine();
        Console.WriteLine("Examples:");
        Console.WriteLine("  dotnet run -- -e 1000 -b 200 -c 5");
        Environment.Exit(0);
    }
}
