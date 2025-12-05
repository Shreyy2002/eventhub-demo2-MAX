import os, sys, asyncio, time, json, random
from datetime import datetime
from faker import Faker
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError


# ============================================================================
# CONFIGURATION
# ============================================================================
CONNECTION_STR = "Endpoint=sb://ns-for-testing.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=bQ4ATBDxRUaumilXUVCFqjRFTQG4tqN2p+AEhCAQE7w="
EVENTHUB_NAME = "triall"

# Default values (can be overridden via command line)
DEFAULT_EVENT_COUNT = 1000
DEFAULT_BATCH_SIZE = 200
DEFAULT_CONCURRENT_BATCHES = 5

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def log(message):
    """Print message with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")


# ============================================================================
# ADT ID MANAGEMENT
# ============================================================================
def load_adt_id():
    """Load the last used ADT ID from file"""
    if os.path.exists(ADT_ID_FILE):
        try:
            with open(ADT_ID_FILE, 'r') as f:
                return int(f.read().strip()) + 1
        except:
            return ADT_ID_START
    return ADT_ID_START


def save_adt_id(last_id):
    """Save the last used ADT ID to file"""
    try:
        with open(ADT_ID_FILE, 'w') as f:
            f.write(str(last_id))
    except:
        pass


# ============================================================================
# PAYLOAD GENERATOR
# ============================================================================
class PayloadGenerator:
    def __init__(self):
        self.fake = Faker()
        self.template = {
            "JSONData": {
                "Table1": [{"Name": "PatientAdmissionAdvice"}],
                "D_PATIENTADMISSIONADVICE": [{
                    "Id": None, "HSPLocationId": 136, "M_PatientId": None,
                    "RegistrationNo": None, "IACode": "MJHL", "LengthOfStay": "4",
                    "ExpDateOfAdmission": "2025-12-06T00:00:00", "ExpArrivalTime": "10:39",
                    "SpecialityId": 48, "TreatingDoctorId": 71767, "BedCategoryId": 8,
                    "SponsorChannelId": 11, "MedicalSurgicalId": 1, "Deleted": 0,
                    "AdvicedDate": "2025-11-12T10:38:00", "VisitId": None,
                    "MedicalCostOfInvRoutine": "342", "MedicalCostOfInvSpecial": "342",
                    "MedicalCostOfPharRoutine": "342", "MedicalCostOfPharSpecial": "234423",
                    "MedicalCostOfOtherCharge1": "", "MedicalCostOfOtherCharge2": "string",
                    "PrimarySurgery": None, "AddiSurgery1": None, "AddiSurgeryCat1": None,
                    "AddiSurgery2": None, "AddiSurgeryCat2": None, "AnesthesiaTypeId": None,
                    "SurgicalCostOfImplant": None, "SurgicalCostOfInvRoutine": None,
                    "SurgicalCostOfInvSpecial": None, "SurgicalCostOfPharRoutine": None,
                    "SurgicalCostOfPharSpecial": None, "AsstSurgeon": None,
                    "SpecialEquipment": None, "BloodUnitsRequirement": None,
                    "ComplicationsHighRiskMarkup": None, "SurgicalCostOfOtherCharge": None,
                    "SecondarySpecialityId": 0, "SecondaryDoctorId": 0,
                    "PrimarySurgeryCat": "", "MedicalCostOfConsumableRoutine": "32",
                    "MedicalCostOfConsumableSpecial": "342",
                    "SurgicalCostOfConsumableRoutine": None,
                    "SurgicalCostOfConsumableSpecial": None, "IPId": 0,
                    "IsEmpatient": False, "Remarks": "test ",
                    "ProvisionalDiagnostic": "test feg3er", "OperatorId": 60210,
                    "IsCprsSaved": False, "VistaId": "",
                    "OtherDesiredBedCategory": "", "IsCancelled": None,
                    "EditedPatientAdmissionAdviceId": 0, "EditorCancelRemarks": "string",
                    "CancelledByVistaId": None, "CancelledBy": None,
                    "CancelledDate": None, "PrimarySpecialityId2": 134,
                    "PrimaryDoctorId2": None, "PatientName": "placeholder",
                    "ContactNumber": "0000000000",
                    "AdmRequestLog_ExpDateOfAdmission": None,
                    "AdmRequestLog_ExpArrivalTime": None,
                    "D_PatientAdmissionAdviceId_ADT": 0, "IsWebHIS": True,
                    "D_PatientAdviceUnregId": 1075,
                    "EditedPatientAdmissionAdviceId_ADT": 0, "IsRegister": False,
                    "AddiSurgery3": None, "AddiSurgeryCat3": None,
                    "AddiSurgery4": None, "AddiSurgeryCat4": None,
                    "AddiSurgery5": None, "AddiSurgeryCat5": None,
                    "KafkaGUID": "A2716FC5-374E-4F4F-8A72-C46CA14E8B41"
                }],
                "M_IPWAITINGLIST": [], "D_PATIENTREGISTRATIONLOG": [],
                "D_PATIENTADMISSIONREQUEST": [], "D_PATIENTADMISSIONADVICE_UPDATE": []
            }
        }
        self._template_str = json.dumps(self.template)
    
    def generate(self, adt_id):
        """Generate unique patient data for each event."""
        payload = json.loads(self._template_str)
        admission = payload["JSONData"]["D_PATIENTADMISSIONADVICE"][0]
        admission["PatientName"] = f"{self.fake.first_name()} {self.fake.last_name()}"
        admission["ContactNumber"] = str(random.randint(CONTACT_NUMBER_MIN, CONTACT_NUMBER_MAX))
        admission["D_PatientAdmissionAdviceId_ADT"] = adt_id
        return json.dumps(payload), admission["PatientName"], admission["ContactNumber"]


# ============================================================================
# EVENT HUB TESTER
# ============================================================================
class EventHubTester:
    def __init__(self, conn_str, eh_name, start_adt):
        self.producer_client = EventHubProducerClient.from_connection_string(
            conn_str=conn_str, eventhub_name=eh_name
        )
        self.payload_gen = PayloadGenerator()
        self.adt_id = start_adt
        self.initial_adt = start_adt
        self.sent_count = 0
        self.adt_lock = asyncio.Lock()
        self.start_time = None
        self.samples = []
        self.batch_counter = 0
    
    async def get_adt_id(self):
        async with self.adt_lock:
            current = self.adt_id
            self.adt_id += 1
            return current
    
    async def send_batch(self, batch_size):
        """Send one batch of events (may internally split into multiple sends if full)."""
        try:
            batch = await self.producer_client.create_batch()
            batch_start_adt = self.adt_id
            
            for _ in range(batch_size):
                adt_id = await self.get_adt_id()
                payload_json, name, contact = self.payload_gen.generate(adt_id)
                
                if len(self.samples) < 3:
                    self.samples.append({
                        "name": name,
                        "contact": contact,
                        "adt_id": adt_id
                    })
                
                event_data = EventData(payload_json)
                
                try:
                    batch.add(event_data)
                except ValueError:
                    if len(batch) > 0:
                        await self.producer_client.send_batch(batch)
                        self.sent_count += len(batch)
                        batch = await self.producer_client.create_batch()
                    batch.add(event_data)
            
            if len(batch) > 0:
                await self.producer_client.send_batch(batch)
                self.sent_count += len(batch)
                self.batch_counter += 1
                batch_end_adt = self.adt_id - 1
                elapsed = time.time() - self.start_time
                timestamp = datetime.now().strftime('%H:%M:%S')
                
                print(
                    f"  [{timestamp}] Batch #{self.batch_counter:>3} | "
                    f"Size: {len(batch):>3} events | "
                    f"ADT: {batch_start_adt}-{batch_end_adt} | "
                    f"Total: {self.sent_count:>5} | "
                    f"Elapsed: {elapsed:.2f}s"
                )
            
            return True
            
        except Exception as e:
            print(f"  ✗ Error in batch #{self.batch_counter + 1}: {e}")
            return False
    
    async def run_test(self, target_count, batch_size, concurrent_batches):
        """Run the entire event sending test."""
        self.start_time = time.time()
        total_batches = (target_count + batch_size - 1) // batch_size
        batches_sent = 0
        
        while batches_sent < total_batches and self.sent_count < target_count:
            tasks = []
            
            for _ in range(min(concurrent_batches, total_batches - batches_sent)):
                remaining = target_count - self.sent_count
                current_batch_size = min(batch_size, remaining)
                
                if current_batch_size > 0:
                    tasks.append(self.send_batch(current_batch_size))
                    batches_sent += 1
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            if self.sent_count >= target_count:
                break
            
            await asyncio.sleep(0.001)
        
        return time.time() - self.start_time
    
    async def close(self):
        """Cleanly close the Event Hub connection"""
        await self.producer_client.close()


# ============================================================================
# CLI ARGUMENT PARSING
# ============================================================================
def parse_arguments():
    """Parse command line arguments."""
    event_count = DEFAULT_EVENT_COUNT
    batch_size = DEFAULT_BATCH_SIZE
    concurrent_batches = DEFAULT_CONCURRENT_BATCHES
    
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        
        if arg in ['-e', '--events']:
            if i + 1 < len(args):
                try:
                    event_count = int(args[i + 1])
                    i += 2
                    continue
                except ValueError:
                    print(f"Error: Invalid event count '{args[i + 1]}'")
                    sys.exit(1)
        
        elif arg in ['-b', '--batch']:
            if i + 1 < len(args):
                try:
                    batch_size = int(args[i + 1])
                    i += 2
                    continue
                except ValueError:
                    print(f"Error: Invalid batch size '{args[i + 1]}'")
                    sys.exit(1)
        
        elif arg in ['-c', '--concurrent']:
            if i + 1 < len(args):
                try:
                    concurrent_batches = int(args[i + 1])
                    i += 2
                    continue
                except ValueError:
                    print(f"Error: Invalid concurrent batches '{args[i + 1]}'")
                    sys.exit(1)
        
        elif arg in ['-h', '--help']:
            print_usage()
            sys.exit(0)
        
        else:
            print(f"Error: Unknown argument '{arg}'")
            print_usage()
            sys.exit(1)
    
    return event_count, batch_size, concurrent_batches

# ============================================================================
# DATA GENERATION CONSTANTS
# ============================================================================
CONTACT_NUMBER_MIN = 9000000000
CONTACT_NUMBER_MAX = 9999999999
ADT_ID_FILE = "adt_id_counter.txt"
ADT_ID_START = 613600

def print_usage():
    """Print usage information."""
    print("\nUsage: python max_throughout_new.py [OPTIONS]")
    print("\nOptions:")
    print("  -e, --events <number>       Number of events to send (default: 1000)")
    print("  -b, --batch <number>        Batch size (default: 200)")
    print("  -c, --concurrent <number>   Concurrent batches (default: 5)")
    print("  -h, --help                  Show this help message")
    print("\nExamples:")
    print("  python max_throughout_new.py")
    print("  python max_throughout_new.py -e 5000")
    print("  python max_throughout_new.py --events 10000 --batch 300")
    print("  python max_throughout_new.py -e 2000 -b 100 -c 10")
    print()


# ============================================================================
# MAIN
# ============================================================================
async def main():
    """Main entry point"""
    event_count, batch_size, concurrent_batches = parse_arguments()
    
    print("\n" + "=" * 70)
    print("AZURE EVENT HUB - PATIENT ADMISSION THROUGHPUT TEST")
    print("=" * 70)
    
    start_adt = load_adt_id()
    log("Test Configuration:")
    print(f"  • Target Events: {event_count:,}")
    print(f"  • Batch Size: {batch_size}")
    print(f"  • Concurrent Batches: {concurrent_batches}")
    print(f"  • Starting ADT ID: {start_adt:,}")
    
    tester = EventHubTester(CONNECTION_STR, EVENTHUB_NAME, start_adt)
    
    log("Sending events to Event Hub...")
    start_time = time.time()
    
    try:
        test_time = await tester.run_test(event_count, batch_size, concurrent_batches)
        
        throughput = tester.sent_count / test_time
        total_time = time.time() - start_time
        
        print("\n" + "=" * 70)
        print("TEST RESULTS")
        print("=" * 70)
        log("Test Completed")
        print(f"✓ Events Sent: {tester.sent_count:,}")
        print(f"✓ Throughput: {throughput:.1f} events/second")
        print(f"✓ Test Duration: {test_time:.2f} seconds")
        print(f"✓ ADT ID Range: {tester.initial_adt:,} to {tester.adt_id - 1:,}")
        
        if tester.samples:
            print(f"\nSample Patient Records (Verification):")
            for i, sample in enumerate(tester.samples[:3], 1):
                print(f"  {i}. {sample['name']:<25} | Contact: {sample['contact']} | ADT ID: {sample['adt_id']}")
        
        save_adt_id(tester.adt_id - 1)
        
        print("\n" + "=" * 70)
        log("Script completed successfully")
        print("=" * 70 + "\n")
        
    except Exception as e:
        log(f"Error: {e}")
    finally:
        await tester.close()


if __name__ == "__main__":
    asyncio.run(main())
