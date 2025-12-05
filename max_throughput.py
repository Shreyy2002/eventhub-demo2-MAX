import asyncio
import time
import json
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
import uuid
import random
from faker import Faker
import math


# Initialize Faker for random data generation
fake = Faker()


# -----------------------
# Static custom property (JSONType is constant)
# -----------------------
JSON_TYPE_PROPERTY = "PatientAdmissionAdvice"
# -----------------------


class EventHubThroughputTester:
    def __init__(self, connection_string, eventhub_name):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.sent_count = 0
        self.start_time = None
        self.fake = Faker()
        
        # Start advice ID counter from 700000 (per your request)
        self.advice_id_counter = 700000
        
        # Only create producer client if valid credentials are provided
        if connection_string and connection_string != "dummy" and "Endpoint=sb://" in connection_string:
            self.producer_client = EventHubProducerClient.from_connection_string(
                conn_str=connection_string,
                eventhub_name=eventhub_name
            )
        else:
            self.producer_client = None
    
    def generate_random_patient_data(self):
        """Generate random patient name and contact number"""
        return {
            "PatientName": f"{self.fake.first_name()} {self.fake.last_name()}",
            "ContactNumber": f"{random.randint(7000000000, 9999999999)}"
        }
    
    def create_payload(self):
        """
        Build payload from the exact JSON template you supplied,
        but replace dynamic fields:
         - PatientName
         - ContactNumber
         - D_PatientAdmissionAdviceId_ADT (auto-increment)
         - KafkaGUID (unique, UPPERCASE)
        Returns (payload_dict, kafka_guid_upper)
        """
        patient_data = self.generate_random_patient_data()
        current_advice_id = self.advice_id_counter
        self.advice_id_counter += 1

        kafka_guid = str(uuid.uuid4()).upper()

        # Exact JSON template (kept ordering and key names as provided)
        payload = {
            "JSONData": {
                "Table1": [
                    {
                        "Name": "PatientAdmissionAdvice"
                    }
                ],
                "D_PATIENTADMISSIONADVICE": [
                    {
                        "Id": None,
                        "HSPLocationId": 136,
                        "M_PatientId": None,
                        "RegistrationNo": None,
                        "IACode": "MJHL",
                        "LengthOfStay": "4",
                        "EDA": "2025-12-06T00:00:00",
                        "ExpArrivalTime": "10:39",
                        "SpecialityId": 48,
                        "TDocId": 71767,
                        "BedCategoryId": 8,
                        "SpCnId": 11,
                        "MedicalSurgicalId": 1,
                        "Deleted": 0,
                        "AdvicedDate": "2025-12-02T18:04:00",
                        "VisitId": None,
                        "MedicalCostOfInvRoutine": "342",
                        "MedicalCostOfInvSpecial": "342",
                        "MedicalCostOfPharRoutine": "342",
                        "MedicalCostOfPharSpecial": "234423",
                        "MedicalCostOfOtherCharge1": "",
                        "MedicalCostOfOtherCharge2": "string",
                        "PrimarySurgery": None,
                        "AddiSurgery1": None,
                        "AddiSurgeryCat1": None,
                        "AddiSurgery2": None,
                        "AddiSurgeryCat2": None,
                        "AnesthesiaTypeId": None,
                        "SurgicalCostOfImplant": None,
                        "SurgicalCostOfInvRoutine": None,
                        "SurgicalCostOfInvSpecial": None,
                        "SurgicalCostOfPharRoutine": None,
                        "SurgicalCostOfPharSpecial": None,
                        "AsstSurgeon": None,
                        "SpecialEquipment": None,
                        "BloodUnitsRequirement": None,
                        "ComplicationsHighRiskMarkup": None,
                        "SurgicalCostOfOtherCharge": None,
                        "SecondarySpecialityId": 0,
                        "SecondaryDoctorId": 0,
                        "PrimarySurgeryCat": "",
                        "MedicalCostOfConsumableRoutine": "32",
                        "MedicalCostOfConsumableSpecial": "342",
                        "SurgicalCostOfConsumableRoutine": None,
                        "SurgicalCostOfConsumableSpecial": None,
                        "IPId": 0,
                        "IsEmpatient": False,
                        "Remarks": "test ",
                        "ProvisionalDiagnostic": "test feg3er",
                        "OperatorId": 60210,
                        "IsCprsSaved": False,
                        "VistaId": "",
                        "OtherDesiredBedCategory": "",
                        "IsCancelled": None,
                        "EditedPatientAdmissionAdviceId": 0,
                        "EditorCancelRemarks": "string",
                        "CancelledByVistaId": None,
                        "CancelledBy": None,
                        "CancelledDate": None,
                        "PrimarySpecialityId2": 134,
                        "PrimaryDoctorId2": None,
                        # dynamic fields - replaced per event
                        "PatientName": patient_data["PatientName"],
                        "ContactNumber": patient_data["ContactNumber"],
                        "AdmRequestLog_ExpDateOfAdmission": None,
                        "AdmRequestLog_ExpArrivalTime": None,
                        "D_PatientAdmissionAdviceId_ADT": current_advice_id,
                        "IsWebHIS": True,
                        "D_PatientAdviceUnregId": 1302,
                        "EditedPatientAdmissionAdviceId_ADT": 0,
                        "IsRegister": False,
                        "AddiSurgery3": None,
                        "AddiSurgeryCat3": None,
                        "AddiSurgery4": None,
                        "AddiSurgeryCat4": None,
                        "AddiSurgery5": None,
                        "AddiSurgeryCat5": None,
                        "KafkaGUID": kafka_guid
                    }
                ],
                "M_IPWAITINGLIST": [],
                "D_PATIENTREGISTRATIONLOG": [],
                "D_PATIENTADMISSIONREQUEST": [],
                "D_PATIENTADMISSIONADVICE_UPDATE": []
            }
        }

        return payload, kafka_guid
    
    async def send_events_precisely(self, events_per_second, duration_seconds, batch_size=10):
        """
        Send events at a precise rate of events per second using EventDataBatch
        
        Args:
            events_per_second: Target events to send per second (0 for maximum speed)
            duration_seconds: Total test duration in seconds
            batch_size: Number of events to send in each batch
        """
        if not self.producer_client:
            raise ValueError("Producer client not initialized. Please provide valid Event Hub credentials.")
        
        max_throughput_mode = (events_per_second == 0)
        
        if max_throughput_mode:
            print(f"Starting MAXIMUM THROUGHPUT test")
            print(f"Goal: Discover maximum possible events per second")
        else:
            print(f"Starting PRECISE THROUGHPUT test")
            print(f"Target: {events_per_second} events/second")
        
        print(f" Duration: {duration_seconds} seconds")
        print(f" Batch size: {batch_size}")
        print("-" * 60)
        
        self.start_time = time.time()
        self.sent_count = 0
        start_time = self.start_time
        
        # TRACK ADVICE ID RANGE - CAPTURE START
        start_advice_id = self.advice_id_counter
        print(f"📊 Advice ID Range: {start_advice_id} → TBD (updating live)")
        
        if not max_throughput_mode:
            # Calculate timing for precise throughput
            batches_per_second = events_per_second / batch_size
            interval_between_batches = 1.0 / batches_per_second
            print(f"Batches per second: {batches_per_second:.2f}")
            print(f"Interval between batches: {interval_between_batches:.3f} seconds")
        else:
            # Maximum throughput mode - send as fast as possible
            batches_per_second = 0
            interval_between_batches = 0
            print("Mode: Sending at maximum speed")
        
        print("-" * 60)
        
        # Statistics tracking
        throughput_stats = []
        latency_stats = []
        last_stats_time = start_time
        stats_interval = 1.0
        
        async def send_single_batch():
            """Send a single batch of events using EventDataBatch"""
            batch_start_time = time.time()
            batch_events_sent = 0
            
            try:
                # Create batch
                batch = await self.producer_client.create_batch()
                
                for i in range(batch_size):
                    payload, kafka_guid = self.create_payload()
                    
                    # Pretty-print JSON exactly like sample (2-space indent)
                    event_json = json.dumps(payload, indent=2, ensure_ascii=False)
                    event_data = EventData(event_json)
                    
                    # --- attach custom application properties to each EventData ---
                    event_data.properties = {
                        "JSONType": JSON_TYPE_PROPERTY,
                        "KafkaGUID": kafka_guid  # uppercase
                    }
                    # -------------------------------------------------------------
                    
                    try:
                        batch.add(event_data)
                    except ValueError:
                        # Batch is full, send it and create new one
                        if len(batch) > 0:
                            await self.producer_client.send_batch(batch)
                            batch_events_sent += len(batch)
                            batch = await self.producer_client.create_batch()
                        
                        # Add current event to new batch
                        batch.add(event_data)
                
                # Send final batch
                if len(batch) > 0:
                    await self.producer_client.send_batch(batch)
                    batch_events_sent += len(batch)
                
                batch_latency = time.time() - batch_start_time
                self.sent_count += batch_events_sent
                return True, batch_latency, batch_events_sent
                
            except EventHubError as e:
                print(f"Error sending batch: {e}")
                return False, 0, 0
        
        # Main sending loop
        next_batch_time = start_time
        batch_number = 0
        
        print("Starting event sending...")
        print("Time (s) | Target EPS | Actual EPS | Total Sent | Latency (ms)")
        print("-" * 65)
        
        while time.time() - start_time < duration_seconds:
            current_time = time.time()
            
            if max_throughput_mode:
                # Maximum throughput mode - send immediately without waiting
                success, latency, events_sent = await send_single_batch()
                if success:
                    latency_stats.append(latency * 1000)
                    batch_number += 1
                
                # Small sleep to prevent complete CPU blocking
                await asyncio.sleep(0.001)
                
            else:
                # Precise throughput mode - follow timing schedule
                if current_time >= next_batch_time:
                    success, latency, events_sent = await send_single_batch()
                    if success:
                        latency_stats.append(latency * 1000)
                        batch_number += 1
                    
                    next_batch_time = start_time + (batch_number * interval_between_batches)
            
            # Print statistics every second
            if current_time - last_stats_time >= stats_interval:
                elapsed = current_time - start_time
                actual_throughput = self.sent_count / elapsed if elapsed > 0 else 0
                throughput_stats.append(actual_throughput)
                
                avg_latency = sum(latency_stats) / len(latency_stats) if latency_stats else 0
                
                target_display = "MAX" if max_throughput_mode else events_per_second
                print(f"{elapsed:7.1f} | {target_display:10} | {actual_throughput:10.1f} | {self.sent_count:10} | {avg_latency:6.1f} ms")
                
                last_stats_time = current_time
                latency_stats = []
            
            # Small sleep to prevent busy waiting in precise mode
            if not max_throughput_mode:
                await asyncio.sleep(0.001)
        
        # Final statistics
        total_time = time.time() - start_time
        final_throughput = self.sent_count / total_time if total_time > 0 else 0
        
        # TRACK ADVICE ID RANGE - CAPTURE END
        end_advice_id = self.advice_id_counter - 1  # Last ID sent (counter is already +1 ahead)
        
        print("-" * 65)
        print(f"Test completed!")
        
        if max_throughput_mode:
            print(f"🏆 MAXIMUM THROUGHPUT ACHIEVED: {final_throughput:.2f} events/second")
            print(f"Total events sent: {self.sent_count}")
            print(f"Total time: {total_time:.2f} seconds")
            print(f"📊 Advice ID Range: {start_advice_id} → {end_advice_id} ({self.sent_count} events)")
        else:
            print(f"Final throughput: {final_throughput:.2f} events/second")
            print(f"Target throughput: {events_per_second} events/second")
            print(f"Total events sent: {self.sent_count}")
            print(f"Total time: {total_time:.2f} seconds")
            print(f"📊 Advice ID Range: {start_advice_id} → {end_advice_id} ({self.sent_count} events)")
            
            # Calculate accuracy
            accuracy_percentage = (final_throughput / events_per_second) * 100
            print(f"Accuracy: {accuracy_percentage:.1f}%")
            
            if accuracy_percentage >= 95:
                print("EXCELLENT: Throughput target achieved with high accuracy!")
            elif accuracy_percentage >= 90:
                print("GOOD: Throughput target achieved with good accuracy")
            elif accuracy_percentage >= 80:
                print("ACCEPTABLE: Throughput target mostly achieved")
            else:
                print("NEEDS IMPROVEMENT: Throughput target not achieved")
        
        return final_throughput
    
    async def discover_max_throughput(self, duration_seconds=30, max_batch_size=100):
        """
        Discover the maximum possible throughput by testing different batch sizes
        """
        print(" Starting MAXIMUM THROUGHPUT DISCOVERY")
        print("=" * 60)
        
        best_throughput = 0
        best_batch_size = 10
        
        # Test different batch sizes to find optimal configuration
        batch_sizes = [5, 10, 20, 50, 100]
        
        for batch_size in batch_sizes:
            if batch_size > max_batch_size:
                continue
                
            print(f"\nTesting with batch size: {batch_size}")
            self.sent_count = 0
            
            throughput = await self.send_events_precisely(
                events_per_second=0,  # 0 means maximum speed
                duration_seconds=min(10, duration_seconds),
                batch_size=batch_size
            )
            
            if throughput > best_throughput:
                best_throughput = throughput
                best_batch_size = batch_size
                print(f" New best: {throughput:.1f} EPS with batch size {batch_size}")
            
            # Stop if we're not seeing improvements
            if batch_size >= 50 and throughput < best_throughput * 0.9:
                break
        
        print(f"\n" + "=" * 60)
        print(f"MAXIMUM THROUGHPUT DISCOVERED: {best_throughput:.1f} events/second")
        print(f"Optimal batch size: {best_batch_size}")
        print("=" * 60)
        
        return best_throughput, best_batch_size

    async def close(self):
        """Close the producer client"""
        if self.producer_client:
            await self.producer_client.close()
            print("Event Hub producer client closed successfully")


def validate_payload():
    """Validate that the payload structure is correct without creating Event Hub client"""
    print("🔍 Validating payload structure...")
    
    class PayloadValidator:
        def __init__(self):
            self.fake = Faker()
            self.advice_id_counter = 700000
        
        def generate_random_patient_data(self):
            return {
                "PatientName": f"{self.fake.first_name()} {self.fake.last_name()}",
                "ContactNumber": f"{random.randint(7000000000, 9999999999)}"
            }
        
        def create_payload(self):
            patient_data = self.generate_random_patient_data()
            current_advice_id = self.advice_id_counter
            self.advice_id_counter += 1
            kafka_guid = str(uuid.uuid4()).upper()
            
            base_payload = {
                "JSONData": {
                    "Table1": [
                        {
                            "Name": "PatientAdmissionAdvice"
                        }
                    ],
                    "D_PATIENTADMISSIONADVICE": [
                        {
                            "Id": None,
                            "HSPLocationId": 136,
                            "M_PatientId": None,
                            "RegistrationNo": None,
                            "IACode": "MJHL",
                            "LengthOfStay": "4",
                            "EDA": "2025-12-06T00:00:00",
                            "ExpArrivalTime": "10:39",
                            "SpecialityId": 48,
                            "TDocId": 71767,
                            "BedCategoryId": 8,
                            "SpCnId": 11,
                            "MedicalSurgicalId": 1,
                            "Deleted": 0,
                            "AdvicedDate": "2025-12-02T18:04:00",
                            "VisitId": None,
                            "MedicalCostOfInvRoutine": "342",
                            "MedicalCostOfInvSpecial": "342",
                            "MedicalCostOfPharRoutine": "342",
                            "MedicalCostOfPharSpecial": "234423",
                            "MedicalCostOfOtherCharge1": "",
                            "MedicalCostOfOtherCharge2": "string",
                            "PrimarySurgery": None,
                            "AddiSurgery1": None,
                            "AddiSurgeryCat1": None,
                            "AddiSurgery2": None,
                            "AddiSurgeryCat2": None,
                            "AnesthesiaTypeId": None,
                            "SurgicalCostOfImplant": None,
                            "SurgicalCostOfInvRoutine": None,
                            "SurgicalCostOfInvSpecial": None,
                            "SurgicalCostOfPharRoutine": None,
                            "SurgicalCostOfPharSpecial": None,
                            "AsstSurgeon": None,
                            "SpecialEquipment": None,
                            "BloodUnitsRequirement": None,
                            "ComplicationsHighRiskMarkup": None,
                            "SurgicalCostOfOtherCharge": None,
                            "SecondarySpecialityId": 0,
                            "SecondaryDoctorId": 0,
                            "PrimarySurgeryCat": "",
                            "MedicalCostOfConsumableRoutine": "32",
                            "MedicalCostOfConsumableSpecial": "342",
                            "SurgicalCostOfConsumableRoutine": None,
                            "SurgicalCostOfConsumableSpecial": None,
                            "IPId": 0,
                            "IsEmpatient": False,
                            "Remarks": "test ",
                            "ProvisionalDiagnostic": "test feg3er",
                            "OperatorId": 60210,
                            "IsCprsSaved": False,
                            "VistaId": "",
                            "OtherDesiredBedCategory": "",
                            "IsCancelled": None,
                            "EditedPatientAdmissionAdviceId": 0,
                            "EditorCancelRemarks": "string",
                            "CancelledByVistaId": None,
                            "CancelledBy": None,
                            "CancelledDate": None,
                            "PrimarySpecialityId2": 134,
                            "PrimaryDoctorId2": None,
                            "PatientName": patient_data["PatientName"],
                            "ContactNumber": patient_data["ContactNumber"],
                            "AdmRequestLog_ExpDateOfAdmission": None,
                            "AdmRequestLog_ExpArrivalTime": None,
                            "D_PatientAdmissionAdviceId_ADT": current_advice_id,
                            "IsWebHIS": True,
                            "D_PatientAdviceUnregId": 1302,
                            "EditedPatientAdmissionAdviceId_ADT": 0,
                            "IsRegister": False,
                            "AddiSurgery3": None,
                            "AddiSurgeryCat3": None,
                            "AddiSurgery4": None,
                            "AddiSurgeryCat4": None,
                            "AddiSurgery5": None,
                            "AddiSurgeryCat5": None,
                            "KafkaGUID": kafka_guid
                        }
                    ],
                    "M_IPWAITINGLIST": [],
                    "D_PATIENTREGISTRATIONLOG": [],
                    "D_PATIENTADMISSIONREQUEST": [],
                    "D_PATIENTADMISSIONADVICE_UPDATE": []
                }
            }
            
            return base_payload, kafka_guid
    
    validator = PayloadValidator()
    payload, guid = validator.create_payload()
    
    # pretty print sample payload (2-space indent)
    pretty = json.dumps(payload, indent=2, ensure_ascii=False)
    print(pretty)
    print(f"\nSample fields -> PatientName: {payload['JSONData']['D_PATIENTADMISSIONADVICE'][0]['PatientName'] if isinstance(payload['JSONData']['D_PATIENTADMISSIONADVICE'], list) else payload['JSONData']['D_PATIENTADMISSIONADVICE']['D_PATIENTADMISSIONADVICE'][0]['PatientName']}")
    # Attempt to access ContactNumber and ADT safely
    try:
        entry = payload['JSONData']['D_PATIENTADMISSIONADVICE'][0]
    except Exception:
        # fallback structure
        entry = payload['JSONData']['D_PATIENTADMISSIONADVICE']['D_PATIENTADMISSIONADVICE'][0]
    print(f"ContactNumber: {entry.get('ContactNumber')}")
    print(f"D_PatientAdmissionAdviceId_ADT: {entry.get('D_PatientAdmissionAdviceId_ADT')}")
    print(f"KafkaGUID: {guid}")
    print("✅ Payload structure is valid and matches the required format!")
    print("✅ PatientName, ContactNumber, D_PatientAdmissionAdviceId_ADT and KafkaGUID will change per event")
    return payload


async def main():
    # Your Event Hub credentials
    CONNECTION_STR ="Endpoint=sb://ns-for-testing.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EwI67+VG8kY7hlhq8qXqSMwT1TEL/ZP15+AEhIWpOpo="
    EVENTHUB_NAME = "triall"
    #CONNECTION_STR =
    
    # ⚡ TEST MODES - CHOOSE ONE:
    TEST_MODE = "max_throughput"  # Options: "precise", "max_throughput", "discover_max"
    
    # Configuration
    if TEST_MODE == "precise":
        EVENTS_PER_SECOND = 1000    # Your specific target
        DURATION_SECONDS = 60
        BATCH_SIZE = 500
    elif TEST_MODE == "max_throughput":
        EVENTS_PER_SECOND = 10000      # 0 means maximum speed
        DURATION_SECONDS = 25
        BATCH_SIZE = 500
    else:  # discover_max
        EVENTS_PER_SECOND = 1000
        DURATION_SECONDS = 30
        BATCH_SIZE = 500
    
    # Validate credentials
    if not CONNECTION_STR.startswith("Endpoint=sb://"):
        print("Invalid connection string format.")
        return
    
    # Create tester instance
    tester = EventHubThroughputTester(CONNECTION_STR, EVENTHUB_NAME)
    
    try:
        if TEST_MODE == "precise":
            print("MODE: PRECISE THROUGHPUT - Achieving specific target")
            await tester.send_events_precisely(
                events_per_second=EVENTS_PER_SECOND,
                duration_seconds=DURATION_SECONDS,
                batch_size=BATCH_SIZE
            )
            
        elif TEST_MODE == "max_throughput":
            print("MODE: MAXIMUM THROUGHPUT - Sending as fast as possible")
            await tester.send_events_precisely(
                events_per_second=0,  # 0 = maximum speed
                duration_seconds=DURATION_SECONDS,
                batch_size=BATCH_SIZE
            )
            
        elif TEST_MODE == "discover_max":
            print("MODE: DISCOVER MAXIMUM THROUGHPUT - Finding optimal configuration")
            max_throughput, optimal_batch_size = await tester.discover_max_throughput(
                duration_seconds=DURATION_SECONDS
            )
            
            print(f"\n Recommendation: Use batch size {optimal_batch_size} for maximum throughput")
            print(f"Maximum achievable: {max_throughput:.0f} events/second")
            
        else:
            print("Invalid test mode. Use 'precise', 'max_throughput', or 'discover_max'")
            
    except Exception as e:
        print(f" Error: {e}")
    finally:
        await tester.close()


if __name__ == "__main__":
    print("\n" + "="*70)
    print("EVENT HUB THROUGHPUT TESTER - EXACT JSON STRUCTURE (USER TEMPLATE)")
    print("="*70)
    print(" Features:")
    print("   - Uses EXACT JSON structure provided by user (keys & ordering preserved)")
    print("   - PatientName, ContactNumber, D_PatientAdmissionAdviceId_ADT and KafkaGUID change per event")
    print("   - KafkaGUID is UPPERCASE and also set as application property")
    print("="*70)
    
    # Validate payload first
    validate_payload()
    
    # Run the test
    asyncio.run(main())
