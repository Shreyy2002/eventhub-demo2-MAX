
---

# **Bulk Consumer – Azure Event Hub**

---

##  **Prerequisites**

Ensure you have the following:

### **1. Python Version**

* Python **3.8+**

### **2. Azure Resources**

* Azure **Event Hub Namespace**
* Event Hub instance
* Azure Storage Account with:

  * Blob Container for checkpoints
  * Access Keys

### **3. Environment Variables / Update Config**

Update the script with:

* `EVENTHUB_CONNECTION_STR`
* `EVENTHUB_NAME`
* `STORAGE_CONNECTION_STR`
* `CHECKPOINT_CONTAINER`

---

##  **Dependencies**

Install required packages:

```bash
pip install azure-eventhub azure-eventhub-checkpointstoreblob-aio
```

---

##  **How to Run the Consumer**

### **Basic Usage**

```bash
python batch_consumer_patient_admission.py <consumer_group>
```

### **With Custom Batch Size and Wait Time**

```bash
python batch_consumer_patient_admission.py <consumer_group> <batch_size> <max_wait_time>
```

### **Example**

```bash
python batch_consumer_patient_admission.py cgfortest 50 10
```

* `cgfortest` → consumer group name
* `50` → processes 50 events per batch
* `10` → waits up to 10 seconds for batch filling

---

## **References**
Microsoft Doc- https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-event-hubs?tabs=isolated-process%2Cextensionv6&pivots=programming-language-python
Collapse
