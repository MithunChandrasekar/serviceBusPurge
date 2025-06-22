from concurrent.futures import ThreadPoolExecutor
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
from azure.mgmt.servicebus import ServiceBusManagementClient
from azure.identity import DefaultAzureCredential
import threading

# Configuration
CONNECTION_STR = ""
SUBSCRIPTION_ID = ""
RESOURCE_GROUP = ""
NAMESPACE_NAME = ""

# Thread-safe dictionary to store results
queue_report = {}
report_lock = threading.Lock()

# Initialize ServiceBusClient using connection string
servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)

# Azure Management Client still needs Azure AD credential (only for listing queues)
credential = DefaultAzureCredential()
mgmt_client = ServiceBusManagementClient(credential, SUBSCRIPTION_ID)

# Function to purge empty-body messages from a given queue
def purge_empty_messages(queue_name):
    print(f"[{queue_name}] Scanning...")

    # Get total messages before purging
    queue_details = mgmt_client.queues.get(RESOURCE_GROUP, NAMESPACE_NAME, queue_name)
    total_messages = queue_details.count_details.active_message_count

    deleted_count = 0

    with servicebus_client.get_queue_receiver(
        queue_name=queue_name,
        receive_mode=ServiceBusReceiveMode.PEEK_LOCK
    ) as receiver:
        for msg in receiver.receive_messages(max_message_count=50, max_wait_time=5):
            try:
                body = str(msg)
                if len(body.strip()) == 0:
                    receiver.complete_message(msg)
                    deleted_count += 1
                else:
                    receiver.abandon_message(msg)
            except Exception as e:
                print(f"[{queue_name}] Error: {e}")

    # Update thread-safe report
    with report_lock:
        queue_report[queue_name] = {
            "total_messages_before": total_messages,
            "deleted_empty_messages": deleted_count
        }

    print(f"[{queue_name}] Done. Deleted {deleted_count} empty messages.")

# Main function to scan all queues with limited parallel threads
def process_queues(max_threads=4):
    queues = mgmt_client.queues.list_by_namespace(RESOURCE_GROUP, NAMESPACE_NAME)
    queue_names = [q.name for q in queues]

    print(f"\n Processing {len(queue_names)} queues with up to {max_threads} threads...\n")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        executor.map(purge_empty_messages, queue_names)

    print("\n Final Summary Report:")
    print("----------------------------------------------------")
    for queue_name, stats in queue_report.items():
        print(f"  Queue: {queue_name}")
        print(f"  Total messages before:  {stats['total_messages_before']}")
        print(f"  Empty messages deleted: {stats['deleted_empty_messages']}\n")

# Entry point
if __name__ == "__main__":
    process_queues(max_threads=4)
