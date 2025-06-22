from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.mgmt.servicebus import ServiceBusManagementClient
from azure.mgmt.servicebus.models import SBQueue
from azure.identity import DefaultAzureCredential

# Configuration
CONNECTION_STR = ""
SUBSCRIPTION_ID = ""
RESOURCE_GROUP = ""
NAMESPACE_NAME = ""

# List of queues to create and populate
QUEUE_NAMES = ["test-queue-1", "test-queue-2"]

# Step 1: Create queues (if not exist)
def create_queues():
    credential = DefaultAzureCredential()
    mgmt_client = ServiceBusManagementClient(credential, SUBSCRIPTION_ID)

    for qname in QUEUE_NAMES:
        try:
            print(f"Creating queue: {qname}")
            mgmt_client.queues.create_or_update(
                RESOURCE_GROUP,
                NAMESPACE_NAME,
                qname,
                parameters=SBQueue(enable_partitioning=False)
            )
        except Exception as e:
            if "Conflict" in str(e):
                print(f"Queue '{qname}' already exists. Skipping.")
            else:
                print(f"Error creating queue '{qname}': {e}")

# Step 2: Send messages (including some with empty bodies)
def send_messages():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)
    
    with servicebus_client:
        for qname in QUEUE_NAMES:
            sender = servicebus_client.get_queue_sender(queue_name=qname)
            with sender:
                print(f"Sending messages to {qname}...")
                # Send some valid messages
                sender.send_messages(ServiceBusMessage("Hello from " + qname))
                sender.send_messages(ServiceBusMessage("Another message"))
                sender.send_messages(ServiceBusMessage("    "))  # Spaces (should be purged if stripped)
                sender.send_messages(ServiceBusMessage(""))       # Empty message
                sender.send_messages(ServiceBusMessage("Last message"))

                print(f"Sent 5 messages to {qname}")

# Main execution
if __name__ == "__main__":
    create_queues()
    send_messages()
