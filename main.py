# import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage

CONNECTION_STR = "<NAMESPACE CONNECTION STRING>"
QUEUE_NAME = "<QUEUE NAME>"

class AzureServiceBus():

    def __init__(self):

        self.sender = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)
        self.receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5)

    def send_single(self, message):

        M = ServiceBusMessage(message)
        self.sender.send_messages(M)
        print("Sent a single message")

    def send_list(self, messages_list):

        self.sender.send_messages(messages_list)
        print("Sent a list of %s messages" % len(messages_list))

    def send_batch(self, messages_list):
        batch_message = self.sender.create_message_batch()

        for message in messages_list:
            try:
                batch_message.add_message(ServiceBusMessage(message))
            except ValueError:
                # ServiceBusMessageBatch object reaches max_size.
                # New ServiceBusMessageBatch object can be created here to send more data.
                break

        self.sender.send_messages(batch_message)

        print("Sent a batch of %s messages" % len(messages_list))

    def recv_peek(self):
        
        received_msgs = self.receiver.peek_messages(max_message_count=2)

        for msg in received_msgs:
            print(str(msg))

if __name__ == "__main__":
    ASB = AzureServiceBus()

    ASB.send_single("Hello Azure Service Bus, this is a single message")
    ASB.send_list(["Hello", "Azure", "Service", "Bus", ",", "this", "is", "a", "list", "of", "messages"])
    ASB.send_batch(["Hello", "Azure", "Service", "Bus", ",", "this", "is", "a", "batch", "of", "messages"])


    # with servicebus_client:
    #     sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
    #     with sender:
    #         send_single_message(sender)
    #         send_a_list_of_messages(sender)
    #         send_batch_message(sender)

    # print("Done sending messages")
    # print("-----------------------")

    # with servicebus_client:
    #     receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5)
    #     with receiver:
    #         for msg in receiver:
    #             print("Received: " + str(msg))
    #             receiver.complete_message(msg)