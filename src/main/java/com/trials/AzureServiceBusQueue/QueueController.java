package com.trials.AzureServiceBusQueue;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusMessageBatch;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class QueueController {

    private final String connectionString;
    private final String queueName;
    private final QueueMessageProcessor queueMessageProcessor;
    private final QueueMessageReceiver queueMessageReceiver;

    @Autowired
    public QueueController(@Value("${aazure.servicebus.queue.connectionString}") String connectionString,
                           @Value("${aazure.servicebus.queue.name}") String queueName,
                           QueueMessageProcessor queueMessageProcessor, QueueMessageReceiver queueMessageReceiver) {
        this.connectionString = connectionString;
        this.queueName = queueName;
        this.queueMessageProcessor = queueMessageProcessor;
        this.queueMessageReceiver = queueMessageReceiver;
    }

    @GetMapping("/send/{count}")
    public void sendMessage(@PathVariable("count") Integer count) {
        // create a Service Bus Sender client for the queue
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName(queueName)
                .buildClient();
        String identifier = new Date().toString();

        ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();
        for(int i =0; i<count; i++) {
            String messageBody = i + " : Message sent at " + identifier;
            messageBatch.tryAddMessage(new ServiceBusMessage(messageBody));
        }
        senderClient.sendMessages(messageBatch);
    }


    @GetMapping("/messageProcessor/start")
    public void startMessageProcessing() {
        queueMessageProcessor.startProcessor();
    }

    @GetMapping("/messageProcessor/stop")
    public void stopMessageProcessing() {
        queueMessageProcessor.stopProcessor();
    }

    @GetMapping("/messageReceiver/start")
    public void startMessageReceiving() {
        queueMessageReceiver.startReceivingMessages();
    }

    @GetMapping("/messageReceiver/stop")
    public void stopMessageReceiving() {
        queueMessageReceiver.stopReceivingMessages();
    }


}
