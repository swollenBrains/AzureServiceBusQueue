package com.trials.AzureServiceBusQueue;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class QueueMessageReceiver {

    Logger LOGGER = LoggerFactory.getLogger(QueueMessageReceiver.class);
    private final String connectionString;
    private final String queueName;
    private final QueueMessageHandler queueMessageHandler;
    private final ServiceBusReceiverClient serviceBusReceiverClient;
    private final ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> scheduledFuture = null;
    private int maxWaitTimeForMessageInMillis = 1_000;

    public QueueMessageReceiver(@Value("${aazure.servicebus.queue.connectionString}") String connectionString,
                                @Value("${aazure.servicebus.queue.name}") String queueName, QueueMessageHandler queueMessageHandler) {
        this.connectionString = connectionString;
        this.queueName = queueName;
        this.queueMessageHandler = queueMessageHandler;

        this.serviceBusReceiverClient = new ServiceBusClientBuilder().connectionString(connectionString).receiver()
                .queueName(queueName).prefetchCount(0).maxAutoLockRenewDuration(Duration.ofSeconds(240_000)).receiveMode(ServiceBusReceiveMode.PEEK_LOCK).disableAutoComplete()
                .buildClient();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);

    }

    public void startReceivingMessages() {
        if(!isServing()) {
            LOGGER.info("isServing : {}, isMarkedToStopServing: {}", isServing(), isMarkedToStopServing());
            int delayInMillisBeforeFirstTask = 0;
            int delayInMillisBetweenTasks = 200;
            LOGGER.info("Starting to receive messages");
            this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(() -> {
                receiveMessages(1);
            }, delayInMillisBeforeFirstTask, delayInMillisBetweenTasks, TimeUnit.MILLISECONDS);
            LOGGER.info("isServing : {}, isMarkedToStopServing: {}", isServing(), isMarkedToStopServing());
        } else {
            LOGGER.info("QueueMessageReceiver already serving");
        }
    }

    public boolean isServing() {
        return  Objects.nonNull(scheduledFuture) && !scheduledExecutorService.isTerminated();
    }

    public boolean isMarkedToStopServing() {
        return scheduledExecutorService.isShutdown() && !scheduledExecutorService.isTerminated();
    }

    public void stopReceivingMessages() {
        if(isServing()) {
            LOGGER.info("isServing : {}, isMarkedToStopServing: {}", isServing(), isMarkedToStopServing());
            LOGGER.info("Stopping to receive any further messages");
            scheduledExecutorService.shutdown();
            LOGGER.info("isServing : {}, isMarkedToStopServing: {}", isServing(), isMarkedToStopServing());
//            try {
//                boolean terminated = scheduledExecutorService.awaitTermination(3000, TimeUnit.MILLISECONDS);
//            } catch (InterruptedException e) {
//                LOGGER.error("Interrupted awaiting termination", e);
//            } finally {
//                if (serviceBusReceiverClient != null) {
//                    LOGGER.info("Closing receiver client");
//                    serviceBusReceiverClient.close();
//                    LOGGER.info("Closed receiver client");
//                }
//            }
//            LOGGER.info("isServing : {}, isMarkedToStopServing: {}", isServing(), isMarkedToStopServing());
        } else {
            LOGGER.info("Queue Receiver already not serving");
        }

    }

    public void receiveMessages(int maxMessages) {
        LOGGER.info("Checking for messages..");
        serviceBusReceiverClient.receiveMessages(maxMessages, Duration.ofMillis(maxWaitTimeForMessageInMillis))
                .forEach(serviceBusReceivedMessage -> {
                    try {
                        queueMessageHandler.processMessage(serviceBusReceivedMessage);
                        serviceBusReceiverClient.complete(serviceBusReceivedMessage);
                    } catch (Exception exception) {
                        serviceBusReceiverClient.abandon(serviceBusReceivedMessage);
                    }
                });
    }


}
