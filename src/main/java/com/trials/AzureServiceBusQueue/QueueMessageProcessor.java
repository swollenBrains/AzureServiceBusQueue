package com.trials.AzureServiceBusQueue;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

@Component
public class QueueMessageProcessor {

    private final ServiceBusProcessorClient processorClient;

    private final String connectionString;
    private final String queueName;
    private final QueueMessageHandler queueMessageHandler;

    public QueueMessageProcessor(@Value("${aazure.servicebus.queue.connectionString}") String connectionString,
                                 @Value("${aazure.servicebus.queue.name}") String queueName, QueueMessageHandler queueMessageHandler) {
        this.connectionString = connectionString;
        this.queueName = queueName;
        this.queueMessageHandler = queueMessageHandler;
        CountDownLatch countdownLatch = new CountDownLatch(1);

        AmqpRetryOptions opt = new AmqpRetryOptions();
        opt.setMaxRetries(1);
//        opt.setMode(AmqpRetryMode.FIXED);
//        opt.setDelay(Duration.ofMillis(800));
//        opt.setTryTimeout(Duration.ofMinutes(1));
//        opt.setMaxDelay(Duration.ofMinutes(1));

        this.processorClient = new ServiceBusClientBuilder()
                .connectionString(this.connectionString)
//                .retryOptions(opt)
//                .sessionProcessor()
                .processor()
                .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                .prefetchCount(0)
                .disableAutoComplete()
                .queueName(this.queueName)
                .maxConcurrentCalls(1)
                .maxAutoLockRenewDuration(Duration.ofSeconds(40_000))
                .processMessage(this.queueMessageHandler::processMessage)
                .processError(context -> this.queueMessageHandler.processError(context))
                .buildProcessorClient();
    }

    public void startProcessor() {
        this.processorClient.start();
    }

    public void stopProcessor() {
        this.processorClient.close();
    }

}
