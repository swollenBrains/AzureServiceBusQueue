package com.trials.AzureServiceBusQueue;

import com.azure.messaging.servicebus.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class QueueMessageHandler {

    Logger LOGGER = LoggerFactory.getLogger(QueueMessageHandler.class);

    public void processMessage(ServiceBusReceivedMessageContext serviceBusReceivedMessageContext) {
        try {
            ServiceBusReceivedMessage message = serviceBusReceivedMessageContext.getMessage();
            LOGGER.info("apple Processing message (lock: {}). Sequence: {}, DeliveryCount: {}, Session: {}, Contents: {}",
                    message.getLockToken(), message.getSequenceNumber(), message.getDeliveryCount(), message.getMessageId(),
                    message.getBody());
//            if (message.getSequenceNumber() % 2 == 0) {
//                throw new RuntimeException("Dummy exception for sequence : " + message.getSequenceNumber() + ", message: " + message.getBody());
//            }
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException e) {
                LOGGER.error("Error processing msg : {}", message.getBody(), e);
            }
            LOGGER.info("apple Processed message. Sequence: {}, Session: {}, Contents: {}", message.getSequenceNumber(),
                    message.getMessageId(), message.getBody());
            serviceBusReceivedMessageContext.complete();
        } catch(Throwable throwable) {
            serviceBusReceivedMessageContext.abandon();
        }
    }

    public void processError(ServiceBusErrorContext context) {
        LOGGER.warn("Error when receiving messages from namespace: '{}'. Entity: '{}'",
                context.getFullyQualifiedNamespace(), context.getEntityPath());

        if (!(context.getException() instanceof ServiceBusException)) {
            LOGGER.error("Non-ServiceBusException occurred: ", context.getException());
            if (context.getException() != null) {
                context.getException().printStackTrace();
            }
            return;
        }

        ServiceBusException exception = (ServiceBusException) context.getException();
        ServiceBusFailureReason reason = exception.getReason();

        if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
                || reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
                || reason == ServiceBusFailureReason.UNAUTHORIZED) {
            LOGGER.error("An unrecoverable error occurred. Stopping processing with reason {} : {}",
                    reason, exception.getMessage(), exception);

//            countdownLatch.countDown();
        } else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
            LOGGER.error("Message lock lost for message", context.getException());
        } else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
            try {
                // Choosing an arbitrary amount of time to wait until trying again.
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOGGER.error("Unable to sleep for period of time");
            }
        } else {
            LOGGER.error("Error source : {}, reason : {}, message: {}", context.getErrorSource(),
                    reason, context.getException(), context.getException());
//            throw new RuntimeException(context.getException());
        }
    }

}

