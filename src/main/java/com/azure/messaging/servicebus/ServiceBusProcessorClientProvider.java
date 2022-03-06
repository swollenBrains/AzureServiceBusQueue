package com.azure.messaging.servicebus;

import reactor.core.scheduler.Schedulers;

public class ServiceBusProcessorClientProvider {

    public static ServiceBusClientBuilder getProcessorClientBuilder() {
        return new ServiceBusClientBuilder().scheduler(Schedulers.immediate());
    }
}
