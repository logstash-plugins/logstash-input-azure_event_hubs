package com.microsoft.azure.eventprocessorhost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class ErrorNotificationHandler implements Consumer<ExceptionReceivedEventArgs> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorNotificationHandler.class);

    @Override
    public void accept(ExceptionReceivedEventArgs exceptionReceivedEventArgs) {
        LOGGER.error("Host {} received general error notification during {}", exceptionReceivedEventArgs.getHostname(), exceptionReceivedEventArgs.getAction(),
                exceptionReceivedEventArgs.getException());
    }
}
