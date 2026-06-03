package com.txloader.processor;

import java.io.Serializable;

public class NatsConfig implements Serializable {

    private final String serverUrl;
    private final String subject;
    private final String streamName;
    private final String consumerName;

    public NatsConfig(String serverUrl, String subject, String streamName, String consumerName) {
        this.serverUrl = serverUrl;
        this.subject = subject;
        this.streamName = streamName;
        this.consumerName = consumerName;
    }

    public String getServerUrl() { return serverUrl; }
    public String getSubject() { return subject; }
    public String getStreamName() { return streamName; }
    public String getConsumerName() { return consumerName; }
}
