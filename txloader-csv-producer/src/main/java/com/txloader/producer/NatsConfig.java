package com.txloader.producer;

public class NatsConfig {

    private final String serverUrl;
    private final String subject;
    private final String streamName;

    public NatsConfig(String serverUrl, String subject, String streamName) {
        this.serverUrl = serverUrl;
        this.subject = subject;
        this.streamName = streamName;
    }

    public String getServerUrl() { return serverUrl; }
    public String getSubject() { return subject; }
    public String getStreamName() { return streamName; }
}
