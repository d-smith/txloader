package com.txloader.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.txloader.model.RawTransaction;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StorageType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TransactionPublisher implements Closeable {

    private final Connection connection;
    private final JetStream jetStream;
    private final String subject;
    private final ObjectMapper mapper = new ObjectMapper();

    public TransactionPublisher(NatsConfig config) throws Exception {
        Options options = Options.builder().server(config.getServerUrl()).build();
        this.connection = Nats.connect(options);
        this.subject = config.getSubject();

        // Create the stream if it does not already exist
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(config.getStreamName())
                .subjects(config.getSubject())
                .storageType(StorageType.Memory)
                .build();
        try {
            connection.jetStreamManagement().addStream(streamConfig);
        } catch (Exception e) {
            // Stream already exists — update to ensure subject mapping is current
            connection.jetStreamManagement().updateStream(streamConfig);
        }

        this.jetStream = connection.jetStream();
    }

    public void publish(RawTransaction transaction) throws Exception {
        byte[] payload = mapper.writeValueAsString(transaction).getBytes(StandardCharsets.UTF_8);
        jetStream.publish(subject, payload);
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
