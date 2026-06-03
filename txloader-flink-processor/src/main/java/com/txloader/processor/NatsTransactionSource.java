package com.txloader.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.txloader.model.RawTransaction;
import io.nats.client.Connection;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.time.Duration;
import java.util.List;

public class NatsTransactionSource extends RichSourceFunction<RawTransaction> {

    private final NatsConfig config;
    private volatile boolean running = true;

    private transient Connection connection;
    private transient JetStreamSubscription subscription;
    private transient ObjectMapper mapper;

    public NatsTransactionSource(NatsConfig config) {
        this.config = config;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        mapper = new ObjectMapper();
        Options options = Options.builder().server(config.getServerUrl()).build();
        connection = Nats.connect(options);

        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .stream(config.getStreamName())
                .durable(config.getConsumerName())
                .build();
        subscription = connection.jetStream().subscribe(config.getSubject(), pullOptions);
    }

    @Override
    public void run(SourceContext<RawTransaction> ctx) throws Exception {
        while (running) {
            List<Message> messages = subscription.fetch(100, Duration.ofMillis(500));
            for (Message msg : messages) {
                RawTransaction txn = mapper.readValue(msg.getData(), RawTransaction.class);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(txn);
                }
                msg.ack();
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() throws Exception {
        if (subscription != null) subscription.unsubscribe();
        if (connection != null) connection.close();
    }
}
