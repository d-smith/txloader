package com.txloader.processor;

import com.txloader.model.ClassifiedTransaction;
import com.txloader.model.RawTransaction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Usage: FlinkProcessorApp [--nats-url <url>] [--subject <subject>] [--stream <name>]
 *                          [--consumer <name>] [--db <path>]
 *
 * Defaults:
 *   --nats-url  nats://localhost:4222
 *   --subject   txns.raw
 *   --stream    TRANSACTIONS
 *   --consumer  flink-processor
 *   --db        transactions.db
 */
public class FlinkProcessorApp {

    public static void main(String[] args) throws Exception {
        String natsUrl    = "nats://localhost:4222";
        String subject    = "txns.raw";
        String streamName = "TRANSACTIONS";
        String consumer   = "flink-processor";
        String dbPath     = "transactions.db";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--nats-url"  -> natsUrl    = args[++i];
                case "--subject"   -> subject    = args[++i];
                case "--stream"    -> streamName = args[++i];
                case "--consumer"  -> consumer   = args[++i];
                case "--db"        -> dbPath      = args[++i];
            }
        }

        NatsConfig natsConfig = new NatsConfig(natsUrl, subject, streamName, consumer);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new NatsTransactionSource(natsConfig))
           .map(new TransactionNormalizer())
           .map(new TransactionClassifier(dbPath))
           .addSink(new SqliteSink(dbPath));

        env.execute("TX Loader Flink Processor");
    }
}
