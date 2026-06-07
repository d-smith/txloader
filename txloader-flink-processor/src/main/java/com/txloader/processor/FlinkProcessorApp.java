package com.txloader.processor;

import com.txloader.model.ClassifiedTransaction;
import com.txloader.model.RawTransaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Usage: FlinkProcessorApp [--nats-url <url>] [--subject <subject>] [--stream <name>]
 *                          [--consumer <name>] [--db <path>] [--web-port <port>]
 *                          [--classifier keyword] [--rules <path>]
 *
 * Defaults:
 *   --nats-url    nats://localhost:4222
 *   --subject     txns.raw
 *   --stream      TRANSACTIONS
 *   --consumer    flink-processor
 *   --db          transactions.db
 *   --web-port    8081
 *   --classifier  keyword
 *   --rules       (built-in category_rules.csv, applies to keyword classifier only)
 */
public class FlinkProcessorApp {

    public static void main(String[] args) throws Exception {
        String natsUrl    = "nats://localhost:4222";
        String subject    = "txns.raw";
        String streamName = "TRANSACTIONS";
        String consumer   = "flink-processor";
        String dbPath     = "transactions.db";
        String classifier = "keyword";
        String rulesPath  = null;
        int    webPort    = 8081;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--nats-url"    -> natsUrl    = args[++i];
                case "--subject"     -> subject    = args[++i];
                case "--stream"      -> streamName = args[++i];
                case "--consumer"    -> consumer   = args[++i];
                case "--db"          -> dbPath      = args[++i];
                case "--classifier"  -> classifier  = args[++i];
                case "--rules"       -> rulesPath   = args[++i];
                case "--web-port"    -> webPort     = Integer.parseInt(args[++i]);
            }
        }

        MerchantCategorizer categorizer = switch (classifier) {
            case "keyword" -> new KeywordMerchantCategorizer(rulesPath);
            default -> throw new IllegalArgumentException("Unknown classifier: " + classifier);
        };

        NatsConfig natsConfig = new NatsConfig(natsUrl, subject, streamName, consumer);

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, webPort);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        env.addSource(new NatsTransactionSource(natsConfig))
           .map(new TransactionNormalizer())
           .map(new TransactionClassifier(dbPath, categorizer))
           .addSink(new SqliteSink(dbPath));

        env.execute("TX Loader Flink Processor");
    }
}
