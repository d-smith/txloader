package com.txloader.producer;

import com.txloader.model.RawTransaction;

import java.util.List;

/**
 * Usage: CsvProducerApp --account <account> <csv-file> [<csv-file> ...]
 *                       [--nats-url <url>] [--subject <subject>] [--stream <name>]
 *
 * Required:
 *   --account   account identifier applied to every transaction row
 *
 * Defaults:
 *   --nats-url  nats://localhost:4222
 *   --subject   txns.raw
 *   --stream    TRANSACTIONS
 */
public class CsvProducerApp {

    public static void main(String[] args) throws Exception {
        String natsUrl = "nats://localhost:4222";
        String subject = "txns.raw";
        String streamName = "TRANSACTIONS";
        String account = null;
        java.util.List<String> files = new java.util.ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--nats-url" -> natsUrl = args[++i];
                case "--subject"  -> subject  = args[++i];
                case "--stream"   -> streamName = args[++i];
                case "--account"  -> account = args[++i];
                case "--help"     -> { printUsage(); return; }
                default           -> files.add(args[i]);
            }
        }

        if (files.isEmpty()) {
            System.err.println("Error: at least one CSV file path is required.");
            printUsage();
            System.exit(1);
        }

        if (account == null) {
            System.err.println("Error: --account is required.");
            printUsage();
            System.exit(1);
        }

        NatsConfig config = new NatsConfig(natsUrl, subject, streamName);
        CsvReader reader = new CsvReader();

        try (TransactionPublisher publisher = new TransactionPublisher(config)) {
            for (String file : files) {
                System.out.printf("Reading %s%n", file);
                List<RawTransaction> transactions = reader.read(file, account);
                for (RawTransaction txn : transactions) {
                    publisher.publish(txn);
                }
                System.out.printf("Published %d transactions from %s%n", transactions.size(), file);
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: CsvProducerApp --account <account> <csv-file> [<csv-file> ...]");
        System.out.println("       [--nats-url <url>] [--subject <subject>] [--stream <name>]");
    }
}
