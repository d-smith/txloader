package com.txloader.processor;

import com.txloader.model.ClassifiedTransaction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Assigns a category to each transaction based on merchant name and resolves
 * the account short-code to an account_id via the SQLite accounts table.
 *
 * Input array: [isoDate, merchant, amount, rawDesc, accountCode]
 *
 * Category rules are loaded from a CSV file (columns: key, category).
 * When rulesPath is null the bundled category_rules.csv resource is used.
 */
public class TransactionClassifier extends RichMapFunction<String[], ClassifiedTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionClassifier.class);

    private final String dbPath;
    private final String rulesPath;

    private transient Connection dbConnection;
    private transient Map<String, Integer> accountCache;
    private transient Map<String, String> categoryRules;

    public TransactionClassifier(String dbPath, String rulesPath) {
        this.dbPath = dbPath;
        this.rulesPath = rulesPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dbConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        accountCache = new LinkedHashMap<>();
        categoryRules = loadRules();
        LOG.info("TransactionClassifier opened DB connection: {}, loaded {} category rules from {}",
                dbPath, categoryRules.size(), rulesPath == null ? "built-in category_rules.csv" : rulesPath);
    }

    private Map<String, String> loadRules() throws Exception {
        Reader reader = rulesPath == null
                ? new InputStreamReader(TransactionClassifier.class.getResourceAsStream("/category_rules.csv"))
                : new FileReader(rulesPath);

        Map<String, String> rules = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(reader)) {
            br.readLine(); // skip header
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(",", 2);
                if (parts.length == 2) {
                    rules.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
        return rules;
    }

    @Override
    public ClassifiedTransaction map(String[] fields) throws Exception {
        String isoDate     = fields[0];
        String merchant    = fields[1];
        String amount      = fields[2];
        String rawDesc     = fields[3];
        String accountCode = fields[4];

        String category = classify(merchant);
        int accountId   = resolveAccountId(accountCode);

        LOG.debug("Classified: merchant='{}' -> category='{}', account='{}' -> accountId={}",
                merchant, category, accountCode, accountId);
        return new ClassifiedTransaction(isoDate, merchant, amount, category, accountId, rawDesc);
    }

    private String classify(String merchant) {
        for (Map.Entry<String, String> rule : categoryRules.entrySet()) {
            if (merchant.contains(rule.getKey())) {
                return rule.getValue();
            }
        }
        LOG.warn("No category rule matched merchant '{}'", merchant);
        return "Uncategorized";
    }

    private int resolveAccountId(String accountCode) throws Exception {
        if (accountCache.containsKey(accountCode)) {
            return accountCache.get(accountCode);
        }
        try (PreparedStatement stmt = dbConnection.prepareStatement(
                "SELECT id FROM accounts WHERE name = ? LIMIT 1")) {
            stmt.setString(1, accountCode);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int id = rs.getInt("id");
                    accountCache.put(accountCode, id);
                    return id;
                }
            }
        }
        try (PreparedStatement insert = dbConnection.prepareStatement(
                "INSERT INTO accounts (name, type) VALUES (?, 'Unknown')",
                Statement.RETURN_GENERATED_KEYS)) {
            insert.setString(1, accountCode);
            insert.executeUpdate();
            try (ResultSet keys = insert.getGeneratedKeys()) {
                if (keys.next()) {
                    int id = keys.getInt(1);
                    accountCache.put(accountCode, id);
                    LOG.info("Auto-inserted account '{}' with id={}", accountCode, id);
                    return id;
                }
            }
        }
        LOG.warn("Failed to retrieve generated key for account '{}', using accountId=0", accountCode);
        return 0;
    }

    @Override
    public void close() throws Exception {
        if (dbConnection != null) dbConnection.close();
    }
}
