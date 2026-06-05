package com.txloader.processor;

import com.txloader.model.ClassifiedTransaction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Assigns a category to each transaction based on merchant name and resolves
 * the account short-code to an account_id via the SQLite accounts table.
 *
 * Input array: [isoDate, merchant, amount, rawDesc, accountCode]
 */
public class TransactionClassifier extends RichMapFunction<String[], ClassifiedTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionClassifier.class);

    private static final Map<String, String> CATEGORY_RULES = new HashMap<>();

    static {
        CATEGORY_RULES.put("STARBUCKS", "Food & Drink");
        CATEGORY_RULES.put("MCDONALD", "Food & Drink");
        CATEGORY_RULES.put("CHIPOTLE", "Food & Drink");
        CATEGORY_RULES.put("DOORDASH", "Food & Drink");
        CATEGORY_RULES.put("UBER EATS", "Food & Drink");
        CATEGORY_RULES.put("WHOLE FOODS", "Groceries");
        CATEGORY_RULES.put("TRADER JOE", "Groceries");
        CATEGORY_RULES.put("KROGER", "Groceries");
        CATEGORY_RULES.put("AMAZON", "Shopping");
        CATEGORY_RULES.put("WALMART", "Shopping");
        CATEGORY_RULES.put("TARGET", "Shopping");
        CATEGORY_RULES.put("NETFLIX", "Entertainment");
        CATEGORY_RULES.put("SPOTIFY", "Entertainment");
        CATEGORY_RULES.put("HULU", "Entertainment");
        CATEGORY_RULES.put("SHELL", "Gas & Fuel");
        CATEGORY_RULES.put("CHEVRON", "Gas & Fuel");
        CATEGORY_RULES.put("EXXON", "Gas & Fuel");
        CATEGORY_RULES.put("UBER", "Transportation");
        CATEGORY_RULES.put("LYFT", "Transportation");
    }

    private final String dbPath;
    private transient Connection dbConnection;
    private transient Map<String, Integer> accountCache;

    public TransactionClassifier(String dbPath) {
        this.dbPath = dbPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dbConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        accountCache = new HashMap<>();
        LOG.info("TransactionClassifier opened DB connection: {}", dbPath);
    }

    @Override
    public ClassifiedTransaction map(String[] fields) throws Exception {
        String isoDate    = fields[0];
        String merchant   = fields[1];
        String amount     = fields[2];
        String rawDesc    = fields[3];
        String accountCode = fields[4];

        String category = classify(merchant);
        int accountId   = resolveAccountId(accountCode);

        LOG.debug("Classified: merchant='{}' -> category='{}', account='{}' -> accountId={}",
                merchant, category, accountCode, accountId);
        return new ClassifiedTransaction(isoDate, merchant, amount, category, accountId, rawDesc);
    }

    private String classify(String merchant) {
        for (Map.Entry<String, String> rule : CATEGORY_RULES.entrySet()) {
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
                    return id;fir
                }
            }
        }
        LOG.warn("Account code '{}' not found in accounts table — using accountId=0", accountCode);
        return 0;
    }

    @Override
    public void close() throws Exception {
        if (dbConnection != null) dbConnection.close();
    }
}
