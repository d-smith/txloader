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
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Flink map operator that assigns a category to each transaction and resolves
 * the account short-code to an account_id via the accounts table.
 *
 * Category assignment is delegated to a {@link MerchantCategorizer}, allowing
 * different classification strategies to be plugged in via configuration.
 *
 * Input array: [isoDate, merchant, amount, rawDesc, accountCode]
 */
public class TransactionClassifier extends RichMapFunction<String[], ClassifiedTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionClassifier.class);

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final MerchantCategorizer categorizer;

    private transient Connection dbConnection;
    private transient Map<String, Integer> accountCache;

    public TransactionClassifier(String dbUrl, String dbUser, String dbPassword, MerchantCategorizer categorizer) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.categorizer = categorizer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dbConnection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        accountCache = new LinkedHashMap<>();
        categorizer.open();
        LOG.info("TransactionClassifier opened: db={}", dbUrl);
    }

    @Override
    public ClassifiedTransaction map(String[] fields) throws Exception {
        String isoDate     = fields[0];
        String merchant    = fields[1];
        String amount      = fields[2];
        String rawDesc     = fields[3];
        String accountCode = fields[4];

        String category = categorizer.categorize(merchant);
        int accountId   = resolveAccountId(accountCode);

        LOG.debug("Classified: merchant='{}' -> category='{}', account='{}' -> accountId={}",
                merchant, category, accountCode, accountId);
        return new ClassifiedTransaction(isoDate, merchant, amount, category, accountId, rawDesc);
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
        categorizer.close();
        if (dbConnection != null) dbConnection.close();
    }
}
