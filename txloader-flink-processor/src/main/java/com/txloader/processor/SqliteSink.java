package com.txloader.processor;

import com.txloader.model.ClassifiedTransaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SqliteSink extends RichSinkFunction<ClassifiedTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(SqliteSink.class);

    private static final String INSERT_SQL =
            "INSERT INTO transactions (date, merchant, amount, category, account_id, raw_desc) " +
            "VALUES (?, ?, ?, ?, ?, ?)";

    private final String dbPath;
    private transient Connection connection;
    private transient PreparedStatement insertStmt;
    private transient long count;

    public SqliteSink(String dbPath) {
        this.dbPath = dbPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        connection.setAutoCommit(false);
        checkSchema();
        insertStmt = connection.prepareStatement(INSERT_SQL);
        count = 0;
        LOG.info("SqliteSink opened: {}", dbPath);
    }

    private void checkSchema() throws Exception {
        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('accounts','transactions')"
            );
            if (rs.getInt(1) < 2) {
                throw new IllegalStateException(
                    "Database not initialized — run: sqlite3 " + dbPath + " < scripts/schema.sql"
                );
            }
        }
        // Release the read transaction so TransactionClassifier's writes aren't blocked by a
        // dangling shared lock on this connection.
        connection.rollback();
    }

    @Override
    public void invoke(ClassifiedTransaction txn, Context context) throws Exception {
        LOG.debug("Writing: date={} merchant={} amount={} category={} accountId={}",
                txn.getDate(), txn.getMerchant(), txn.getAmount(), txn.getCategory(), txn.getAccountId());
        insertStmt.setString(1, txn.getDate());
        insertStmt.setString(2, txn.getMerchant());
        insertStmt.setDouble(3, Double.parseDouble(txn.getAmount()));
        insertStmt.setString(4, txn.getCategory());
        insertStmt.setInt(5, txn.getAccountId());
        insertStmt.setString(6, txn.getRawDesc());
        insertStmt.executeUpdate();
        connection.commit();
        count++;
        if (count % 100 == 0) {
            LOG.info("SqliteSink: {} transactions written this session", count);
        }
    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) insertStmt.close();
        if (connection != null) connection.close();
        LOG.info("SqliteSink closed after writing {} transactions", count);
    }
}
