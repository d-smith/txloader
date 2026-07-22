package com.txloader.processor;

import com.txloader.model.ClassifiedTransaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class PostgresSink extends RichSinkFunction<ClassifiedTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSink.class);

    private static final String INSERT_SQL =
            "INSERT INTO transactions (date, merchant, amount, category, subcategory, account_id, raw_desc, " +
            "confidence, category_confidence, subcategory_confidence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private transient Connection connection;
    private transient PreparedStatement insertStmt;
    private transient long count;

    public PostgresSink(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        connection.setAutoCommit(false);
        checkSchema();
        insertStmt = connection.prepareStatement(INSERT_SQL);
        count = 0;
        LOG.info("PostgresSink opened: {}", dbUrl);
    }

    private void checkSchema() throws Exception {
        try (var stmt = connection.createStatement();
             var rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_schema = 'public' AND table_name IN ('accounts', 'transactions')")) {
            if (!rs.next() || rs.getInt(1) < 2) {
                throw new IllegalStateException(
                    "Database not initialized — run: psql <dbname> < scripts/schema.sql"
                );
            }
        }
    }

    @Override
    public void invoke(ClassifiedTransaction txn, Context context) throws Exception {
        LOG.debug("Writing: date={} merchant={} amount={} category={} subcategory={} accountId={}",
                txn.getDate(), txn.getMerchant(), txn.getAmount(),
                txn.getCategory(), txn.getSubcategory(), txn.getAccountId());
        insertStmt.setDate(1, java.sql.Date.valueOf(txn.getDate()));
        insertStmt.setString(2, txn.getMerchant());
        insertStmt.setDouble(3, Double.parseDouble(txn.getAmount()));
        insertStmt.setString(4, txn.getCategory());
        insertStmt.setString(5, txn.getSubcategory());
        insertStmt.setInt(6, txn.getAccountId());
        insertStmt.setString(7, txn.getRawDesc());
        setNullableDouble(insertStmt, 8, txn.getConfidence());
        setNullableDouble(insertStmt, 9, txn.getCategoryConfidence());
        setNullableDouble(insertStmt, 10, txn.getSubcategoryConfidence());
        insertStmt.executeUpdate();
        connection.commit();
        count++;
        if (count % 100 == 0) {
            LOG.info("PostgresSink: {} transactions written this session", count);
        }
    }

    private void setNullableDouble(PreparedStatement stmt, int idx, Double value) throws java.sql.SQLException {
        if (value == null) {
            stmt.setNull(idx, java.sql.Types.NUMERIC);
        } else {
            stmt.setDouble(idx, value);
        }
    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) insertStmt.close();
        if (connection != null) connection.close();
        LOG.info("PostgresSink closed after writing {} transactions", count);
    }
}
