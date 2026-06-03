package com.txloader.processor;

import com.txloader.model.ClassifiedTransaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SqliteSink extends RichSinkFunction<ClassifiedTransaction> {

    private static final String INSERT_SQL =
            "INSERT INTO transactions (date, merchant, amount, category, account_id, raw_desc) " +
            "VALUES (?, ?, ?, ?, ?, ?)";

    private final String dbPath;
    private transient Connection connection;
    private transient PreparedStatement insertStmt;

    public SqliteSink(String dbPath) {
        this.dbPath = dbPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        connection.setAutoCommit(false);
        ensureSchema();
        insertStmt = connection.prepareStatement(INSERT_SQL);
    }

    private void ensureSchema() throws Exception {
        try (var stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id   INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL
                )
            """);
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id         INTEGER PRIMARY KEY,
                    date       DATE NOT NULL,
                    merchant   TEXT NOT NULL,
                    amount     REAL NOT NULL,
                    category   TEXT,
                    account_id INTEGER REFERENCES accounts(id),
                    raw_desc   TEXT
                )
            """);
            connection.commit();
        }
    }

    @Override
    public void invoke(ClassifiedTransaction txn, Context context) throws Exception {
        insertStmt.setString(1, txn.getDate());
        insertStmt.setString(2, txn.getMerchant());
        insertStmt.setDouble(3, Double.parseDouble(txn.getAmount()));
        insertStmt.setString(4, txn.getCategory());
        insertStmt.setInt(5, txn.getAccountId());
        insertStmt.setString(6, txn.getRawDesc());
        insertStmt.executeUpdate();
        connection.commit();
    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) insertStmt.close();
        if (connection != null) connection.close();
    }
}
