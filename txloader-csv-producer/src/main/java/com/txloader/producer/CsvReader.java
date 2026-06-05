package com.txloader.producer;

import com.txloader.model.RawTransaction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class CsvReader {

    // Expected CSV columns (case-insensitive header matching)
    private static final String COL_DATE = "date";
    private static final String COL_DESCRIPTION = "description";
    private static final String COL_AMOUNT = "amount";

    public List<RawTransaction> read(String filePath, String account) throws IOException {
        List<RawTransaction> transactions = new ArrayList<>();

        try (Reader reader = new FileReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setIgnoreHeaderCase(true)
                     .setTrim(true)
                     .build()
                     .parse(reader)) {

            for (CSVRecord record : parser) {
                transactions.add(new RawTransaction(
                        record.get(COL_DATE),
                        account,
                        record.get(COL_DESCRIPTION),
                        record.get(COL_AMOUNT)
                ));
            }
        }

        return transactions;
    }
}
