package com.txloader.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.Map;

public class KeywordMerchantCategorizer implements MerchantCategorizer {

    private static final Logger LOG = LoggerFactory.getLogger(KeywordMerchantCategorizer.class);

    private final String rulesPath;
    private transient Map<String, String> categoryRules;

    public KeywordMerchantCategorizer(String rulesPath) {
        this.rulesPath = rulesPath;
    }

    @Override
    public void open() throws Exception {
        categoryRules = loadRules();
        LOG.info("KeywordMerchantCategorizer loaded {} rules from {}",
                categoryRules.size(), rulesPath == null ? "built-in category_rules.csv" : rulesPath);
    }

    private Map<String, String> loadRules() throws Exception {
        Reader reader = rulesPath == null
                ? new InputStreamReader(KeywordMerchantCategorizer.class.getResourceAsStream("/category_rules.csv"))
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
    public String categorize(String merchant) {
        for (Map.Entry<String, String> rule : categoryRules.entrySet()) {
            if (merchant.contains(rule.getKey())) {
                return rule.getValue();
            }
        }
        LOG.warn("No category rule matched merchant '{}'", merchant);
        return "Uncategorized";
    }
}
