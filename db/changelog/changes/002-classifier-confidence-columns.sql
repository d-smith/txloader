-- liquibase formatted sql

-- changeset txloader:6
ALTER TABLE transactions ADD COLUMN confidence NUMERIC(5,4);
ALTER TABLE transactions ADD COLUMN category_confidence NUMERIC(5,4);
ALTER TABLE transactions ADD COLUMN subcategory_confidence NUMERIC(5,4);
