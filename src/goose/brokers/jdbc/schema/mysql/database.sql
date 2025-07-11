-- Goose JDBC Broker MySQL Database Creation
-- This creates the database for Goose. Run this before tables.sql
--
-- Example installation:
-- mysql -h localhost -u root -p < database.sql
-- mysql -h localhost -u root -p goose < tables.sql

CREATE DATABASE IF NOT EXISTS goose;