-- ============================================================
-- Toll traffic streaming pipeline: target database and table
-- Run:  mysql --host=mysql --port=3306 --user=root -p < sql/01_create_schema.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS tolldata;
USE tolldata;

-- One row = one vehicle passing one toll plaza (schema from the course)
CREATE TABLE IF NOT EXISTS livetolldata (
    timestamp      DATETIME,
    vehicle_id     INT,
    vehicle_type   CHAR(15),
    toll_plaza_id  SMALLINT
);

-- Optional: speeds up the per-plaza / per-time analysis queries
-- in 02_analysis_queries.sql once the table grows.
CREATE INDEX idx_plaza_time ON livetolldata (toll_plaza_id, timestamp);
