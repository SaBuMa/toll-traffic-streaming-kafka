-- ============================================================
-- Analysis queries: turning the stream into congestion insights
-- Run inside the mysql CLI after:  USE tolldata;
-- ============================================================

-- 0. Lab verification: first 10 rows
SELECT * FROM livetolldata LIMIT 10;

-- 1. Total vehicles per toll plaza (busiest first)
SELECT toll_plaza_id, COUNT(*) AS vehicles
FROM livetolldata
GROUP BY toll_plaza_id
ORDER BY vehicles DESC;

-- 2. Vehicle mix across the whole network
SELECT vehicle_type,
       COUNT(*) AS vehicles,
       ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM livetolldata), 1) AS pct
FROM livetolldata
GROUP BY vehicle_type
ORDER BY vehicles DESC;

-- 3. Heavy-vehicle (truck) share per plaza: trucks slow toll lanes most
SELECT toll_plaza_id,
       SUM(vehicle_type = 'truck') AS trucks,
       COUNT(*)                    AS vehicles,
       ROUND(100 * SUM(vehicle_type = 'truck') / COUNT(*), 1) AS truck_pct
FROM livetolldata
GROUP BY toll_plaza_id
ORDER BY truck_pct DESC;

-- 4. Traffic per minute per plaza (peak detection)
SELECT toll_plaza_id,
       DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i') AS minute,
       COUNT(*) AS vehicles
FROM livetolldata
GROUP BY toll_plaza_id, minute
ORDER BY vehicles DESC
LIMIT 10;

-- 5. Pipeline health: how many rows, and over what time window
SELECT COUNT(*)        AS total_rows,
       MIN(timestamp)  AS first_event,
       MAX(timestamp)  AS last_event,
       TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp)) AS seconds_streamed
FROM livetolldata;

-- 6. Duplicate check: events loaded more than once
--    (expected after a replay from offset 0, see docs/troubleshooting.md)
SELECT `timestamp`, vehicle_id, vehicle_type, toll_plaza_id, COUNT(*) AS copies
FROM livetolldata
GROUP BY `timestamp`, vehicle_id, vehicle_type, toll_plaza_id
HAVING COUNT(*) > 1
ORDER BY copies DESC
LIMIT 10;
