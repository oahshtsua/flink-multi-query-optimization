SET 'sql-client.execution.result-mode' = 'tableau';

-- Create table sensors
CREATE TABLE sensors (
  day_str      STRING,
  time_str     STRING,
  epoch        BIGINT,
  mote_id      INT,
  temperature  DOUBLE,
  humidity     DOUBLE,
  light        DOUBLE,
  voltage      DOUBLE,
  ts AS CAST(CONCAT(day_str, ' ', time_str) AS TIMESTAMP(3)),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensors',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sensors',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.field-delimiter' = ' ',
  'csv.null-literal' = ''
);

-- Create table
CREATE TABLE ad_clicks (
  click         INT,
  impression    INT,
  display_url   STRING,
  ad_id         BIGINT,
  advertiser_id BIGINT,
  ad_depth      INT,
  ad_pos        INT,
  query_id      BIGINT,
  keyword_id    BIGINT,
  title_id      BIGINT,
  desc_id       BIGINT,
  user_id       BIGINT,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ad_clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-ad-clicks',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.field-delimiter' = '\t'
);

-- Verify that you can read the data
SELECT * FROM sensors LIMIT 10;

SELECT * FROM ad_clicks LIMIT 10;

-- Queries
-- Literal value
SELECT ad_id, SUM(click) AS clicks, SUM(impression) AS impressions
FROM ad_clicks
WHERE ad_depth = 2 	-- vary ad_depth: 1, 2, 3
GROUP BY ad_id;

-- Window sizes
SELECT window_start, window_end, ad_pos,
       SUM(click) AS clicks, COUNT(*) AS events
FROM TABLE(TUMBLE(TABLE ad_clicks, DESCRIPTOR(ts), INTERVAL '200' MILLISECOND))  -- vary: 100, 200, 500 ms
GROUP BY window_start, window_end, ad_pos;

-- Alias
SELECT advertiser_id AS adv, COUNT(DISTINCT user_id) AS users
FROM ad_clicks
WHERE click > 0
GROUP BY advertiser_id;
-- Maybe vary the alias for advertiser_id and count of users
