-- Dimension Tables
CREATE TABLE IF NOT EXISTS date_dimension (
    date_id Date,
    day_of_week UInt8,
    day_of_month UInt8,
    month UInt8,
    quarter UInt8,
    year UInt16,
    is_weekend UInt8
) ENGINE = MergeTree()
ORDER BY (date_id);

-- Populate date_dimension table
INSERT INTO date_dimension
SELECT 
    date_id,
    toDayOfWeek(date_id) as day_of_week,
    toDayOfMonth(date_id) as day_of_month,
    toMonth(date_id) as month,
    toQuarter(date_id) as quarter,
    toYear(date_id) as year,
    if(toDayOfWeek(date_id) IN (6, 7), 1, 0) as is_weekend
FROM (
    SELECT toDate('2020-01-01') + number as date_id
    FROM numbers(2192)  -- 6 years worth of days
); 

CREATE TABLE IF NOT EXISTS campaign_dimension (
    campaign_id UInt32,
    campaign_name String,
    advertiser_name String,
    bid_amount Decimal(10,2),
    budget_amount Decimal(10,2),
    campaign_duration_days UInt16,
    is_active UInt8,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (campaign_id);


-- Fact Tables
CREATE TABLE IF NOT EXISTS ad_events (
    event_id UInt32,
    campaign_id UInt32,
    event_type Enum('impression' = 1, 'click' = 2),
    created_at DateTime,
    event_date Date DEFAULT toDate(created_at),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (campaign_id, event_date, event_type);


-- Metrics Tables
CREATE TABLE IF NOT EXISTS daily_campaign_performance (
    campaign_id UInt32,
    date_id Date DEFAULT toDate(now()),
    impressions_count UInt64,
    clicks_count UInt64,
    spend_amount Decimal(10,2),
    ctr Decimal(5,4),
    cpm Decimal(10,2),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (date_id, campaign_id)
PARTITION BY toYYYYMMDD(date_id);

-- Materialized Views for Common Queries
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_clicks
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (campaign_id, event_date)
AS SELECT
        campaign_id,
        event_date,
        count() AS click_count
    FROM ad_events
    WHERE event_type = 'click'
    GROUP BY campaign_id, event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_impressions
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (campaign_id, event_date)
AS SELECT
        campaign_id,
        event_date,
        count() AS click_count
    FROM ad_events
    WHERE event_type = 'impression'
    GROUP BY campaign_id, event_date;
