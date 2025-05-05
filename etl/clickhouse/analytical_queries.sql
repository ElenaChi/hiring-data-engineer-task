-- Analytical Queries for Campaign Performance Metrics
-- =================================================

-- 1. Campaign Performance Metrics
-- ------------------------------

-- Daily Campaign Performance (CTR, CPM, CPC)
SELECT 
    c.campaign_id,
    c.campaign_name,
    c.advertiser_name,
    d.date_id,
    d.impressions_count,
    d.clicks_count,
    d.spend_amount,
    d.ctr,
    d.cpm,
    CASE 
        WHEN d.clicks_count > 0 THEN d.spend_amount / d.clicks_count 
        ELSE 0 
    END as cpc
FROM daily_campaign_performance d
JOIN campaign_dimension c ON d.campaign_id = c.campaign_id
ORDER BY d.date_id DESC, d.impressions_count DESC;

-- Campaign Performance Summary (Overall)
SELECT 
    c.campaign_id,
    c.campaign_name,
    c.advertiser_name,
    SUM(d.impressions_count) as total_impressions,
    SUM(d.clicks_count) as total_clicks,
    SUM(d.spend_amount) as total_spend,
    SUM(d.clicks_count) / SUM(d.impressions_count) as overall_ctr,
    SUM(d.spend_amount) / (SUM(d.impressions_count) / 1000.0) as overall_cpm,
    SUM(d.spend_amount) / SUM(d.clicks_count) as overall_cpc
FROM daily_campaign_performance d
JOIN campaign_dimension c ON d.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.advertiser_name;

-- 2. Time-based Analysis
-- ---------------------

-- Monthly Performance Trends
SELECT 
    toYear(d.date_id) as year,
    toMonth(d.date_id) as month,
    SUM(d.impressions_count) as monthly_impressions,
    SUM(d.clicks_count) as monthly_clicks,
    SUM(d.spend_amount) as monthly_spend,
    SUM(d.clicks_count) / SUM(d.impressions_count) as monthly_ctr
FROM daily_campaign_performance d
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- Weekend vs Weekday Performance
SELECT 
    dd.is_weekend,
    SUM(d.impressions_count) as total_impressions,
    SUM(d.clicks_count) as total_clicks,
    SUM(d.spend_amount) as total_spend,
    SUM(d.clicks_count) / SUM(d.impressions_count) as ctr
FROM daily_campaign_performance d
JOIN date_dimension dd ON d.date_id = dd.date_id
GROUP BY dd.is_weekend;

-- 3. Campaign Health Metrics
-- ------------------------

-- Active Campaign Performance
SELECT 
    c.campaign_id,
    c.campaign_name,
    c.advertiser_name,
    c.budget_amount,
    SUM(d.spend_amount) as total_spend,
    (c.budget_amount - SUM(d.spend_amount)) as remaining_budget,
    (SUM(d.spend_amount) / c.budget_amount) * 100 as budget_utilization_percent
FROM daily_campaign_performance d
JOIN campaign_dimension c ON d.campaign_id = c.campaign_id
WHERE c.is_active = 1
GROUP BY c.campaign_id, c.campaign_name, c.advertiser_name, c.budget_amount;

-- Campaign Duration Analysis
SELECT 
    c.campaign_duration_days,
    AVG(d.impressions_count) as avg_daily_impressions,
    AVG(d.clicks_count) as avg_daily_clicks,
    AVG(d.ctr) as avg_ctr
FROM daily_campaign_performance d
JOIN campaign_dimension c ON d.campaign_id = c.campaign_id
GROUP BY c.campaign_duration_days
ORDER BY c.campaign_duration_days;

-- 4. Event Analysis
-- ---------------

-- Event Type Distribution
SELECT 
    event_type,
    COUNT(*) as event_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM ad_events
GROUP BY event_type;

-- Event Timing Analysis
SELECT 
    toHour(created_at) as hour_of_day,
    COUNT(*) as event_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM ad_events
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 5. Advertiser Performance
-- -----------------------

-- Advertiser Performance Summary
SELECT 
    c.advertiser_name,
    COUNT(DISTINCT c.campaign_id) as total_campaigns,
    SUM(d.impressions_count) as total_impressions,
    SUM(d.clicks_count) as total_clicks,
    SUM(d.spend_amount) as total_spend,
    SUM(d.clicks_count) / SUM(d.impressions_count) as overall_ctr
FROM daily_campaign_performance d
JOIN campaign_dimension c ON d.campaign_id = c.campaign_id
GROUP BY c.advertiser_name
ORDER BY total_spend DESC;

-- 6. Performance Anomalies
-- ----------------------

-- Campaigns with Unusual CTR
SELECT 
    c.campaign_id,
    c.campaign_name,
    d.date_id,
    d.ctr,
    avg_ctr.avg_ctr,
    d.ctr - avg_ctr.avg_ctr as ctr_deviation
FROM daily_campaign_performance d
JOIN campaign_dimension c ON d.campaign_id = c.campaign_id
JOIN (
    SELECT 
        campaign_id,
        AVG(ctr) as avg_ctr
    FROM daily_campaign_performance
    GROUP BY campaign_id
) avg_ctr ON d.campaign_id = avg_ctr.campaign_id
WHERE ABS(d.ctr - avg_ctr.avg_ctr) > 0.1
ORDER BY ctr_deviation DESC; 