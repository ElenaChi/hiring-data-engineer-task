# Retailmediatool - ETL Documentation

## Setup and Usage

### Initial Setup and Data Population

1. Clean up existing Docker volumes and containers:
```bash
docker-compose down -v
```

2. Start all services:
```bash
docker-compose up -d
```

3. Install dependencies:
```bash
uv sync
```

4. Seed initial data in PostgreSQL:
```bash
uv run python main.py batch
```

5. Run the ETL pipeline:
```bash
uv run python etl/pipeline.py
```

### Pipeline Options

The ETL pipeline supports the following command-line arguments:

- `--full-refresh`: Completely refreshes the ClickHouse tables by truncating them before syncing
- `--no-metrics-update`: Skips the metrics update step

Example usage:
```bash
# Full refresh of all tables
uv run python etl/pipeline.py --full-refresh

# Skip metrics update
uv run python etl/pipeline.py --no-metrics-update

# Both options
uv run python etl/pipeline.py --full-refresh --no-metrics-update
```

## Architecture Overview

### ClickHouse Initialization and Schema

The ClickHouse database is initialized using the existing docker-compose file, which creates and populates the necessary tables bootstrapping them from etl/clickhouse/init_ch_create_analytical_schema.sql:

1. **Dimension Tables**:
   - `date_dimension`: Pre-populated with date attributes
   - `campaign_dimension`: Denormalized view combining campaign and advertiser data

2. **Fact Table**:
   - `ad_events`: Stores raw event data (impressions and clicks)

3. **Metrics Table**:
   - `daily_campaign_performance`: Aggregated metrics for campaign analysis

This initialization approach:
- Leverages existing Docker infrastructure
- Establishes clear data boundaries
- Reduces complexity in the ETL pipeline
- Ensures consistent data structure

### Incremental Data Loading

The ETL pipeline implements an incremental loading strategy for `ad_events` to optimize resource utilization:

1. For impressions and clicks, we track the last sync time
2. Only new or updated records since the last sync are processed
3. This approach minimizes the data transfer between PostgreSQL and ClickHouse
4. The `inserted_at` timestamp is used to track changes

### Data Flow

1. PostgreSQL (Source):
   - Contains raw data (campaigns, impressions, clicks)
   - Uses standard SQL tables with appropriate indexes

2. ClickHouse (Target):
   - Stores denormalized and aggregated data
   - Uses specialized table engines (MergeTree, ReplacingMergeTree)
   - Implements materialized views for common queries

## Data Visualization and Querying

### PostgreSQL (PGAdmin)
- Access PGAdmin at: `http://localhost:5050`
- Default credentials:
  - Email: `admin@admin.com`
  - Password: `admin`

### ClickHouse
Connect to the ClickHouse client:
```bash
docker-compose exec clickhouse-client clickhouse-client --host clickhouse
```

### Analytical Queries

The `analytical_queries.sql` file contains a collection of pre-defined queries for analyzing campaign performance:

1. **Campaign Performance Metrics**
   - Daily performance metrics (CTR, CPM, CPC)
   - Overall campaign performance summary
   - Key metrics: impressions, clicks, spend, CTR, CPM, CPC

2. **Time-based Analysis**
   - Monthly performance trends
   - Weekend vs weekday performance comparison
   - Helps identify seasonal patterns and optimal timing

3. **Campaign Health Metrics**
   - Active campaign performance monitoring
   - Budget utilization tracking
   - Campaign duration analysis
   - Helps identify underperforming campaigns

4. **Event Analysis**
   - Event type distribution (impressions vs clicks)
   - Hourly event patterns
   - Helps understand user behavior patterns

5. **Advertiser Performance**
   - Advertiser-level performance summary
   - Campaign count and performance metrics
   - Helps evaluate advertiser ROI

6. **Performance Anomalies**
   - Identifies campaigns with unusual CTR patterns
   - Helps detect potential issues or opportunities
   - Uses statistical deviation from average performance

Example usage:
```sql
SELECT * FROM daily_campaign_performance LIMIT 10;
```

These queries can be used to:
- Monitor campaign health
- Track performance trends
- Identify optimization opportunities
- Generate reports
- Support data-driven decision making

## Future Improvements

### 1. Scheduling
- Implement a scheduling system (e.g. Airflow)
- Add support for different sync frequencies (hourly, daily, real-time)
- Implement retry mechanisms for failed syncs

### 2. CI/CD and Testing
- Add unit tests for pipeline components
- Implement integration tests for the full ETL flow
- Add data validation tests
- Set up CI/CD pipeline

### 3. Pipeline Enhancements
- Support multiple data sources (not just PostgreSQL)
- Add support for schema evolution
- Implement CDC (Change Data Capture) for real-time updates
- Add monitoring and alerting

### 4. Performance Optimization
- Implement parallel processing for large datasets
- Add batch processing capabilities
- Optimize memory usage
- Add caching mechanisms
- Implement incremental materialized view updates

### 5. Data Quality
- Implement data validation rules
- Add data quality checks
- Create data quality dashboards
- Implement data cleansing processes 