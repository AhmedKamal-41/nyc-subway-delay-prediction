# NYC Subway Delay Risk - Step 1

Foundation setup with PostgreSQL database and connectivity.

## Prerequisites

- Docker and Docker Compose
- Python 3.11 or higher

## Setup Steps

1. **Copy environment file:**
   ```bash
   cp .env.example .env
   ```

2. **Start services:**
   ```bash
   make up
   ```

3. **Verify database connectivity:**
   ```bash
   make db-check
   ```

## Services

- **PostgreSQL**: Running on `localhost:5432`
- **pgAdmin**: Available at `http://localhost:5050`

### Connecting pgAdmin to PostgreSQL

1. Open http://localhost:5050 in your browser
2. Log in with credentials from `.env`:
   - Email: `admin@local.dev` (default)
   - Password: `admin` (default)
3. Add a new server:
   - **Host**: `postgres` (service name in Docker network)
   - **Port**: `5432`
   - **Username**: Value from `POSTGRES_USER` in `.env`
   - **Password**: Value from `POSTGRES_PASSWORD` in `.env`
   - **Database**: Value from `POSTGRES_DB` in `.env`

## Makefile Commands

- `make up` - Start Docker services
- `make down` - Stop Docker services
- `make logs` - Follow service logs
- `make db-check` - Verify database connectivity
- `make reset-db` - Remove volumes and restart (clears all data)

## Python Environment

Install dependencies:
```bash
pip install -r requirements.txt
```

## Database Schema

The database initializes with:
- Schema: `mta`
- Tables: `ingest_runs`, `raw_events`, `station_minute_facts`
- See `docker/postgres/init/001_init.sql` for details

---

## Step 2: Ingestion Setup

### Prerequisites

Install Python dependencies:
```bash
pip install -r requirements.txt
```

### Configuration

1. **Copy environment file** (if not already done):
   ```bash
   cp .env.example .env
   ```

2. **Update `.env` with MTA API configuration:**
   - Set `MTA_API_KEY` to your MTA API key
   - Update feed URLs:
     - `SERVICE_ALERTS_URL`
     - `TRIP_UPDATES_URL`
     - `VEHICLE_POSITIONS_URL`
   - Optionally adjust `POLL_INTERVAL_SECONDS` (default: 60)

### Running Ingestion

**Single ingestion run:**
```bash
python scripts/ingest_once.py
```

**Continuous ingestion loop:**
```bash
python scripts/ingest_loop.py
```

### Verification

After running `ingest_once.py`, verify data in pgAdmin:
1. Connect to database (see Step 1 instructions)
2. Query: `SELECT COUNT(*) FROM mta.raw_events;`
3. Query: `SELECT * FROM mta.ingest_runs ORDER BY started_at DESC LIMIT 5;`

The ingestion process will:
- Create run records in `mta.ingest_runs`
- Fetch GTFS-RT feeds from configured URLs
- Parse protobuf messages and extract key fields
- Store raw events in `mta.raw_events` with JSONB payloads

---

## Step 3: Aggregation

### Prerequisites

- Step 2 must be completed with data in `mta.raw_events` table

### Running Aggregation

Aggregate raw events into time-bucketed facts:
```bash
python scripts/aggregate_facts.py
```

The aggregation:
- Counts events by feed type (service_alerts, trip_updates, vehicle_positions)
- Groups by time buckets (60 seconds and 300 seconds)
- Groups by station (line_id, stop_id)
- Upserts results into `mta.station_minute_facts` table
- Processes events from the last N minutes (configured via `WINDOW_MINUTES` env var, default: 120)

### Configuration

Set `WINDOW_MINUTES` in `.env` to control how far back to aggregate (default: 120 minutes).

### Verification

After running aggregation, verify in pgAdmin:
```sql
SELECT bucket_size_seconds, COUNT(*) as fact_count
FROM mta.station_minute_facts
GROUP BY bucket_size_seconds;
```

---

## Step 4: Dataset Building

### Prerequisites

- Step 3 must be completed with aggregated data in `mta.station_minute_facts` table

### Building Dataset

Create ML dataset with features and labels:
```bash
python scripts/build_dataset.py
```

The dataset building process:
- Loads 60-second bucket facts from `mta.station_minute_facts`
- Creates features:
  - Direct counts: alerts_count, trip_updates_count, vehicle_positions_count
  - Time features: hour_of_day, day_of_week
  - Rolling features: 15-minute and 60-minute rolling sums per station (line_id, stop_id)
- Creates binary labels: 1 if alerts occur in the next 15 minutes, 0 otherwise
- Splits data time-based: 70% train, 15% val, 15% test
- Saves to `data/train.parquet`, `data/val.parquet`, `data/test.parquet`

### Output

The script prints:
- Number of rows per split
- Positive label rate per split

Output files are saved in the `data/` directory (automatically created if needed).

---

## Step 5: Model Training

### Prerequisites

- Step 4 must be completed with dataset files in `data/` directory

### Training Models

Train and evaluate models:
```bash
python scripts/train.py
```

The training process:
- Loads train/val/test datasets from parquet files
- Trains two models:
  - **Logistic Regression**: Baseline model with balanced class weights
  - **LightGBM**: Gradient boosting model with early stopping
- Evaluates models on validation and test sets:
  - Metrics: accuracy, precision, recall, f1, roc_auc
  - Generates confusion matrix plots
- Tracks experiments with MLflow (experiment name: "nyc_delay_risk")
- Saves best model (based on validation ROC-AUC) to `models/best_model.pkl`

### Outputs

- **MLflow**: Experiment tracking with parameters, metrics, and model artifacts
- **Confusion matrices**: Saved as PNG files in `models/` directory
- **Best model**: Saved to `models/best_model.pkl` for deployment

View MLflow UI:
```bash
mlflow ui
```

Then open http://localhost:5000 in your browser.

---

## Step 6: FastAPI Service

### Prerequisites

- Step 5 must be completed with `models/best_model.pkl` file

### Starting the API

Start all services including the API:
```bash
docker compose up -d
```

The API service:
- Loads the trained model from `models/best_model.pkl` at startup
- Mounts the `models/` directory as a volume into the container
- Exposes port 8000

### API Endpoints

**Health check:**
```bash
curl http://localhost:8000/health
```

**Predict delay risk:**
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"line_id": "A", "stop_id": "A12"}'
```

### API Response

The `/predict` endpoint returns:
- `line_id`, `stop_id`: Station identifiers
- `as_of`: Timestamp of the latest data point used (ISO-8601 format)
- `risk_label`: Binary prediction (0 = no risk, 1 = risk of delay)
- `risk_probability`: Probability of delay (0.0 to 1.0)
- `features`: Dictionary of computed features used for prediction

### Feature Computation

The API computes features matching the training pipeline:
- Current counts from the latest 60-second bucket
- Rolling sums over 15-minute and 60-minute windows
- Time features (hour_of_day, day_of_week) from the latest timestamp

If no data is found for a station, the API returns a 404 error.

### API Documentation

FastAPI provides interactive API documentation:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

---

## Step 7: Streamlit Dashboard

### Prerequisites

- Step 6 must be completed with the API service running
- API must be accessible at http://localhost:8000

### Running the Dashboard

Start the Streamlit dashboard:
```bash
streamlit run dashboard/app.py
```

The dashboard will open in your browser at http://localhost:8501

### Dashboard Features

The dashboard provides:
- **Real-time risk monitoring**: Displays top N risky stations based on current predictions
- **Line filtering**: Filter results by subway line (e.g., "A", "1", "2")
- **Interactive table**: Shows risk probability, risk label, and timestamp for each station
- **Summary metrics**: Total stations, high-risk count, and average risk probability
- **Refresh button**: Manually reload predictions from the API

### Tracked Stops

The dashboard uses a hardcoded list of tracked stops for MVP. These stops are queried against the API to generate the risk rankings. You can modify the `TRACKED_STOPS` list in `dashboard/app.py` to add or change monitored stations.

### Usage

1. Ensure the API is running: `docker compose up -d`
2. Start the dashboard: `streamlit run dashboard/app.py`
3. Use the sidebar filters to:
   - Filter by line ID (optional)
   - Set the number of top results to display
   - Click "Refresh" to reload predictions
4. View the results table sorted by risk probability (highest first)

---

## Step 8: Monitoring

### Prerequisites

- Step 6 must be completed with the API service running

### Starting Monitoring Services

Start all services including monitoring:
```bash
docker compose up -d
```

This starts:
- **Prometheus**: Metrics collection and storage (http://localhost:9090)
- **Grafana**: Metrics visualization dashboard (http://localhost:3000)

### Monitoring Setup

The monitoring infrastructure includes:

- **FastAPI Metrics**: The API automatically exposes Prometheus metrics at `/metrics`
- **Prometheus**: Scrapes API metrics every 15 seconds
- **Grafana**: Pre-configured dashboard with API metrics visualization

### Accessing Monitoring

**Prometheus:**
- URL: http://localhost:9090
- Query metrics directly using PromQL
- View targets and scrape status

**Grafana:**
- URL: http://localhost:3000
- Default credentials:
  - Username: `admin`
  - Password: `admin`
- Pre-configured "API Metrics" dashboard includes:
  - **Request Rate**: Requests per second by endpoint
  - **Request Latency (p95)**: 95th percentile latency
  - **Error Rate**: HTTP 5xx errors per second

### Dashboard Features

The Grafana dashboard provides:
- Real-time API performance metrics
- Request rate monitoring
- Latency percentiles (p95)
- Error rate tracking
- Automatic refresh every 10 seconds

### Changing Grafana Credentials

To change the default Grafana admin credentials, update the environment variables in `docker-compose.yml`:
```yaml
environment:
  - GF_SECURITY_ADMIN_USER=your_username
  - GF_SECURITY_ADMIN_PASSWORD=your_password
```

---

## Step 9: Drift Monitoring and Retraining

### Prerequisites

- Steps 3 and 5 must be completed (aggregated data and trained model)

### Drift Detection

Run drift report to detect feature distribution changes:
```bash
python scripts/drift_report.py
```

The drift report:
- Computes PSI (Population Stability Index) for key features
- Compares last 24 hours vs previous 7 days baseline
- Features monitored:
  - `alerts_sum_15m`
  - `trip_updates_sum_15m`
  - `vehicle_positions_sum_15m`
- Outputs PSI values per feature (lower is better, < 0.1 is stable)
- Saves JSON report to `data/drift_report.json`

**PSI Interpretation:**
- PSI < 0.1: No significant change
- PSI 0.1-0.25: Moderate change
- PSI > 0.25: Significant change (may indicate data drift)

### Weekly Retraining

Run automated retraining pipeline:
```bash
python scripts/retrain_weekly.py
```

The retraining process:
1. **Builds dataset**: Runs `build_dataset.py` to create fresh training data
2. **Trains models**: Runs `train.py` to train new LogisticRegression and LightGBM models
3. **Compares performance**: Compares new model's test F1 score with current model
4. **Model replacement**: Replaces `models/best_model.pkl` only if:
   - New model test F1 > current model test F1 + 0.01 threshold
   - Or no previous model exists

**Output:**
- Console logs showing comparison and decision
- Metrics saved to `models/last_metrics.json`
- Model backup created as `models/best_model.pkl.backup` before training

### Scheduling

For production, schedule these scripts using cron or a task scheduler:
- **Drift report**: Run daily
- **Retraining**: Run weekly (e.g., Sunday nights)

Example cron entries:
```bash
# Daily drift report at 2 AM
0 2 * * * cd /path/to/project && python scripts/drift_report.py

# Weekly retraining on Sundays at 3 AM
0 3 * * 0 cd /path/to/project && python scripts/retrain_weekly.py
```

