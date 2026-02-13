# MarketPrediction


This project connects to Polymarket’s public APIs to discover active tennis prediction markets, subscribe to their live order book (CLOB) feed, and record real-time market data with latency measurements.



## What This Project Does

* Searches Polymarket for active tennis events

* Automatically selects a CLOB-enabled market

* Extracts outcome token IDs for the match

* Subscribes to Polymarket’s WebSocket order book feed

* Computes: Best bid, Best ask, Mid price

* Logs all updates to CSV with timestamps and latency metrics


## Data Collected

Each row in the CSV represents a single order book update for one outcome token.

### CSV Columns

* utc_iso:         UTC timestamp when data was processed

* slug:            Polymarket event slug

* question:        Market question (match description)

* asset_id:        Outcome token ID

* event_type:      WebSocket event type

* best_bid:        Highest bid price

* best_ask:        Lowest ask price

* mid:             (best_bid + best_ask) / 2

* exch_ts_raw:     Exchange-provided timestamp

* recv_ts_ns:      Local receive timestamp

* proc_end_ts_ns:  Processing completion timestamp

* proc_latency_ms: Local processing latency

* net_latency_ms:  Best-effort network latency

* e2e_latency_ms:  Best-effort end-to-end latency

> [!NOTE]
> Local processing latency is precise.
> Best-effort network latency and Best-effort end-to-end latency are best-effort estimates based on the WebSocket timestamp field.



## Requirements

* Python 3.9+

* No authentication required

* Uses only public Polymarket endpoints



## Disclaimer

> [!IMPORTANT]
> This project is for data collection and analysis only.
> It does not place trades or interact with private endpoints.



## How To Run

### 1. Clone the repo
```
git clone https://github.com/ErkutGurcenan/MarketPrediction.git
cd MarketPrediction
```

### 2. Install dependencies
```
pip install -r requirements.txt
```

### 3. Option 1: Auto-select an active tennis market
```
python3 src/main.py --auto --out data
```

This will:
* Search Polymarket for active tennis events
* Pick the first CLOB-enabled market
* Start streaming order book data

### 3. Option 2: Track a specific match by player name
```
python3 src/main.py --auto --query "Ben Shelton" --out data
```

## Output

CSV files are written to:
```
data/polymarket_tennis.csv
```



## Possible Extensions

* Visualization dashboard (e.g. matplotlib / Plotly)

* Event-based price reaction analysis

* Multi-market monitoring

