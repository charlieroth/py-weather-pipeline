# `py-weather-pipeline`

A simple ETL pipeline for learning purposes. The pipeline ingests weather data
from a CSV file (bulk historical data for a single location), transforms the
weather data for feature engineering purposes, loads the data into a
PostgreSQL database.

## Setup

1. Clone the repository

```bash
git clone https://github.com/charlieroth/py-weather-pipeline.git
```

2. Create UV environment

```bash
uv sync
```

3. Create a `.env` file and update the environment variables appropriately
