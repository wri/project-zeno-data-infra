
# Project Zeno Data Infrastructure - API

API service for Project Zeno's data infrastructure.

## Prerequisites

- **Python 3.12+**
- **uv** - Python package manager ([installation guide](https://docs.astral.sh/uv/getting-started/installation/))
- **Docker & Docker Compose**

## Development Setup

### 1. Install Dependencies

```bash
cd api
uv sync
```

### 2. Activate Virtual Environment

```bash
source .venv/bin/activate
```

### 3. Configure Environment Variables

Set up the following environment variables in your shell or create a `.env` file:

```bash

# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# DATA API
API_KEY=your_gfw_api_key
```
Adjust the values according to your local or deployment environment.

### 4. Running the API

```bash
docker-compose -f docker-compose-api.yml up --build
```

The API will be available at `http://localhost:8000`

API documentation: `http://localhost:8000/docs`

### Results Storage Note

Analysis metadata and status are stored in DynamoDB in the `analyses-dev` table, while analysis results are stored in the corresponding S3 bucket. **If you need to generate new results for an existing analysis, you must first purge the records from DynamoDB and S3.**

## Running Tests

The test suite requires additional environment variables and dependencies.

### 1. Install Test Dependencies

```bash
uv sync --dev
```

```bash
source .venv/bin/activate
```

### 2. Set Test Environment Variables

```bash
# API Keys
export API_KEY=your_gfw_api_key

# AWS Credentials for Tests
export AWS_ACCESS_KEY_ID=your_test_access_key
export AWS_SECRET_ACCESS_KEY=your_test_secret_key

# Test Resources
export ANALYSES_TABLE_NAME=Analyses-test
export ANALYSIS_RESULTS_BUCKET_NAME=gnw-analytics-api-analysis-results-test
```

### 3. Run Tests

```bash
pytest test --disable-warnings
```

Or run with all environment variables inline:

```bash
API_KEY=your_gfw_api_key \
  AWS_ACCESS_KEY_ID=your_test_access_key \
  AWS_SECRET_ACCESS_KEY=your_test_secret_key \
  ANALYSES_TABLE_NAME=Analyses-test \
  ANALYSIS_RESULTS_BUCKET_NAME=gnw-analytics-api-analysis-results-test \
  pytest test --disable-warnings
```