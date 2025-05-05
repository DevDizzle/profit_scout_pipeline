# Profit Scout - Financial Data Pipeline

## What is Profit Scout?

Profit Scout is a fintech agent designed to deliver deep insights into companies by collecting and analyzing financial data. This repository covers the **data pipeline**, which gathers public financial reports, stock prices, and news, then processes them into actionable insights. It’s one of three parts of the Profit Scout project, alongside a machine learning pipeline and a web application (with frontend and backend), each in their own repositories.

The data pipeline’s main job is to prepare and store data for two key uses: feeding our ML model for predictions and powering our financial bot’s analysis with accurate, real-world data.

## What Does the Data Pipeline Do?

Every day, the data pipeline:

1. **Checks for new SEC filings**: Finds new 10-Ks or 10-Qs, saves their details, and downloads the PDFs.
2. **Analyzes filings**: Reads the PDFs and creates summaries about the company’s performance using AI (Gemini).
3. **Looks at recent news**: Uses AI to summarize news about the company from the past two weeks.
4. **Tracks stock prices**: Pulls daily stock prices for companies we’re watching.
5. **Calculates financial metrics**: Uses the filings and prices to compute key ratios, like profitability or debt levels.
6. **Stores everything**: Saves all data (PDFs, summaries, prices, ratios) in Google Cloud for the ML model and financial bot to use.

## How It Works

The pipeline runs as a series of small, focused tasks (called "jobs") on Google Cloud. These jobs are like workers on an assembly line, each handling one part of the process. Here’s the flow:

- **Daily Filing Check (6 AM, Mon-Fri)**: A schedule kicks off a workflow that:
  1. Checks the SEC for new filings and logs their details in BigQuery.
  2. Downloads PDFs of new filings and stores them in Google Cloud Storage.
  3. Summarizes the PDFs and recent news using AI, saving the results as text files.
- **Daily Stock Prices (4 PM, Mon-Fri)**: A separate job grabs stock prices and adds them to BigQuery.
- **Daily Metrics (4:30 PM, Mon-Fri)**: Another job calculates financial ratios for new filings and saves them in BigQuery.

The jobs are managed by Google Cloud tools like Cloud Run, Cloud Workflow, and Cloud Scheduler to keep everything running smoothly.

## Project Structure

The code is organized to keep things simple:

```
profit_scout_pipeline/
├── jobs/                   # Code for each task (e.g., fetching filings, downloading PDFs)
├── infrastructure/         # Setup files for Google Cloud (using Terraform)
├── cicd/                   # Instructions for building and deploying the code
├── tests/                  # Tests to make sure everything works
├── README.md               # This file
└── .env.example            # Template for local settings
```
To get the data pipeline running, you’ll need:

- A Google Cloud project with APIs enabled (like BigQuery, Cloud Run, Vertex AI).
- An SEC API key (for fetching filings).
- A Gemini API key (for AI summaries and calculations).
- Python 3.11+ and the Google Cloud CLI for local development.
- Terraform to set up Google Cloud resources.

### Steps:

1. Clone this repository and install dependencies for each job (listed in `requirements.txt`).
2. Set up your Google Cloud project and enable the required APIs.
3. Store your API keys in Google Secret Manager.
4. Use Terraform (in `infrastructure/terraform/`) to create the necessary Google Cloud resources.
5. Deploy the jobs using Cloud Build (in `cicd/cloudbuild.yaml`).

For local testing, copy `.env.example` to `.env`, fill in your settings, and run individual jobs with Python.

## Why It’s Useful

The data pipeline automates the heavy lifting of financial analysis. It collects raw data (filings, prices), generates insights (summaries, ratios), and stores everything in Google Cloud. This data powers our ML model’s predictions and equips our financial bot with tools for accurate, grounded analysis, making it easier to understand companies and make informed decisions.

