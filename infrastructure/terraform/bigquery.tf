# infrastructure/terraform/bigquery.tf

# --- Filing Metadata Table ---
resource "google_bigquery_table" "filing_metadata" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_metadata_table_id

  deletion_protection = false

  # Schema for storing filing metadata
  schema = <<SCHEMA
[
  {"name": "Ticker",             "type": "STRING", "mode": "NULLABLE", "description": "Stock ticker symbol"},
  {"name": "ReportEndDate",      "type": "DATE",   "mode": "NULLABLE", "description": "End date of the reporting period"},
  {"name": "FiledDate",          "type": "DATE",   "mode": "NULLABLE", "description": "Date the filing was submitted to the SEC"},
  {"name": "FormType",           "type": "STRING", "mode": "NULLABLE", "description": "Type of SEC form (e.g., 10-K, 10-Q)"},
  {"name": "AccessionNumber",    "type": "STRING", "mode": "NULLABLE", "description": "Unique identifier for the filing"},
  {"name": "LinkToFilingDetails","type": "STRING", "mode": "NULLABLE", "description": "URL to the full filing details"},
  {"name": "SIC",                "type": "STRING", "mode": "NULLABLE", "description": "Standard Industrial Classification code"},
  {"name": "Sector",             "type": "STRING", "mode": "NULLABLE", "description": "Human-readable sector based on SIC code"},
  {"name": "Industry",           "type": "STRING", "mode": "NULLABLE", "description": "Human-readable industry based on SIC code"},
  {"name": "CompanyName",        "type": "STRING", "mode": "NULLABLE", "description": "Full legal name of the company"},
  {"name": "CIK",                "type": "STRING", "mode": "NULLABLE", "description": "Central Index Key for the company"},
  {"name": "PrimaryExchange",    "type": "STRING", "mode": "NULLABLE", "description": "Primary stock exchange (e.g., NYSE, NASDAQ)"},
  {"name": "IncomeStatementURI", "type": "STRING", "mode": "NULLABLE", "description": "GCS URI for the income statement CSV file"},
  {"name": "BalanceSheetURI",    "type": "STRING", "mode": "NULLABLE", "description": "GCS URI for the balance sheet CSV file"},
  {"name": "CashFlowURI",        "type": "STRING", "mode": "NULLABLE", "description": "GCS URI for the cash flow CSV file"}
]
SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}

# --- Price Data Table ---
resource "google_bigquery_table" "price_data" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_price_table_id

  deletion_protection = false

  # Schema for historical price data
  schema = <<SCHEMA
[
  {"name": "ticker",   "type": "STRING",    "mode": "NULLABLE", "description": "Stock ticker symbol"},
  {"name": "date",     "type": "TIMESTAMP", "mode": "NULLABLE", "description": "Date and time of the price data"},
  {"name": "open",     "type": "FLOAT64",   "mode": "NULLABLE", "description": "Opening price of the stock for the period"},
  {"name": "high",     "type": "FLOAT64",   "mode": "NULLABLE", "description": "Highest price of the stock during the period"},
  {"name": "low",      "type": "FLOAT64",   "mode": "NULLABLE", "description": "Lowest price of the stock during the period"},
  {"name": "adj_close","type": "FLOAT64",   "mode": "NULLABLE", "description": "Adjusted closing price accounting for splits and dividends"},
  {"name": "volume",   "type": "INTEGER",   "mode": "NULLABLE", "description": "Trading volume for the period"}
]
SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}

# --- Financial Ratios Table ---
resource "google_bigquery_table" "financial_ratios" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_ratios_table_id

  deletion_protection = false

  # Schema for calculated financial ratios
  schema = <<SCHEMA
[
  {"name": "ticker",             "type": "STRING",    "mode": "REQUIRED", "description": "Stock ticker symbol"},
  {"name": "accession_number",   "type": "STRING",    "mode": "REQUIRED", "description": "Unique identifier for the SEC filing"},
  {"name": "report_end_date",    "type": "DATE",      "mode": "REQUIRED", "description": "End date of the reporting period"},
  {"name": "filed_date",         "type": "DATE",      "mode": "NULLABLE", "description": "Date the filing was submitted to the SEC"},
  {"name": "debt_to_equity",     "type": "FLOAT64",   "mode": "NULLABLE", "description": "Debt-to-equity ratio, measuring financial leverage"},
  {"name": "fcf_yield",          "type": "FLOAT64",   "mode": "NULLABLE", "description": "Free cash flow yield, indicating cash generation relative to market cap"},
  {"name": "current_ratio",      "type": "FLOAT64",   "mode": "NULLABLE", "description": "Current ratio, measuring short-term liquidity"},
  {"name": "roe",                "type": "FLOAT64",   "mode": "NULLABLE", "description": "Return on equity, measuring profitability relative to shareholders' equity"},
  {"name": "gross_margin",       "type": "FLOAT64",   "mode": "NULLABLE", "description": "Gross margin, indicating profitability after cost of goods sold"},
  {"name": "operating_margin",   "type": "FLOAT64",   "mode": "NULLABLE", "description": "Operating margin, indicating profitability from core operations"},
  {"name": "quick_ratio",        "type": "FLOAT64",   "mode": "NULLABLE", "description": "Quick ratio, measuring immediate liquidity excluding inventory"},
  {"name": "eps",                "type": "FLOAT64",   "mode": "NULLABLE", "description": "Earnings per share, indicating per-share profitability"},
  {"name": "eps_change",         "type": "FLOAT64",   "mode": "NULLABLE", "description": "Percentage change in earnings per share over a period"},
  {"name": "revenue_growth",     "type": "FLOAT64",   "mode": "NULLABLE", "description": "Percentage growth in revenue over a period"},
  {"name": "price_trend_ratio",  "type": "FLOAT64",   "mode": "NULLABLE", "description": "Ratio indicating stock price trend relative to financial metrics"},
  {"name": "shares_outstanding", "type": "FLOAT64",   "mode": "NULLABLE", "description": "Number of shares outstanding for the company"},
  {"name": "data_source",        "type": "STRING",    "mode": "NULLABLE", "description": "Source of the financial data (e.g., SEC filing, calculated)"},
  {"name": "created_at",         "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Timestamp when the record was created"}
]
SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}