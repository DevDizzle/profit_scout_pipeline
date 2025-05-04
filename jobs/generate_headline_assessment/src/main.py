#!/usr/bin/env python3
# main.py — Generate search-based headline assessments from BigQuery metadata

import os
import time
import logging
import tempfile
import json
import sys
import pkg_resources
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google import genai
from google.genai.types import GenerateContentConfig, Tool, GoogleSearch, HttpOptions

# ——— Configuration ———
PROJECT_ID                  = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME             = os.getenv('GCS_BUCKET_NAME')
GCS_HEADLINE_OUTPUT_FOLDER  = os.getenv('GCS_HEADLINE_OUTPUT_FOLDER', 'headline-analysis/').rstrip('/') + '/'
GEMINI_MODEL_NAME           = os.getenv('GEMINI_MODEL_NAME', 'gemini-2.0-flash')
GEMINI_TEMPERATURE          = float(os.getenv('GEMINI_TEMPERATURE', '0.0'))
GEMINI_MAX_TOKENS           = int(os.getenv('GEMINI_MAX_TOKENS', '8192'))
GEMINI_REQ_TIMEOUT          = int(os.getenv('GEMINI_REQ_TIMEOUT', '300'))
MAX_ASSESSMENTS_TO_GENERATE = int(os.getenv('MAX_ASSESSMENTS_TO_GENERATE', '0'))
BQ_TABLE                    = f"{PROJECT_ID}.profit_scout.filing_metadata"
LOCATION                    = os.getenv('GOOGLE_CLOUD_LOCATION', 'us-central1')

# ——— Logging Setup ———
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("Installed Python packages: %s", [p.project_name for p in pkg_resources.working_set])

# ——— Validate Configuration ———
missing = [n for n,v in [
    ('GCP_PROJECT_ID', PROJECT_ID),
    ('GCS_BUCKET_NAME', GCS_BUCKET_NAME),
    ('GEMINI_MODEL_NAME', GEMINI_MODEL_NAME),
] if not v]
if missing:
    logging.critical(f"Missing env vars: {missing}")
    raise RuntimeError(f"Missing env vars: {missing}")

# ——— Globals ———
gcs_client = None
bq_client = None
genai_client = None

def get_headline_risk_assessment(ticker: str, filed_date: str, lookback_days: int = 30) -> str:
    """Generate a search-grounded risk assessment using Google Search and a lookback from FiledDate."""
    end_date = pd.to_datetime(filed_date)
    start_date = end_date - pd.Timedelta(days=lookback_days)

    prompt = f"""
You are a seasoned financial analyst with a sharp eye for market opportunities, tasked with analyzing recent news for **{ticker}**. Your insights fuel:  
(1) a financial analysis bot that detects impactful developments, and  
(2) an ML model predicting stock price movements over the next 90 days.

Deliver concise, grounded insights that highlight events and factors likely to move the stock price up or down.  
Base all analysis strictly on the provided Google Search results.

---

**Analysis Window**:  
Focus only on news published from **{start_date.date()} to {end_date.date()}**.  
If no relevant news is found in this range, state: "No significant news found in the specified date range."

---

**Instructions**:

1. **Key News Events**  
   * List up to 3 impactful events (e.g., earnings surprises, product launches, regulatory actions).  
   * Format: “[Date]: [Event]; Directional Impact: [Upward, Downward, Neutral].”  
   * Example: “2025-04-15: Earnings beat consensus; Directional Impact: Upward”

2. **Sentiment Analysis**  
   * State overall sentiment: Positive, Neutral, or Negative.  
   * List 1–2 drivers influencing this sentiment (e.g., “Earnings momentum,” “Regulatory uncertainty”).

3. **90-Day Outlook**  
   * One-sentence forecast of stock price direction (upward, downward, or stable) over the next 90 days, based on recent news.

4. **Growth Catalysts**  
   * Up to 2 factors likely to drive the stock price upward in the next 90 days.  
   * Format: “[Catalyst]: [Metric or Detail].”  
   * Example: “Product Launch: New AI platform.”

5. **Potential Challenges**  
   * Up to 2 factors that could push the stock price downward in the next 90 days.  
   * Format: “[Challenge]: [Metric or Detail].”  
   * Example: “Regulatory Probe: Ongoing investigation.”

---

**CRITICAL INSTRUCTIONS**:

- **Output only the analysis.**
- **Do not include introductions, disclaimers, or speculative content.**
- **Ensure all insights are grounded in the provided Google Search results.**
- **Keep the content concise, predictive, and focused on factors that could impact the stock price over the next 90 days.**
- **If no relevant news is found, state: "No significant news found in the specified date range." and skip the other sections.**

---

**Output Format**:
- Begin with: `**Headline Summary Date Range**: {start_date} to {end_date}`
- Use bullet points for all sections.

**Example**:

  * **Headline Summary Date Range**: 2025-04-01 to 2025-04-30  
  * **Key News Events**:  
    * 2025-04-15: Earnings beat consensus; Directional Impact: Upward  
    * 2025-04-20: AI platform launch; Directional Impact: Upward  
    * 2025-04-25: Regulatory probe announced; Directional Impact: Downward  
  * **Sentiment Analysis**:  
    * Sentiment: Positive  
    * Drivers: Earnings momentum, product innovation  
  * **90-Day Outlook**:  
    * Bullish momentum from earnings and product launch, tempered by regulatory concerns.  
  * **Growth Catalysts**:  
    * Product Launch: New AI platform  
    * Analyst Upgrade: Target raised to $75  
  * **Potential Challenges**:  
    * Regulatory Probe: Ongoing investigation  
    * Supply Chain: Potential component delays

**If no news is found**:

  * **Headline Summary Date Range**: 2025-04-01 to 2025-04-30  
  * No significant news found in the specified date range.
"""

    try:
        logging.info(f"Generating assessment for {ticker}…")
        response = genai_client.models.generate_content(
            model=GEMINI_MODEL_NAME,
            contents=prompt,
            config=GenerateContentConfig(
                tools=[Tool(google_search=GoogleSearch())],
                temperature=GEMINI_TEMPERATURE,
                max_output_tokens=GEMINI_MAX_TOKENS
            )
        )
        return response.text or "Received empty response"
    except Exception as e:
        logging.error(f"Error generating assessment for {ticker}: {e}", exc_info=True)
        return f"Error: {e}"

def main():
    global gcs_client, bq_client, genai_client
    start = time.time()
    processed = skipped = failed = 0

    # Initialize clients
    bq_client = bigquery.Client(project=PROJECT_ID)
    gcs_client = storage.Client(project=PROJECT_ID)
    genai_client = genai.Client(
        http_options=HttpOptions(api_version="v1"),
        vertexai=True,
        project=PROJECT_ID,
        location=LOCATION
    )

    # Fetch filings
    try:
        df = bq_client.query(f"""
            SELECT Ticker, AccessionNumber, FiledDate
            FROM `{BQ_TABLE}`
            WHERE FormType IN ('10-K','10-Q')
        """).to_dataframe()
    except Exception as e:
        logging.critical(f"Failed to query BigQuery: {e}", exc_info=True)
        sys.exit(1)

    total = len(df)
    logging.info(f"Found {total} filings.")

    # List existing assessments
    bucket = gcs_client.bucket(GCS_BUCKET_NAME)
    existing = {
        blob.name.split('/')[-1].rsplit('.',1)[0]
        for blob in bucket.list_blobs(prefix=GCS_HEADLINE_OUTPUT_FOLDER)
        if blob.name.lower().endswith('.txt')
    }
    logging.info(f"{len(existing)} existing assessments.")

    # Determine worklist
    to_process = []
    for _, row in df.iterrows():
        t = row['Ticker']
        acc = row['AccessionNumber'].replace('-', '')
        key = f"{t}_{acc}"
        if key in existing:
            skipped += 1
        else:
            to_process.append((t, acc, row['FiledDate']))

    logging.info(f"{len(to_process)} to process (skipped {skipped}).")

    if MAX_ASSESSMENTS_TO_GENERATE > 0:
        to_process = to_process[:MAX_ASSESSMENTS_TO_GENERATE]

    tmp_dir = tempfile.mkdtemp()
    for idx, (t, acc, filed_date) in enumerate(to_process, 1):
        base = f"{t}_{acc}"
        local_path = os.path.join(tmp_dir, f"{base}.txt")
        try:
            txt = get_headline_risk_assessment(t, filed_date=filed_date, lookback_days=30)
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(txt)
            blob = bucket.blob(f"{GCS_HEADLINE_OUTPUT_FOLDER}{base}.txt")
            blob.upload_from_filename(local_path)
            processed += 1
            logging.info(f"[{idx}/{len(to_process)}] Uploaded {base}.txt")
        except Exception as e:
            failed += 1
            logging.error(f"Failed {base}: {e}", exc_info=True)
        finally:
            try: os.remove(local_path)
            except: pass

    # Cleanup
    try: os.rmdir(tmp_dir)
    except: pass

    duration = time.time() - start
    logging.info(f"Done in {duration:.1f}s — processed {processed}, failed {failed}")
    print(json.dumps({
        "status":        "completed_with_errors" if failed else "completed",
        "total":         total,
        "processed":     processed,
        "skipped":       skipped,
        "failed":        failed,
        "duration_sec":  round(duration,2)
    }))
    sys.exit(0)

if __name__ == "__main__":
    main()