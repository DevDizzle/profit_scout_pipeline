# infrastructure/terraform/bigquery.tf

# --- Filing Metadata Table (New) ---
resource "google_bigquery_table" "filing_metadata" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_metadata_table_id

  deletion_protection = false

  # Schema for storing filing metadata
  schema = <<SCHEMA
[
  {"name": "Ticker",             "type": "STRING", "mode": "NULLABLE"},
  {"name": "ReportEndDate",      "type": "DATE",   "mode": "NULLABLE"},
  {"name": "FiledDate",          "type": "DATE",   "mode": "NULLABLE"},
  {"name": "FormType",           "type": "STRING", "mode": "NULLABLE"},
  {"name": "AccessionNumber",    "type": "STRING", "mode": "NULLABLE"},
  {"name": "LinkToFilingDetails","type": "STRING", "mode": "NULLABLE"}
]
SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}

# --- Price Data Table (Existing) ---
# NOTE: This table already exists in BigQuery. Import it with:
# terraform import google_bigquery_table.price_data <project_id>/<dataset_id>/<table_id>
resource "google_bigquery_table" "price_data" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_price_table_id

  # Add partitioning or clustering here if present in your original table

  # Schema for historical price data
  schema = <<SCHEMA
[
  {"name": "ticker",   "type": "STRING",    "mode": "NULLABLE"},
  {"name": "date",     "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "open",     "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "high",     "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "low",      "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "adj_close","type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "volume",   "type": "INTEGER",   "mode": "NULLABLE"}
]
SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}

# --- Financial Ratios Table (New) ---
resource "google_bigquery_table" "financial_ratios" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_ratios_table_id

  deletion_protection = false

  # Schema for calculated financial ratios
  schema = <<SCHEMA
[
  {"name": "ticker",             "type": "STRING",    "mode": "REQUIRED"},
  {"name": "accession_number",   "type": "STRING",    "mode": "REQUIRED"},
  {"name": "report_end_date",    "type": "DATE",      "mode": "REQUIRED"},
  {"name": "filed_date",         "type": "DATE",      "mode": "NULLABLE"},
  {"name": "debt_to_equity",     "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "fcf_yield",          "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "current_ratio",      "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "roe",                "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "gross_margin",       "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "operating_margin",   "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "quick_ratio",        "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "eps",                "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "eps_change",         "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "revenue_growth",     "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "price_trend_ratio",  "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "shares_outstanding", "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "data_source",        "type": "STRING",    "mode": "NULLABLE"},
  {"name": "created_at",         "type": "TIMESTAMP", "mode": "REQUIRED"}
]
SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}

# --- Balance Sheet Table (Existing) ---
# NOTE: This table already exists in BigQuery. Import it with:
# terraform import google_bigquery_table.balance_sheet <project_id>/<dataset_id>/<table_id>
resource "google_bigquery_table" "balance_sheet" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_bs_table_id

  # Add partitioning or clustering here if present in your original table

  # Schema for balance sheet data
 # Schema for balance sheet data
schema = <<SCHEMA
[
  {"name": "ticker", "type": "STRING", "mode": "NULLABLE"},
  {"name": "period_end_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "accounts_payable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "accounts_receivable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "accrued_interest_receivable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "accumulated_depreciation", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "additional_paid_in_capital", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "allowance_for_doubtful_accounts_receivable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "assets_held_for_sale_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "available_for_sale_securities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "buildings_and_improvements", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "capital_lease_obligations", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "capital_stock", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cash_and_cash_equivalents", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cash_cash_equivalents_and_federal_funds_sold", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cash_cash_equivalents_and_short_term_investments", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cash_equivalents", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cash_financial", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "commercial_paper", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "common_stock", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "common_stock_equity", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "construction_in_progress", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_accrued_expenses", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_capital_lease_obligation", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_debt", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_debt_and_capital_lease_obligation", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_deferred_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_deferred_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_deferred_revenue", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_deferred_taxes_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_deferred_taxes_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_notes_payable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "current_provisions", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "defined_pension_benefit", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "derivative_product_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "dividends_payable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "duefrom_related_parties_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "duefrom_related_parties_non_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "dueto_related_parties_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "dueto_related_parties_non_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "employee_benefits", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "financial_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "financial_assets_designatedas_fair_value_through_profitor_loss_total", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "finished_goods", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "foreign_currency_translation_adjustments", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "gains_losses_not_affecting_retained_earnings", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "general_partnership_capital", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "goodwill", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "goodwill_and_other_intangible_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "gross_accounts_receivable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "gross_ppe", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "hedging_assets_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "held_to_maturity_securities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "income_tax_payable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "interest_payable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "inventories_adjustments_allowances", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "inventory", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "invested_capital", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "investment_properties", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "investmentin_financial_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "investments_and_advances", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "investments_in_other_ventures_under_equity_method", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "investmentsin_associatesat_cost", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "investmentsin_joint_venturesat_cost", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "investmentsin_subsidiariesat_cost", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "land_and_improvements", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "leases", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "liabilities_heldfor_sale_non_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "limited_partnership_capital", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "line_of_credit", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "loans_receivable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "long_term_capital_lease_obligation", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "long_term_debt", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "long_term_debt_and_capital_lease_obligation", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "long_term_equity_investment", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "long_term_provisions", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "machinery_furniture_equipment", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "minimum_pension_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "minority_interest", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_debt", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_ppe", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_tangible_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_accounts_receivable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_accrued_expenses", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_deferred_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_deferred_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_deferred_revenue", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_deferred_taxes_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_deferred_taxes_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_note_receivables", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_pension_and_other_postretirement_benefit_plans", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "non_current_prepaid_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "notes_receivable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "ordinary_shares_number", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_current_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_current_borrowings", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_current_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_equity_adjustments", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_equity_interest", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_intangible_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_inventories", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_investments", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_non_current_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_non_current_liabilities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_payable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_properties", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_receivables", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_short_term_investments", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "payables", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "payables_and_accrued_expenses", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "pensionand_other_post_retirement_benefit_plans_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "preferred_securities_outside_stock_equity", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "preferred_shares_number", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "preferred_stock", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "preferred_stock_equity", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "prepaid_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "properties", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "raw_materials", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "receivables", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "receivables_adjustments_allowances", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "restricted_cash", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "restricted_common_stock", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "retained_earnings", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "share_issued", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "stockholders_equity", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "tangible_book_value", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "taxes_receivable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_capitalization", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_debt", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_equity_gross_minority_interest", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_liabilities_net_minority_interest", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_non_current_assets", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_non_current_liabilities_net_minority_interest", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_partnership_capital", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_tax_payable", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "tradeand_other_payables_non_current", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "trading_securities", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "treasury_shares_number", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "treasury_stock", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "unrealized_gain_loss", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "work_in_process", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "working_capital", "type": "FLOAT64", "mode": "NULLABLE"}
  ]
  
  SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}
# --- Cash Flow Table (Existing) ---
# NOTE: This table already exists in BigQuery. Import it with:
# terraform import google_bigquery_table.cash_flow <project_id>/<dataset_id>/<table_id>
resource "google_bigquery_table" "cash_flow" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_cf_table_id

  # Schema for cash flow data
  schema = <<SCHEMA
[
  {"name": "ticker",    "type": "STRING",    "mode": "NULLABLE"},
  {"name": "period_end_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "amortization_cash_flow",                  "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "amortization_of_intangibles",              "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "amortization_of_securities",               "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "asset_impairment_charge",                  "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "beginning_cash_position",                  "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "capital_expenditure",                      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "capital_expenditure_reported",             "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cash_dividends_paid",                      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cash_flow_from_continuing_financing_activities", "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "cash_flow_from_continuing_investing_activities", "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "cash_flow_from_continuing_operating_activities", "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "cash_flow_from_discontinued_operation",     "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cash_from_discontinued_financing_activities", "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "cash_from_discontinued_investing_activities", "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "cash_from_discontinued_operating_activities", "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "change_in_account_payable",                "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_accrued_expense",                "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_dividend_payable",               "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_income_tax_payable",             "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_interest_payable",               "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_inventory",                      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_other_current_assets",           "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_other_current_liabilities",      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_other_working_capital",          "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_payable",                        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_payables_and_accrued_expense",   "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_prepaid_assets",                 "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_receivables",                    "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_tax_payable",                    "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "change_in_working_capital",                "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "changes_in_account_receivables",           "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "changes_in_cash",                          "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "common_stock_dividend_paid",               "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "common_stock_issuance",                    "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "common_stock_payments",                    "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "deferred_income_tax",                      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "deferred_tax",                             "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "depletion",                                "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "depreciation",                             "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "depreciation_amortization_depletion",      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "depreciation_and_amortization",            "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "dividend_paid_cfo",                        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "dividend_received_cfo",                    "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "dividends_received_cfi",                   "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "earnings_losses_from_equity_investments",  "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "effect_of_exchange_rate_changes",          "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "end_cash_position",                        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "excess_tax_benefit_from_stock_based_compensation","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "financing_cash_flow",                      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "free_cash_flow",                           "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "gain_loss_on_investment_securities",       "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "gain_loss_on_sale_of_business",            "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "gain_loss_on_sale_of_ppe",                 "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "income_tax_paid_supplemental_data",         "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "interest_paid_cff",                        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "interest_paid_cfo",                        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "interest_paid_supplemental_data",           "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "interest_received_cfi",                    "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "interest_received_cfo",                    "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "investing_cash_flow",                      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "issuance_of_capital_stock",                "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "issuance_of_debt",                         "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "long_term_debt_issuance",                  "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "long_term_debt_payments",                  "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_business_purchase_and_sale",           "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_common_stock_issuance",                "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_foreign_currency_exchange_gain_loss",  "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_income_from_continuing_operations",    "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_intangibles_purchase_and_sale",        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "net_investment_properties_purchase_and_sale","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_investment_purchase_and_sale",         "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_issuance_payments_of_debt",            "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_long_term_debt_issuance",              "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_other_financing_charges",              "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_other_investing_changes",              "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_ppe_purchase_and_sale",                "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_preferred_stock_issuance",             "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_short_term_debt_issuance",             "type":"FLOAT64","mode":"NULLABLE"},
  {"name": "operating_cash_flow",                      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "operating_gains_losses",                   "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "other_cash_adjustment_inside_changein_cash","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "other_cash_adjustment_outside_changein_cash","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "other_non_cash_items",                     "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "pension_and_employee_benefit_expense",     "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "preferred_stock_dividend_paid",            "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "preferred_stock_issuance",                 "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "preferred_stock_payments",                 "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "proceeds_from_stock_option_exercised",     "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "provisionand_write_offof_assets",          "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "purchase_of_business",                     "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "purchase_of_intangibles",                  "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "purchase_of_investment",                   "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "purchase_of_investment_properties",        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "purchase_of_ppe",                          "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "repayment_of_debt",                        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "repurchase_of_capital_stock",              "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sale_of_business",                         "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sale_of_intangibles",                      "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sale_of_investment",                       "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sale_of_investment_properties",            "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sale_of_ppe",                              "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "short_term_debt_issuance",                 "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "short_term_debt_payments",                 "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "stock_based_compensation",                 "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "taxes_refund_paid",                        "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "unrealized_gain_loss_on_investment_securities","type":"FLOAT64","mode":"NULLABLE"}
]
SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}

# --- Income Statement Table (Existing) ---
# NOTE: This table already exists in BigQuery. Import it with:
# terraform import google_bigquery_table.income_statement <project_id>/<dataset_id>/<table_id>
resource "google_bigquery_table" "income_statement" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  table_id   = var.bq_is_table_id

  # Schema for income statement data
  schema = <<SCHEMA
[
  {"name": "ticker",                             "type": "STRING",  "mode": "NULLABLE"},
  {"name": "period_end_date",                    "type": "TIMESTAMP","mode": "NULLABLE"},
  {"name": "amortization",                       "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "amortization_of_intangibles_income_statement","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "average_dilution_earnings",          "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "basic_average_shares",               "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "basic_eps",                          "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "cost_of_revenue",                    "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "depletion_income_statement",         "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "depreciation_amortization_depletion_income_statement","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "depreciation_and_amortization_in_income_statement","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "depreciation_income_statement",      "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "diluted_average_shares",             "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "diluted_eps",                        "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "diluted_ni_availto_com_stockholders","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "earnings_from_equity_interest",      "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "earnings_from_equity_interest_net_of_tax","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "ebit",                               "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "ebitda",                             "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "excise_taxes",                       "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "gain_on_sale_of_business",           "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "gain_on_sale_of_ppe",                "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "gain_on_sale_of_security",           "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "general_and_administrative_expense", "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "gross_profit",                       "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "impairment_of_capital_assets",       "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "insurance_and_claims",               "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "interest_expense",                   "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "interest_expense_non_operating",     "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "interest_income",                    "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "interest_income_non_operating",      "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "loss_adjustment_expense",            "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "minority_interests",                 "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "net_income",                         "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "net_income_common_stockholders",     "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "net_income_continuous_operations",   "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "net_income_discontinuous_operations","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_income_from_continuing_and_discontinued_operation","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_income_from_continuing_operation_net_minority_interest","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_income_from_tax_loss_carryforward","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_income_including_noncontrolling_interests","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_interest_income",                "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "net_non_operating_interest_income_expense","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "net_policyholder_benefits_and_claims","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "normalized_ebitda",                  "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "normalized_income",                  "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "occupancy_and_equipment",            "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "operating_expense",                  "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "operating_income",                   "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "operating_revenue",                  "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "other_gand_a",                       "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "other_income_expense",               "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "other_non_interest_expense",         "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "other_non_operating_income_expenses","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "other_operating_expenses",           "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "other_special_charges",              "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "other_taxes",                        "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "otherunder_preferred_stock_dividend","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "policyholder_benefits_ceded",        "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "policyholder_benefits_gross",        "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "preferred_stock_dividends",          "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "pretax_income",                      "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "professional_expense_and_contract_services_expense","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "provision_for_doubtful_accounts",    "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "reconciled_cost_of_revenue",         "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "reconciled_depreciation",            "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "rent_and_landing_fees",              "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "rent_expense_supplemental",          "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "research_and_development",           "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "restructuring_and_mergern_acquisition","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "salaries_and_wages",                 "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "securities_amortization",            "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "selling_and_marketing_expense",      "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "selling_general_and_administration", "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "special_income_charges",             "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "tax_effect_of_unusual_items",        "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "tax_provision",                      "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "tax_rate_for_calcs",                 "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "total_expenses",                     "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "total_operating_income_as_reported", "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "total_other_finance_cost",           "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "total_revenue",                      "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "total_unusual_items",                "type": "FLOAT64",  "mode": "NULLABLE"},
  {"name": "total_unusual_items_excluding_goodwill","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "weighted_average_number_of_diluted_shares_outstanding","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "weighted_average_number_of_shares_outstanding_basic","type":"FLOAT64","mode":"NULLABLE"},
  {"name": "write_off",                          "type": "FLOAT64",  "mode": "NULLABLE"}
]
SCHEMA

  depends_on = [google_bigquery_dataset.profit_scout_dataset]
}
