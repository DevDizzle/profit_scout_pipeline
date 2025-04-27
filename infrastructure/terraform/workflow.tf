# workflow.tf

resource "google_workflows_workflow" "main_workflow" {
  project         = var.gcp_project_id
  name            = var.workflow_name
  region          = var.workflow_region
  description     = "Orchestrates the daily SEC filing processing pipeline."
  service_account = google_service_account.workflow_sa.email

  # Workflow definition in YAML. Expressions are escaped ($$).
  # Simple expressions (variable access, basic functions) used for assignments, conditions, loops, or simple args are NOT quoted.
  # Complex expressions (internal strings, concatenation, ternary) used for env vars or complex args ARE quoted ('$${{...}}').
  source_contents = <<-YAML
main:
  params: [project_id, location, fetch_job_id, download_job_id, qual_analysis_job_id, headline_job_id, event]
  steps:
  - init:
      assign:
        - new_filings: []
        - all_errors: []
      next: callFetchFilingsJob

  - callFetchFilingsJob:
      try:
        call: googleapis.run.v2.projects.locations.jobs.run
        args:
          name: $${{fetch_job_id}}
        result: fetch_result
      except:
        assign:
          - fetch_job_error: $${{last_error}}
        next: recordErrorAndFail
      next: decodeFetchResult

  - decodeFetchResult:
      try:
        assign:
          - decoded_output: $${{json.decode(fetch_result.body)}}
          - new_filings: $${{decoded_output.new_filing_details}}
          - processed_count: $${{decoded_output.newly_processed_count}}
      except:
        assign:
          - decode_error: "Failed to decode fetch job output."
        next: recordErrorAndFail
      next: checkNewFilingsList

  - checkNewFilingsList:
      switch:
        - condition: $${{len(new_filings) > 0}}
          next: processFilingsLoop
      next: noNewFilings

  - noNewFilings:
      call: sys.log
      args:
        text: "No new filings found. Workflow finished successfully."
        severity: INFO
      next: returnSuccess

  - processFilingsLoop:
      for:
        value: current_filing
        in: $${{new_filings}}
        steps:
          - callDownloadPDF:
              try:
                call: googleapis.run.v2.projects.locations.jobs.run
                args:
                  name: $${{download_job_id}}
                  overrides:
                    containerOverrides:
                      - env:
                          - name: INPUT_TICKER
                            value: $${{current_filing.Ticker}}
                          - name: INPUT_ACCESSION_NUMBER
                            value: $${{current_filing.AccessionNumber}}
                          - name: INPUT_FILING_URL
                            value: $${{current_filing.LinkToFilingDetails}}
                result: download_result
              except:
                assign:
                  - all_errors: $${{list.concat(all_errors, ["Download failed for " + current_filing.Ticker])}}
                next: endIteration
              next: decodeDownloadResult

          - decodeDownloadResult:
              try:
                assign:
                  - decoded_download: $${{json.decode(download_result.body)}}
                  - pdf_gcs_path: $${{decoded_download.output_gcs_path}}
              except:
                assign:
                  - all_errors: $${{list.concat(all_errors, ["Decode failed for " + current_filing.Ticker])}}
                next: endIteration
              next: checkPdfPath

          - checkPdfPath:
              switch:
                - condition: $${{pdf_gcs_path != null}}
                  next: parallelAnalysisSteps
              assign:
                - all_errors: $${{list.concat(all_errors, ["Missing PDF for " + current_filing.Ticker])}}
              next: endIteration

          - parallelAnalysisSteps:
              parallel:
                shared: [current_filing, pdf_gcs_path, qual_analysis_job_id, headline_job_id]
                branches:
                  - qualitative_branch:
                      steps:
                        - callQualitativeAnalysis:
                            call: googleapis.run.v2.projects.locations.jobs.run
                            args:
                              name: $${{qual_analysis_job_id}}
                              overrides:
                                containerOverrides:
                                  - env:
                                      - name: INPUT_TICKER
                                        value: $${{current_filing.Ticker}}
                                      - name: INPUT_ACCESSION_NUMBER
                                        value: $${{current_filing.AccessionNumber}}
                                      - name: INPUT_FILING_DATE
                                        value: $${{current_filing.FiledDate}}
                                      - name: INPUT_FORM_TYPE
                                        value: $${{current_filing.FormType}}
                                      - name: INPUT_PDF_GCS_PATH
                                        value: $${{pdf_gcs_path}}

                  - headline_branch:
                      steps:
                        - callHeadlineAssessment:
                            call: googleapis.run.v2.projects.locations.jobs.run
                            args:
                              name: $${{headline_job_id}}
                              overrides:
                                containerOverrides:
                                  - env:
                                      - name: INPUT_TICKER
                                        value: $${{current_filing.Ticker}}
                                      - name: INPUT_FILING_DATE
                                        value: $${{current_filing.FiledDate}}
                                      - name: INPUT_COMPANY_NAME
                                        value: '$${{"CompanyName" in current_filing ? current_filing.CompanyName : current_filing.Ticker}}'
              next: endIteration

          - endIteration:
              assign:
                - loop_iteration_complete: true
      next: logCompletion

  - recordErrorAndFail:
      assign:
        - all_errors: $${{list.concat(all_errors, ["Critical error in workflow."])}}
      next: logCompletion

  - logCompletion:
      call: sys.log
      args:
        # Complex expressions for args seem to require quoting
        text: '$${{"Workflow completed. Errors: " + string(len(all_errors))}}'
        severity: '$${{len(all_errors) > 0 ? "WARNING" : "INFO"}}' # <<< FIX: Added quotes here
      next: decideResult

  - decideResult:
      switch:
        - condition: $${{len(all_errors) > 0}}
          next: returnFailure
      next: returnSuccess

  - returnFailure:
      return: "Failure - See Logs"

  - returnSuccess:
      return: "Success"
YAML

  depends_on = [
    google_service_account.workflow_sa,
    google_cloud_run_v2_job.fetch_filings_job,
    google_cloud_run_v2_job.download_pdf_job,
    google_cloud_run_v2_job.qualitative_analysis_job,
    google_cloud_run_v2_job.headline_assessment_job,
    google_service_account_iam_member.workflow_act_as_job_sas
  ]
}
