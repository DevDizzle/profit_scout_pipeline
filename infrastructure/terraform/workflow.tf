# workflow.tf

resource "google_workflows_workflow" "main_workflow" {
  project         = var.gcp_project_id
  name            = var.workflow_name
  region          = var.workflow_region
  description     = "Orchestrates the daily SEC filing processing pipeline."
  service_account = google_service_account.workflow_sa.email

  # Workflow definition in YAML. Expressions use $${} to escape Terraform interpolation.
  # Simple expressions (variable access, basic functions) are NOT quoted.
  # Complex expressions (concatenation, etc.) used for args ARE quoted ('$${...}').
  source_contents = <<-YAML
main:
  params: [input]
  steps:
  - init:
      assign:
        - new_filings: []
        - all_errors: []
      next: initFetchJob

  - initFetchJob:
      assign:
        - fetch_job_failed: false
      next: callFetchFilingsJob

  - callFetchFilingsJob:
      try:
        call: googleapis.run.v2.projects.locations.jobs.run
        args:
          name: $${input.fetch_job_id}
        result: fetch_result
      except:
        as: error
        assign:
          - fetch_job_error: $${error}
          - fetch_job_failed: true
      next: checkFetchJobResult

  - checkFetchJobResult:
      switch:
        - condition: $${fetch_job_failed}
          next: recordErrorAndFail
      next: decodeFetchResult

  - decodeFetchResult:
      assign:
        - decoded_output: $${json.decode(fetch_result.body)}
        - new_filings: $${decoded_output.new_filing_details}
        - processed_count: $${decoded_output.newly_processed_count}
      next: checkNewFilingsList

  - checkNewFilingsList:
      switch:
        - condition: $${len(new_filings) > 0}
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
        in: $${new_filings}
        steps:
          - initDownload:
              assign:
                - download_failed: false
              next: callDownloadPDF

          - callDownloadPDF:
              try:
                call: googleapis.run.v2.projects.locations.jobs.run
                args:
                  name: $${input.download_job_id}
                  body:
                    overrides:
                      containerOverrides:
                        - env:
                            - name: INPUT_TICKER
                              value: $${current_filing.Ticker}
                            - name: INPUT_ACCESSION_NUMBER
                              value: $${current_filing.AccessionNumber}
                            - name: INPUT_FILING_URL
                              value: $${current_filing.LinkToFilingDetails}
                result: download_result
              except:
                assign:
                  - all_errors: $${list.concat(all_errors, ["Download failed for " + current_filing.Ticker])}
                  - download_failed: true
              next: checkDownloadResult

          - checkDownloadResult:
              switch:
                - condition: $${download_failed}
                  next: endIteration
              next: decodeDownloadResult

          - decodeDownloadResult:
              assign:
                - decoded_download: $${json.decode(download_result.body)}
                - pdf_gcs_path: $${decoded_download.output_gcs_path}
              next: checkPdfPath

          - checkPdfPath:
              switch:
                - condition: $${pdf_gcs_path != null}
                  next: setCompanyName
                - condition: true
                  assign:
                    - all_errors: $${list.concat(all_errors, ["Missing PDF for " + current_filing.Ticker])}
                  next: endIteration

          - setCompanyName:
              switch:
                - condition: $${"CompanyName" in current_filing}
                  assign:
                    - company_name: $${current_filing.CompanyName}
                - condition: true
                  assign:
                    - company_name: $${current_filing.Ticker}
              next: initParallel

          - initParallel:
              assign:
                - qual_job_id: $${input.qual_analysis_job_id}
                - headline_job_id: $${input.headline_job_id}
              next: parallelAnalysisSteps

          - parallelAnalysisSteps:
              parallel:
                shared: [current_filing, pdf_gcs_path, qual_job_id, headline_job_id, company_name]
                branches:
                  - qualitative_branch:
                      steps:
                        - callQualitativeAnalysis:
                            call: googleapis.run.v2.projects.locations.jobs.run
                            args:
                              name: $${qual_job_id}
                              body:
                                overrides:
                                  containerOverrides:
                                    - env:
                                        - name: INPUT_TICKER
                                          value: $${current_filing.Ticker}
                                        - name: INPUT_ACCESSION_NUMBER
                                          value: $${current_filing.AccessionNumber}
                                        - name: INPUT_FILING_DATE
                                          value: $${current_filing.FiledDate}
                                        - name: INPUT_FORM_TYPE
                                          value: $${current_filing.FormType}
                                        - name: INPUT_PDF_GCS_PATH
                                          value: $${pdf_gcs_path}

                  - headline_branch:
                      steps:
                        - callHeadlineAssessment:
                            call: googleapis.run.v2.projects.locations.jobs.run
                            args:
                              name: $${headline_job_id}
                              body:
                                overrides:
                                  containerOverrides:
                                    - env:
                                        - name: INPUT_TICKER
                                          value: $${current_filing.Ticker}
                                        - name: INPUT_FILING_DATE
                                          value: $${current_filing.FiledDate}
                                        - name: INPUT_COMPANY_NAME
                                          value: $${company_name}
              next: endIteration

          - endIteration:
              assign:
                - loop_iteration_complete: true
      next: setLogSeverity

  - recordErrorAndFail:
      assign:
        - all_errors: $${list.concat(all_errors, ["Critical error in workflow."])}
      next: setLogSeverity

  - setLogSeverity:
      switch:
        - condition: $${len(all_errors) > 0}
          assign:
            - log_severity: "WARNING"
        - condition: true
          assign:
            - log_severity: "INFO"
      next: logCompletion

  - logCompletion:
      call: sys.log
      args:
        text: '$${"Workflow completed. Errors: " + string(len(all_errors))}'
        severity: $${log_severity}
      next: decideResult

  - decideResult:
      switch:
        - condition: $${len(all_errors) > 0}
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