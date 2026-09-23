### Changed

- Log job lifecycle as structured JSON with a severity, an event field and the job ID in the message, so logs are parsed properly in Google Cloud Logging
- Every job now logs how it ended (finished, failed or aborted), with timings and backend details like the execution ID and query time
- Stack traces are kept on a single log line, and upload progress logs moved to debug
