# Testing_Automation
Automation testing using pandas
Scope -
Testing automation is an approach / process to automate SIT, UAT and PROD Test cases execution that will greatly reduce time and effort.
This includes:
Design metadata model to accomadate testing automation requirements
Design and create the required tables for testing automation-
1. TESTING_AUTOMATION_INPUT table for file based source and target test cases collection in testing automation ingestion template format.
2. TESTING_AUTOMATION_OUTPUT table for storing results post processing of test cases.
3. TESTIG_AUTOMATION_RESULTS table for writing the output from source query and target query.

Create Testing automation input csv fileand uplaod the TESTING_AUTOMATION_INPUT table.
Then run the testing_automation_validation.py job to load data in TESTING_AUTOMATION_OUTPUT and TESTING_AUTOMATION_RESULTS table.

command to run  python 

python testing_automation_validation.py "<data_src_name>","<workstream_name">, "environment"




