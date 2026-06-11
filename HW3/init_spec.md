(20%) Replace hardcoded values with Jinja templates. Try to use DAG params (can be done on DAGs from next tasks).

(30%) Cross-Dag dependencies - implement for one of the cities:

Split your pipeline into 2 DAGs and connect them. 
weather_ingestion_dag
extracting weather data + storing raw data into external storage
weather_processing_dag
reading raw data from storage + transforming it + producing final dataset
(50%) Use design patterns

Create a factory that generates DAGs for cities (shown as example during lecture)
Implement data quality checks
Implement ETL with explicit external storage. After each step store data and use it in following processing step
In case of failure, successfully completed steps should not be re-executed. The DAG should be able to resume from the failed step.