• **Read data from Azure SQL Database. **

First create azure sql database in azure.Then migrate your onprem database to your azure sql database. let me Assume the azure sql database server name is nlazu06003.database.windows.net. under this server Database name is BasicFit.under Basic Fit 3 tables are there .1.Club 2.Visit 3. Member

Then Enable change tracking and enable CDC on database BasicFit with retention period 14 Days. Then Enable change tracking and CDC on 3 tables as well 1. Start tracking row level changes. 

then opened Databricks for connecting to azure sql database.then check lakeflow connect for sqlserver is on . Then from catalog create catalog Lakeflow_BF.under that catalog create schema Delta_BF_raw

Then go to Data Ingestion ,Create pipeline after choosing SQL Server. Make the ingestion gateway connect to Catalog Lakeflow_BF and source will be the Azure Sql Database.

After running the pipeline all 3 tables have been copied to your Delta_BF_Raw schema.

