**Storage ACCount Creation**
At First , before ingesting from Azure SQL Database we make storage account named **basicfitete**. Under basicfitete we make 4 containers. 1.BF_Raw 2.BF_Bronze 3.BF_Silver 4. BF_Gold 5.metastore
Under each container we make 3 folders . 1. club 2. Member 3. Visit

**Configure Catalog**

Create metastore in catalog first. After that create catalog folder Lakeflow_BF. Under Lakeflow_BF, Please create schema BF_Raw , BF_Bronze, BF_Silver, BF_Gold. And Now create catalog External which is nothing but the mounting point from ADLS storage account. so 1.BF_Raw_ext 2. BF_Bronze_ext 3. BF_Silver_ext 4. BF_Gold_ext.

• **Read data from Azure SQL Database. **

First create azure sql database in azure.Then migrate your onprem database to your azure sql database. let me Assume the azure sql database server name is nlazu06003.database.windows.net. under this server Database name is BasicFit.under Basic Fit 3 tables are there .1.Club 2.Visit 3. Member

Then Enable change tracking and enable CDC on database BasicFit with retention period 14 Days. Then Enable change tracking and CDC on 3 tables as well 1. Start tracking row level changes. 

then opened Databricks for connecting to azure sql database.then check lakeflow connect for sqlserver is on . Then from catalog create catalog Lakeflow_BF.under that catalog create schema BF_raw

Then go to Data Ingestion ,Create pipeline after choosing SQL Server. Make the ingestion gateway connect to Catalog Lakeflow_BF and source will be the Azure Sql Database.

After running the pipeline all 3 Delta tables have been copied to your BF_Bronze schema. So go to storage account, under the Metastore you can you Deltatable.

Now in Databricks  will go to dev Workspace and open notebook for bronze Layer  . After opening the notebook attach with the cluster.











