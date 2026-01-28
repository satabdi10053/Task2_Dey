**Storage ACCount Creation**
At First , before ingesting from Azure SQL Database we make storage account named **basicfitete**. Under basicfitete we make 4 containers. 1.BF_Raw 2.BF_Bronze 3.BF_Silver 4. BF_Gold 5.metastore
Under each container we make 3 folders . 1. club 2. Member 3. Visit

**Configure Catalog**

Create metastore in catalog first. After that create catalog folder Lakeflow_BF. Under Lakeflow_BF, Please create logical layer BF_Raw , BF_Bronze, BF_Silver, BF_Gold. And Now create catalog External which is nothing but the mounting point from ADLS storage account. so 1.BF_Raw_ext 2. BF_Bronze_ext 3. BF_Silver_ext 4. BF_Gold_ext.

• **Read data from Azure SQL Database. **

First create azure sql database in azure.Then migrate your onprem database to your azure sql database. let me Assume the azure sql database server name is nlazu06003.database.windows.net. under this server Database name is BasicFit.under Basic Fit 3 tables are there .1.Club 2.Visit 3. Member

Then Enable change tracking and enable CDC on database BasicFit with retention period 14 Days. Then Enable change tracking and CDC on 3 tables as well 1. Start tracking row level changes. 

then opened Databricks for connecting to azure sql database.then check lakeflow connect for sqlserver is on . Then from catalog create catalog Lakeflow_BF.under that catalog create logical layer BF_raw.
run **ingestionfromAzuresqlDB.py**  [ingested data from sql database in raw folderin ADLS ] after that  used to create sql table in logical layer BF_Raw.
After that we will run **autoloaderinBronze.py** from mounted raw file. we use here incremental loading and save it bronze folder and same as before  created sql tables in bronze layer.

Then run all silver notebook for member , club and visit.As in bronze layer we ingested data through autoloader so extra column has been created. that can be dropped. then in silver notebook change the time in UTC.also in silver notebook SCD applied in silver_member file.

Then with surrogate key we are creating dimention club and dimention member table. from two dimention table we create fact table fact visit









pipeline Design:














