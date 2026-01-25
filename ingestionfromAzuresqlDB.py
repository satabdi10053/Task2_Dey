Ingestion from Azure sql DB and stored in ADLS Gen2

jdbc_url = "jdbc:sqlserver://nlazu06003.database.windows.net:1433;database=BasicFit" 
connection_props = {
  "user": "sa", 
  "password": "dgjhgvj", 
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver" }

df_member = spark.read.jdbc(jdbc_url, "dbo.Member", connection_props) 
df_club = spark.read.jdbc(jdbc_url, "dbo.Club", connection_props) 
df_visit = spark.read.jdbc(jdbc_url, "dbo.Visit", connection_props)
bronze_base = "abfss://BF_Bronze@basicfitete.dfs.core.windows.net/" 
for name, df in [("member", df_member), ("club", df_club), ("visit", df_visit)]:
  (df.withColumn("ingestion_ts", F.current_timestamp()) .write.format("delta") .mode("append") .save(f"{bronze_base}/{name}"))



%SQL [Register tables in Unity Catalog]
CREATE TABLE Lakeflow_BF.BF_Bronze.Member USING DELTA LOCATION 'abfss://BF_Bronze@basicfitete.dfs.core.windows.net/member';
CREATE TABLE Lakeflow_BF.BF_Bronze.club USING DELTA LOCATION 'abfss://BF_Bronze@basicfitete.dfs.core.windows.net/club';
CREATE TABLE Lakeflow_BF.BF_Bronze.visit USING DELTA LOCATION 'abfss://BF_Bronze@basicfitete.dfs.core.windows.net/visit';
