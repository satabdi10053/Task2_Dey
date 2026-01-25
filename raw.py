Lakeflow_BF.BF_Raw.Club [Read the table]
Lakeflow_BF.BF_Raw.Member
Lakeflow_BF.BF_Raw.Visit

df = spark.table("Lakeflow_BF.BF_Raw.Club") [load table in Dataframe]
df = spark.table("Lakeflow_BF.BF_Raw.Member")
df = spark.table("Lakeflow_BF.BF_Raw.Visit")

  df.write .mode("overwrite")
 .partitionBy("ingestion_date")
 .parquet("raw@basicfitete.dfs.core.windows.net/BF_Raw/Club/ingestion_date=2026-01-25/")

df.write .mode("overwrite")
 .partitionBy("ingestion_date")
 .parquet("raw@basicfitete.dfs.core.windows.net/BF_Raw/Visit/ingestion_date=2026-01-25/")

df.write .mode("overwrite")
 .partitionBy("ingestion_date")
 .parquet("raw@basicfitete.dfs.core.windows.net/BF_Raw/member/ingestion_date=2026-01-25/")

