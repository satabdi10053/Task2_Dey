df_visit=spark.read.format("delta")\
.load("abfss://BF_Bronze@basicfitete.dfs.core.windows.net/visit")\
.display(df_visit)  
df_visit.withcolumnRenamed("_rescued_data", "unexpected_Data").display()\ 
df_visit=df_visit.withcolumn("access_visit_datetime_utc",to_utc_timestamp(col("access_visit_datetime"), "UTC"))
df_visit.write.format("delta").mode("äppend").save("abfss://BF_Silver@basicfitete.dfs.core.windows.net/visit")

