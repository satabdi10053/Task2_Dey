df_club=spark.read.format("delta")\
.load("abfss://BF_Bronze@basicfitete.dfs.core.windows.net/club")\
.display(df_club)  [autoloader automatically creates extra schema that is "rescued_data" or I am saying it as unexpected data]
df_club.withcolumnRenamed("_rescued_data", "unexpected_Data").display()\ 
df_club=df_club.withcolumn("last_modification_date_utc",to_utc_timestamp(col("last_modification_date"), "UTC"))
df_club.write.format("delta").mode("äppend").save("abfss://BF_Silver@basicfitete.dfs.core.windows.net/club")

%SQL
CREATE TABLE Lakeflow_BF.BF_Silver.club USING DELTA LOCATION 'abfss://BF_Silver@basicfitete.dfs.core.windows.net/club';




