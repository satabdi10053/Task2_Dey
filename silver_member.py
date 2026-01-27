df_member=spark.read.format("delta")\
.load("abfss://BF_Bronze@basicfitete.dfs.core.windows.net/member")\
.display(df_member)  
df_member.withcolumnRenamed("_rescued_data", "unexpected_Data").display()\ 
df_member=df_member.withcolumn("last_modification_date_utc",to_utc_timestamp(col("last_modification_date"), "UTC"))
df_member.write.format("delta").mode("äppend").save("abfss://BF_Silver@basicfitete.dfs.core.windows.net/member")

df_member=df_member.dropDuplicates(subset['people_id'])

%SQL
CREATE TABLE Lakeflow_BF.BF_Silver.Member USING DELTA LOCATION 'abfss://BF_Silver@basicfitete.dfs.core.windows.net/member';
A
MERGE INTO silver.member AS tgt USING bronze.member AS src ON tgt.member_id = src.member_id WHEN MATCHED AND src.SYS_CHANGE_OPERATION = 'D' THEN DELETE WHEN MATCHED AND src.SYS_CHANGE_OPERATION <> 'D' THEN UPDATE SET * WHEN NOT MATCHED AND src.SYS_CHANGE_OPERATION <> 'D' THEN INSERT *;
