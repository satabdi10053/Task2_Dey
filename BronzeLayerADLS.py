df_club = spark.table("Lakeflow_BF.BF_Bronze.club")
df_member = spark.table("Lakeflow_BF.BF_Bronze.member") 
df_visit = spark.table("Lakeflow_BF.BF_Bronze.visit")

df_club.write.format("delta") \ 
.mode("append") \ 
.save("abfss://BF_Bronze@basicfitete.dfs.core.windows.net/club")

df_member.write.format("delta") \ 
.mode("append") \ 
.save("abfss://BF_Bronze@basicfitete.dfs.core.windows.net/member")

df_visit.write.format("delta") \ 
.mode("append") \ 
.save("abfss://BF_Bronze@basicfitete.dfs.core.windows.net/visit")
