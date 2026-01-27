from pyspark.sql.functions import *
from pyspark.sql.types import *

df_visit.spark.sql("select * from Lakeflow_BF.BF_Silver.visit")
df=spark.sql("select * from Lakeflow_BF.BF_Silver.member")
df=spark.sql("select * from Lakeflow_BF.BF_Silver.club")
df_dim_member=spark.sql("select dim_people_key,people_id as dim_people_id from Lakeflow_BF.BF_gold.dim_Member")
df_dim_club=spark.sql("select dim_club_key,club_id as dim_club_id from Lakeflow_BF.BF_gold.dim_club"


df_fact_visit =df.join(df_dim_member,df['people_id']==df_dim_member['dim_people_id'],how=left).join(df_dim_club,df['club_id']==df_dim_club['dim_club_id'],how=left)
df_fact_visit_new=df_fact_visit.drop('dim_people_id', 'dim_club_id')

dim_club.write.format("delta").mode("append").save("abfss://BF_Gold@basicfitete.dfs.core.windows.net/visit")
