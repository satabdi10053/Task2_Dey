from pyspark.sql.functions import *
from pyspark.sql.types import *

df_visit.spark.sql("select * from Lakeflow_BF.BF_Silver.visit")

df_dim_member=spark.sql("select dim_people_key,people_id as dim_people_id from Lakeflow_BF.BF_gold.dim_Member")


fact_visit = ( df_visit.alias("v") 
              .join(dim_member.alias("m"), "people_id") 
              .join(dim_club.alias("c"), "club_id") 
              .withColumn("visit_date", to_date(col("v.access_visit_datetime "))) 
              select("v.access_id", "m.dim_member_key", "c.dim_club_key", "v.access_people_id", "v.visit_date", "v.access_status", "v.access_type") )
