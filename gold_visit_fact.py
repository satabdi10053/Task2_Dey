from pyspark.sql.functions import *
from pyspark.sql.types import *

df_visit.spark.sql("select * from Lakeflow_BF.BF_Silver.visit")

fact_visit = ( df_visit.alias("v") 
              .join(dim_member.alias("m"), "people_id") 
              .join(dim_club.alias("c"), "club_id") 
              .select( "v.access_id", "m.dim_member_key)", "c.dim_club_key", "v.access_people_id", "v.check_in_time", "v.check_out_time", "v.duration_minutes", "v.device_id", "v.created_at" ) )

