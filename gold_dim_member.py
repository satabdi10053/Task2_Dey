from pyspark.sql.functions import *
from pyspark.sql.types import *

df_member= spark.sql("select * from Lakeflow_BF.BF_Silver.member")
dim_member = ( df_member .select( F.monotonically_increasing_id().alias("Dim_people_key"), "people_id", "people_age", "people_membership_type", "last_modification_date" ) )
dim_member.write.format("delta").mode("append").save("abfss://BF_gold@basicfitete.dfs.core.windows.net/member")


%SQL

CREATE TABLE Lakeflow_BF.BF_gold.dim_Member USING DELTA LOCATION 'abfss://BF_Silver@basicfitete.dfs.core.windows.net/member';



