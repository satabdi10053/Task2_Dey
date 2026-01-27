from pyspark.sql.functions import *
from pyspark.sql.types import *

df_club.spark.sql("select * from Lakeflow_BF.BF_Silver.club")

dim_club = ( df_club .select( F.monotonically_increasing_id().alias("club_sk"), "club_id", "club_name", "city", "state", "country", "opened_date" ) )

dim_club.write.format("delta").mode("append").save("abfss://BF_Gold@basicfitete.dfs.core.windows.net/club")

%Sql

%SQL
CREATE TABLE Lakeflow_BF.BF_gold.dim_club USING DELTA LOCATION 'abfss://BF_Gold@basicfitete.dfs.core.windows.net/club';
