df.spark.sql("select * from Lakeflow_BF.BF_Silver.club")

dim_club = ( df_club .select( F.monotonically_increasing_id().alias("club_sk"), "club_id", "club_name", "city", "state", "country", "opened_date" ) )
