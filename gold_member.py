from pyspark.sql.functions import *
from pyspark.sql.types import *

df= spark.sql("select * from Lakeflow_BF.BF_gold.club")
