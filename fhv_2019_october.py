import pyspark
from pyspark.sql import SparkSession, types, functions as F
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

fhv_schema = types.StructType([
    types.StructField("dispatching_base_num", types.StringType(), True),
    types.StructField("pickup_datetime", types.TimestampType(), True),
    types.StructField("dropoff_datetime", types.TimestampType(), True),
    types.StructField("PUlocationID", types.IntegerType(), True),
    types.StructField("DOlocationID", types.IntegerType(), True),
    types.StructField("SR_Flag", types.StringType(), True),
])


df = spark.read \
    .option("header", "true") \
    .schema(fhv_schema) \
    .csv('fhv_tripdata_2019-10.csv')

df = df.repartition(6)
df.write.parquet('fhv/2019/10',mode='overwrite')
df = spark.read.parquet('fhv/2019/10/')
df.printSchema()

#df.select('pickup_datetime','dropoff_datetime','PUlocationID','DOlocationID')
#df.withColumn('pickup_datetime', F.to_date(df.pickup_datetime)).filter(df.pickup_datetime >= '2019-10-15').orderBy(df.pickup_datetime,ascending=True).filter(df.pickup_datetime == '2019-10-15').show()
#df.withColumn('duration_trip', (df.dropoff_datetime.cast('long') - df.pickup_datetime.cast('long')) / 3600).orderBy('duration_trip',ascending=False).show()
##mik = df.orderBy(df.pickup_datetime,ascending=True).filter(F.to_date(df.pickup_datetime) == '2019-10-15').count()
##print(mik)
df.select('PUlocationID').groupBy('PUlocationID').count().orderBy('PUlocationID').show()

#df.filter(df.pickup_date == '2019-10-15').show()
