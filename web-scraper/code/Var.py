# spark is from the previous example.
sc = spark.sparkContext

# Lectura de los json historicos
path = "hdfs://namenode:8020/usr/logstash/vl*.json"
VLDF = spark.read.json(path)

# visualizacion del schema
VL.printSchema()
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)


# SQL statements can be run by using the sql methods provided by spark
VLDF.createOrReplaceTempView("VL")
distinctVLDF = spark.sql("SELECT distinct ISIN,VL,VL_date FROM VL")
distinctVLDF.createOrReplaceTempView("VL")

PL = spark.sql ("SELECT *,VL-VL_PREV as PL FROM (SELECT ISIN,VL_DATE, VL, LAG(VL,1) OVER (PARTITION BY ISIN ORDER BY VL_DATE) AS VL_PREV FROM VL) where VL_PREV IS NOT NULL ORDER BY ISIN,PL ASC")  
PL.createOrReplaceTempView("PL")

PL=spark.sql ("SELECT ISIN,date_format(current_date(),'yyyy-MM-dd'),percentile_approx(PL,0.05,100) from PL GROUP BY ISIN")

PL.write.json("hdfs://namenode:8020/usr/logstash/VARFUNDOS.json")
