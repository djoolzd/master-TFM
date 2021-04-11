#park is from the previous example.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# Lectura de los json historicos
path = "hdfs://namenode:8020/user/logstash/vl/logstash*.json"
VLDF = spark.read.json(path)


# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

##Collect date from VL to be calcultaed:
VLDF.createOrReplaceTempView("VL")
date=spark.sql("SELECT distinct VL_date FROM VL where VL_date > '2020-04-04' order by VL_date asc")
# SQL statements can be run by using the sql methods provided by spark

distinctVLDF = spark.sql("SELECT distinct ISIN,VL,VL_date FROM VL order by VL_date asc")
distinctVLDF.createOrReplaceTempView("VL")

for date_var in date.collect():
        PL = spark.sql ("SELECT *,VL-VL_PREV as PL FROM (SELECT ISIN,VL_DATE, VL, LAG(VL,1) OVER (PARTITION BY ISIN ORDER BY VL_DATE) AS VL_PREV FROM VL where VL_DATE < '{0}' and VL_DATE> add_months('{1}',-12)) where VL_PREV IS NOT NULL ORDER BY ISIN,PL ASC".format(date_var["VL_date"],date_var["VL_date"]))
        #PL.show()
        PL.createOrReplaceTempView("PL")
        PL=spark.sql ("SELECT ISIN,'{}' as DATE_VAR,percentile_approx(PL,0.05,100) as VAR from PL GROUP BY ISIN".format(date_var["VL_date"]))
        #PL.show()
        #PL.toPandas().to_json("hdfs://namenode:8020/usr/logstash/var/VARFUNDOS_{}.json".format(date_var["VL_date"]))
        PL.coalesce(1).write.json("hdfs://namenode:8020/user/logstash/var2/")
