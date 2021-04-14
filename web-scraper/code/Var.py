#park is from the previous example.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# Lectura de los json historicos
path = "hdfs://namenode:8020/user/logstash/vl/logstash*.json"
VLDF = spark.read.json(path)


##Creacion vista partial
VLDF.createOrReplaceTempView("VL")
#Se recu`peran en un dataframe todas las fechas del ultim años. Para cada una vamos a tener que calcular el VAR
date=spark.sql("SELECT distinct VL_date FROM VL where VL_date > '2020-04-04' order by VL_date asc")

#Se quitan los duplicados de VL'S por si a caso
distinctVLDF = spark.sql("SELECT distinct ISIN,VL,VL_date FROM VL order by VL_date asc")
distinctVLDF.createOrReplaceTempView("VL")
losy calcular la ganancia o perdida. Ademas se clasifican los PL del año anterior del mas bajo al mas alto
PL = spark.sql ("SELECT *,VL-VL_PREV as PL FROM (SELECT ISIN,VL_DATE as DATE_REF, VL, LAG(VL,1) OVER (PARTITION BY ISIN ORDER BY VL_DATE) AS VL_PREV FROM VL ) where VL_PREV IS NOT NULL ORDER BY ISIN,PL ASC")
#PL.coalesce(1).write.mode('overwrite').json("hdfs://namenode:8020/user/logstash/all_vl")
#Para cada fecha 
for date_var in date.collect():

		#Se calculan los Profit and lossses comparando el valor de VL con el del dia anterior para restar 
        PL = spark.sql ("SELECT *,VL-VL_PREV as PL FROM (SELECT ISIN,VL_DATE, VL, LAG(VL,1) OVER (PARTITION BY ISIN ORDER BY VL_DATE) AS VL_PREV FROM VL where VL_DATE <= '{0}' and VL_DATE> add_months('{1}',-12)) where VL_PREV IS NOT NULL ORDER BY ISIN,PL ASC".format(date_var["VL_date"],date_var["VL_date"]))
        #vista partial de PL
        PL.createOrReplaceTempView("PL")
		#se busca el percentil valor mas cerca del percentil a 5% de las peores perdidas. Es el VAR
        PL=spark.sql ("SELECT ISIN,'{}' as DATE_REF,percentile_approx(PL,0.05,100) as VAR from PL GROUP BY ISIN".format(date_var["VL_date"]))
        PL.createOrReplaceTempView("VAR")
		#Se recupera el VAR para la fecha en curso, el codigo del fondo. Se recupera el VL para este dia y el PL para tenerlo en un unico docuemento json
        VAR=spark.sql("SELECT VAR.*,PL.PL,PL.VL from VAR INNER JOIN PL ON (VAR.ISIN=PL.ISIN AND VAR.DATE_REF=PL.VL_DATE) ")
		#se manda el json HDFS
        VAR.coalesce(1).write.mode('append').json("hdfs://namenode:8020/user/logstash/all_var")
