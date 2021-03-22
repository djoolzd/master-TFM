import lxml
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from kafka import KafkaProducer

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))

codigos_fondos = [
    "IE00B3B2KS38", "IE0032620787", "IE0031786142", "IE00B04GQT48"
]
for fondo in codigos_fondos:
	df=pd.read_csv(fondo+'data.csv', sep = ',')
	for index,row in df.interrows:
		data = {}
		data['ISIN'] = fondo
		data['VL'] = float(row.Close)
		data['VL_date'] = row.Date
		json_data = json.dumps(data)
		#print(json_data)
		producer = KafkaProducer(bootstrap_servers='broker:29092')
		##for _ in range(100):
		producer.send('vl2',json_data.encode('utf-8'))
		producer.flush()
