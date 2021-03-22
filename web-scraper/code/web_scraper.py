import lxml
import requests
import json
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
    url = "https://www.quefondos.com/es/fondos/ficha/index.html?isin=" + fondo
    result = requests.get(url)
    c = result.content
    soup = BeautifulSoup(c, "lxml")
    title = soup.title.contents[0]
    valor_liquidativo = soup.find('span',text='Valor liquidativo: ').parent.find_all("span", class_="floatright")[0].text
    fecha_valor_liquidativo = soup.find('span', text='Fecha: ').parent.find_all("span",class_="floatright")[0].text
    table = soup.find('h4',text='Rentabilidad/Riesgo a 1 año').parent
    ##print(table)
    data = {}
    data['ISIN'] = fondo
    data['VL'] = float(valor_liquidativo.split(' ')[0].replace(',', '.'))
    data['VL_date'] = fecha_valor_liquidativo
    for td in table.find_all('p'):
      key=td.find('span',{"class": "floatleft"}).text.split(':')[0]
      value=td.find('span',{"class": "floatright"}).text.split(':')[0]
      data[key]=value
      ##print(bla.text.split(':')[0])
        ##print(bla.parentfind)
            #data[td.find('span',{"class": "floatleft"}).text.split(':')[0]]=td.find('span',{"class": "floatright"}).text.split(':')[0]
    ##print (table)
    #print(data)
    ##print(fecha_valor_liquidativo)
    json_data = json.dumps(data)
    print(json_data)
    producer = KafkaProducer(bootstrap_servers='broker:29092')
    ##for _ in range(100):
    producer.send('vl2',json_data.encode('utf-8'))
	producer.flush()
