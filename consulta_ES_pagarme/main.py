# -*- coding: utf-8 -*-
"""Esse script tem como propósito a extração de dados via api da pagarme
através da metodologia de busca avançada, a qual usa o elasticsearch de
maneira atípica, uma vez que devemos enviar a query de consulta via um request
comum e então eles convertem para uma busca elasticsearch. Ele funciona
de maneira que se fornece um ano e mês inicial, a api key pra permissão,
o número de linhas de informação que serão extraídas a cada consulta e o
tipo do dado que será extraído, os meses que serão extraídos em sequência do
mês inicial serão aqueles que o antecedem"""
import requests
import pandas as pd
import time as ts
headers = {
  'Accept': 'application/json, text/plain, */*',
  'Content-Type': 'application/json',
  'Origin': 'https://dashboard.pagar.me',
  'Postman-Token': 'd7b0956b-2deb-4f96-9d15-e5f88af227eb',
  'Referer': 'https://dashboard.pagar.me/',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
  'X-Live': '1',
  'cache-control': 'no-cache'
}
api_key = 'api key'
#parâmetro: chave de permissão para extração
i = 0
year = 2020
#Ano em que começará a extração
month = 1
#Mês em que começará a extração
passo = 5000
#parâmetro da query: Valor do 'count' do request
type = 'transaction'
#parâmetro: tipo do dado a ser extraido
print('Start')
while(i<7):
#O 7 se refere ao número de meses que serão extraídos
    frm = 0
    size = passo
    if (month-i) == 0:
        date_end = '"'+str(year)+'-'+str(month-i+1).zfill(2)+'-01"'
        year = year - 1
        month = i+12
        date_start = '"'+str(year)+'-'+str(month-i).zfill(2)+'-01"'
    else:
        date_end = '"'+str(year)+'-'+str(month-i+1).zfill(2)+'-01"'
        date_start = '"'+str(year)+'-'+str(month-i).zfill(2)+'-01"'
    url = 'https://api.pagar.me/1/search?api_key='+api_key+'&type='+type+''
    df = pd.DataFrame()
    c = 1
    while(frm<c):
        query = """{
                "query": {
                    "query": {
                        "constant_score": {
                            "filter": {
                                "and": [
                                    {"or":[{
                                        "term": {
                                            "status": "paid"
                                        }
                                        },
                                        {
                                            "term": {
                                            "status": "chargeback"
                                        }
                                        },
                                        {
                                            "term": {
                                            "status": "refunded"
                                        }
                                        }
                                    ]
                                    },
                                    {
                                        "range": {
                                            "date_created": {
                                                 "lte": """+date_end+""",
                                                 "gte": """+date_start+"""
                                            }
                                        }
                                    }
                                ]
                            },
                            "boost": 1
                        }
                    },
                    "size": """+str(size)+""",
                    "sort": [
                        {
                            "date_created": "desc"
                        }
                    ],
                    "from": """+str(frm)+"""
                }
        }"""
        response = requests.request("GET", url, headers=headers, data=query)
        lst = response.json()['hits']['hits']
        new_lst = [n['_source'] for n in lst]
        df1 = pd.DataFrame(new_lst)
        df = pd.concat([df,df1], axis=0, ignore_index=True)
        size += passo
        frm += passo
        c = int(response.json()['hits']['total'])
    print(month-i)
    df = df[['id','installments','date_updated','amount','cost','date_created']]
    path = r'Caminho''
    #Caminho onde serão salvos os arquivos
    df.to_csv(path+r'Extracao_'+str(year)+'_'+str(month-i).zfill(2)+'.csv')
    #Os dados extraídos sairão no formato csv com o nome de Extracao_'ano'_'mes'
    i += 1
