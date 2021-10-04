#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch.helpers import bulk


def client():
    '''
    Configuracion del cliente para la cuenta Elastic
    Parameters
    ----------

    Returns
    ---------
    dict
    '''
    try:
        proes = Elasticsearch(
            ["rgioddbprd57455.dtvpan.com","rgioddbprd57456.dtvpan.com","rgioddbprd57457.dtvpan.com"],
            http_auth=("logstash", "logstash"),
            use_ssl=True
        )
    except:
        proes = None

    try:
        deves = Elasticsearch(
        host="localhost",
        port=9200,
        http_auth=None,
        use_ssl=False
        )
    except:
        deves = None

    _dict = {'pro': proes, 'dev': deves}

    return _dict


def esdelete(client, index, days):
    '''
    Elimina los registros en el indice especificado en el rango de dias especificado
    '''
    try:
        s = Search(using=client, index=index).query('range' ,  **{'timecreated': {'gte': "now-"+str(days)+"d/d"}})
        response = s.delete()
        return True
    except:
        return False


def lastupdate(client, index):
    '''
    Configuracion del cliente para la cuenta Elastic
    Parameters
    ----------
    client : elasticsearch obj
        obj con la configuracion de elasticsearch
    index: str
        indice elastic desde donde consumir los datos

    Returns
    ---------
    str
    '''
    filename = None
    try:
        s = Search(using=client, index=index) \
        .sort({ "Date": "desc"}) \
        .extra(size=1)

        response = s.execute()
        for hit in response:
            filename = hit.filename
            
        return filename
    except:
        filename = 'reports/cost-csv/0001000000564150.csv.gz' ### Asignar como -> None <- para reprocesar todo

    return filename

    


def count_rows_oci(client ,index, filename):
    '''
    Devuelve cantidad de registros del file especificado
    '''
    # try:
    s = Search(using=client, index=index) \
        .query('regexp', filename=".*{}.*".format(filename))
    rows = s.count()
    # except:
    #     rows = 0
    print(rows)
    return rows


def insert(path, client, index):
    '''
    Funcion para desarrollo ....
    Bulk insert elastic
    
    Parameters
    ----------

    Returns
    ---------
    bool
    '''
    files = glob.glob(path+'*.json')
    files = sorted(files)

    try:
        for file in files:
            print('Elastic insert desde: ', file, 'hacia:', index)
            df = pd.read_json(file, lines=True)
            df['timecreated'] = pd.to_datetime(df['timecreated'])
            df['filename'] = df['filename'].astype(str)
            df = df.to_dict(orient='records')
            bulk(client, df, index=index, raise_on_error=False)
    except:
        pass

    return True
