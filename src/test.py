from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


##################################
# File para hacer pruebas simples#
##################################

es = Elasticsearch(
    ["rgioddbprd57455.dtvpan.com","rgioddbprd57456.dtvpan.com","rgioddbprd57457.dtvpan.com"],
    http_auth=("logstash", "logstash"),
    use_ssl=True
)

def count_rows_oci(client ,index, filename):
    '''
    Devuelve cantidad de registros del file especificado
    '''
    # try:
    s = Search(using=client, index=index) \
        .query('regexp', path=".*{}.*".format(filename))
    rows = s.count()
    # except:
    #     rows = 0
    print(rows)
    return rows


count_rows_oci(es, "oci-dc", "593303")