#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
from modules.path import create_paths
from modules.client import get_client
from modules.ElasticQuery import client, esdelete, lastupdate
from modules.request import getfiles, RequestDownload

from modules.ElasticQuery import insert ### Dev funtion


### Configuracion cliente OCI
config = '../OciConfig/config.prod'
account = 'NC'
ociclient = get_client(config, account)


### Configuracion cliente elastic
esclient = client()
print('\n clientes elastic: ', esclient)

### Variables para desarrollo
index = "oci-dev"
index_target = 'oci-metadata-dev'
esclient = esclient['dev']


### Variables productivas
# index = "oci-nc"
# index_target = 'oci-metadata'
# esclient = esclient['pro']


if __name__ == '__main__':
    ### Directorio de descargas
    path = create_paths()

    ### Borrado de indice metadata
    # days = 1
    # esdelete(esclient, index_target, days)
    # print('\n Tratando de eliminar los registros de', index_target)
    # time.sleep(10)

    ### Obtengo la ultima actualizacion de los indices
    start = lastupdate(esclient, index_target)
    start = getfiles(ociclient['client'], ociclient['bucket'], start)
    print('\n Nombre del files:',start)

    # ################################
    start = 'reports/cost-csv/0001000000692364.csv.gz'
    # ################################

    # Descarga de los nuevos files
    list_files = RequestDownload(
        ociclient['client'],
        ociclient['bucket'],
        start,
        esclient,
        account,
        index,
        path
        )


    ### Desa funcion
    # insert(path, esclient, index_target)
