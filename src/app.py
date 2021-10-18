#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import time
from modules.path import create_paths
from modules.client import get_client
from modules.ElasticQuery import client, esdelete, lastupdate, listreprocess
from modules.request import getfiles, RequestDownload

from modules.ElasticQuery import insert ### Dev funtion


### Configuracion cliente OCI
config = '../OciConfig/config.prod'
account = 'DC'
ociclient = get_client(config, account)


### Configuracion cliente elastic
esclient = client()
print('\n clientes elastic: ', esclient)

### Variables para desarrollo
esclient = esclient['dev']
index = "oci-dev"
index_target = 'oci-metadata-dev'

### Variables productivas
# index = "oci-nc"
# index_target = 'oci-metadata'
# esclient = esclient['pro']

### Hora server
time_start = datetime.datetime.now()
time_start = time_start.strftime("%Y-%m-%d 00:00:00")
time_start = datetime.datetime.strptime(time_start, "%Y-%m-%d %H:%M:%S")

### Intervalos de dias a tomar en la descarga
days = 2
time_start = time_start - datetime.timedelta(days=days)


if __name__ == '__main__':
    ### Directorio de descargas
    path = create_paths()

    ### Borrado de indice metadata
    esdelete(esclient, index_target, days)
    print('\n Tratando de eliminar los registros de', index_target)
    time.sleep(3)

    ### Obtengo la ultima actualizacion de los indices
    start = lastupdate(esclient, index_target)
    start = getfiles(ociclient['client'], ociclient['bucket'], start)
    print('\n Nombre del files:',start)

    # Descarga de los nuevos files
    list_files = RequestDownload(
        ociclient['client'],
        ociclient['bucket'],
        start,
        time_start,
        esclient,
        account,
        index,
        path
        )

    listreprocess(list_files, path)
    
    ### Desa funcion
    insert(path, esclient, index_target)
