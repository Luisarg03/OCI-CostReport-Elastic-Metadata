#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gzip
import json
import os
import datetime
import pandas as pd
from modules.ElasticQuery import count_rows_oci


def getfiles(client, bucket, file):
    last = None
    list_data = []

    try:

        while True:
            namespace = 'bling'
            prefix = "reports/cost-csv"
            fields = "name, timeCreated"
            report_bucket_objects = client.list_objects(namespace_name=namespace,
                                                        bucket_name=bucket,
                                                        prefix=prefix,
                                                        start_after=last,
                                                        fields=fields)

            for o in report_bucket_objects.data.objects:
                list_data.append({'name': o.name, 'time_created': o.time_created})

            if len(report_bucket_objects.data.objects) == 0:
                break
            else:
                df = pd.DataFrame(list_data)
                last = df.name.tail(1).values[0]

        if file is not None:
            matching = [s for s in df.name if file in s]
            value = df.loc[df.name == matching[0]]
            value = value.name
            for i in value:
                value = i
        else:
            value = None

    except:
        value = None

    return value


def RequestDownload(client, bucket, start, time_start, esclient, account, index, path):
    '''
    Descarga los files de costos desde OCI 
    Parameters
    ----------
    client : oci obj
        obj con la configuracion de OCI
    bucket: str
        nombre del bucket
    start: str
        nombre de file desde donde empezar las descargas (excluye)
    end: str
        nombre de file donde finalizar las descargas (excluye)
    path: str
        directorio donde se guardaran las descargas
    Returns
    ---------
    list
    '''
    namespace = 'bling'
    prefix = "reports/cost-csv"
    fields = "name, timeCreated, size"
    report_bucket_objects = client.list_objects(namespace_name=namespace,
                                                bucket_name=bucket,
                                                prefix=prefix,
                                                start_after=start,
                                                fields=fields)

    dataframe = []

    for i in report_bucket_objects.data.objects:
        filename = i.name
        object_details = client.get_object(namespace_name=namespace,
                                           bucket_name=bucket,
                                           object_name=i.name)

        time_file = i.time_created.strftime("%Y-%m-%d %H:%M:%S")
        time_file = datetime.datetime.strptime(time_file, "%Y-%m-%d %H:%M:%S")

        if time_file >= time_start:
            metadata = []
            filename = i.name.rsplit('/', 1)[-1]
            timecreated = i.time_created
            size = i.size
            _dir = path+filename
            print('Descargando desde: ', filename, 'hacia: ', _dir)


            with open(_dir, 'wb') as f:
                for chunk in object_details.data.raw.stream(2048 ** 2, decode_content=False):
                    f.write(chunk)

            with gzip.open(_dir,'rt') as f:
                count = 0
                for line in f:
                    count = count + 1
                count = count - 1

            escount = count_rows_oci(esclient, index, filename[:-7].replace('0001', ''))
            diff = count - escount

            if diff != 0:
                diff_bool = True
            else:
                diff_bool = False

            info_dict = {
                'filename': filename[:-7],
                'timecreated': timecreated.strftime("%Y-%m-%d %H:%M:%S"),
                'size': size,
                'account': account,
                'rowsoci': count,
                'rowselastic': escount,
                'diff': diff,
                'difference': diff_bool
                }

            metadata.append(info_dict)
            dataframe.append(info_dict)

            output_file = open(_dir[:-7]+'.json', 'w', encoding='utf-8')

            for dic in metadata:
                json.dump(dic, output_file)
                output_file.write("\n")
            output_file.close()


            ## Elimino el .gz descargado para optimizar espacio
            if os.path.exists(_dir):
                os.remove(_dir)
            else:
                pass

        else:
            pass
    
    return dataframe