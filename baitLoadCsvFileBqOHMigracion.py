#***************************************************
#***Programa: baitLoadCsvFileBqOHMigracion.py
#***Fecha de Creación: 20220713
#***Autor: CBV
#***Usuario: vn53w0m
#***Proyecto: Almacenamiento BAIT
#***Archivo a cargar:  APGW_Transacciones_142_YYYYMMDD.csv
#***BigQuery tabla: BAIT_OHMIGRACION
#***************************************************

import json
import pandas as pd
import gcsfs
import dask.dataframe as dd
from pickle import FALSE, TRUE
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from configBait import GCP_PROJECT_ID, GCP_PUBLIC_KEY_PATH, GCP_BQTBL_BAIT_MIGRACION_ID, GCP_BUCKET_GS_PATH, PATH_ANIO2020
from datetime import datetime, timedelta, date
from google.cloud.bigquery.table import Row, Table

#========Parámetros de Conexión a GCP Storage==============
print('=====Info [baitLoadCsvFileBqOHMigracion]: Inicia conexión BAIT BigQuery...')
fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID, token=GCP_PUBLIC_KEY_PATH)
with open(GCP_PUBLIC_KEY_PATH) as source:fileCredentialsBQ = json.load(source)
storage_credentials = service_account.Credentials.from_service_account_info(fileCredentialsBQ)
storage_client = storage.Client(project=GCP_PROJECT_ID, credentials=storage_credentials)
bigquery_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=storage_credentials)
print('=====Info [baitLoadCsvFileBqOHMigracion]: Credenciales validas GS, BQ...')

#======DECLARA FUNCIONES PYTHON===========================
def baitOpenCsvFileGS(pathBucketName, file):
    dateStart = datetime.now()
    print('=====Info [function baitOpenCsvFileGS]: inicia lectura [%s], hora inicio: %s.' %(file, dateStart))
    try:
        df = dd.read_csv(pathBucketName+file, storage_options={'token': GCP_PUBLIC_KEY_PATH}, sep='|', header=None)
        print("Columnas DataFrame:")
        print(df.dtypes)   
        print("Primeros 5 registros de archivo:")
        print(df.head(5))
        print("Últimos 5 registros del archivo:")
        print(df.tail(5))
        print("Lista de cabeceras [1er Registro del archivo]: ") 
        list_of_column_names = list(df.columns)
        print(list_of_column_names)
        print("===========================================================") 
        df['CBV'] = 'CBV'
        list_of_column_names = list(df.columns)
        print(list_of_column_names)   
        print("Primeros 5 registros de archivo:")
        print(df.head(5))
        print("Últimos 5 registros del archivo:")
        print(df.tail(5))
        response = TRUE   
    except Exception:
        response = FALSE   
        print('Error [function baitOpenCsvFileGS]: fallo al leer el archivo %s a bucket de GCP %s.' %
                    (file, pathBucketName))
        raise    
    dateEnd = datetime.now()
    print('=====Info [function baitOpenCsvFileGS]: finaliza lectura [%s], hora fin: %s.' %(file, dateEnd))
    print('=====Info [function baitOpenCsvFileGS]: Tiempo de ejecución: ',dateEnd-dateStart)
    return response

def createBQTableTmp():
    table_id = bigquery.Table.from_string(GCP_BQTBL_BAIT_MIGRACION_ID)
    schema = [                      
        bigquery.SchemaField("objectId", "INTEGER"), 
        bigquery.SchemaField("pathId", "STRING"), 
        bigquery.SchemaField("filePath", "STRING"), 
        bigquery.SchemaField("fileName", "STRING"), 
        bigquery.SchemaField("fileSize", "INTEGER"), 
        bigquery.SchemaField("volSumSize", "INTEGER"), 
        bigquery.SchemaField("fileExt", "STRING"), 
        bigquery.SchemaField("contentKey", "STRING"), 
        bigquery.SchemaField("dateSearch", "DATE"), 
        bigquery.SchemaField("statusMigration", "INTEGER"), 
        bigquery.SchemaField("dateMigration", "DATE"), 
        bigquery.SchemaField("fileSizeMigration", "INTEGER")
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = bigquery_client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

def deleteBQTableTmp():    
    bigquery_client.delete_table(GCP_BQTBL_BAIT_MIGRACION_ID, not_found_ok=True)
    print("Deleted table '{}'.".format(GCP_BQTBL_BAIT_MIGRACION_ID))

def baitLoadCsvFileBQtableTmp(pathBucketName, file):
    dateStart = datetime.now()
    print('=====Info [function baitLoadCsvFileBQtableTmp]: inicia carga [%s], hora inicio: %s.' %(file, dateStart))
    try:
        #df = dd.read_csv(pathBucketName+file, storage_options={'token': GCP_PUBLIC_KEY_PATH}, sep='|', header=None)
        #df['NOMBRE_ARCHIVO'] = file
        job_config = bigquery.LoadJobConfig(
            schema=[        
                bigquery.SchemaField("objectId", "INTEGER"), 
                bigquery.SchemaField("pathId", "STRING"), 
                bigquery.SchemaField("filePath", "STRING"), 
                bigquery.SchemaField("fileName", "STRING"), 
                bigquery.SchemaField("fileSize", "INTEGER"), 
                bigquery.SchemaField("volSumSize", "INTEGER"), 
                bigquery.SchemaField("fileExt", "STRING"), 
                bigquery.SchemaField("contentKey", "STRING"), 
                bigquery.SchemaField("dateSearch", "DATE"), 
                bigquery.SchemaField("statusMigration", "INTEGER"), 
                bigquery.SchemaField("dateMigration", "DATE"), 
                bigquery.SchemaField("fileSizeMigration", "INTEGER")
            ],
            skip_leading_rows=1, 
            field_delimiter = ',',
            source_format=bigquery.SourceFormat.CSV,
        )
        
        load_job = bigquery_client.load_table_from_uri(pathBucketName+file, GCP_BQTBL_BAIT_MIGRACION_ID, job_config=job_config)  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = bigquery_client.get_table(GCP_BQTBL_BAIT_MIGRACION_ID)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))
        response = TRUE   
    except Exception:
        response = FALSE   
        print('Error [function baitLoadCsvFileBQtableTmp]: fallo al cargar el archivo %s a bucket de GCP %s.' %
                    (file, pathBucketName))
        raise    
    dateEnd = datetime.now()
    print('=====Info [function baitLoadCsvFileBQtableTmp]: finaliza carga [%s], hora fin: %s.' %(file, dateEnd))
    print('=====Info [function baitLoadCsvFileBQtableTmp]: Tiempo de ejecución: ',dateEnd-dateStart)
    return response

def baitExecQuerySelect(file):
    dateStart = datetime.now()
    print('=====Info [function baitExecQuerySelect]: inicia consulta [%s], hora inicio: %s.' %(GCP_BQTBL_BAIT_MIGRACION_ID, dateStart))
    try:        
        QUERY = (
                "SELECT A.objectId, A.pathId, A.filePath, A.fileName, A.fileSize, A.fileExt, A.contentKey "
                "FROM {bqTable} A "
                "WHERE A.pathId = '{pathId}' AND NOT A.fileExt IS NULL "
                "AND A.statusMigration = 0"
                "ORDER BY A.objectId "
                "LIMIT 1000 "
                ).format(bqTable=GCP_BQTBL_BAIT_MIGRACION_ID, pathId=PATH_ANIO2020)
        print('Query: ', QUERY)
        query_job = bigquery_client.query(QUERY)  
        rows = query_job.result() 
        for row in rows:
            print(row.objectId,' ', row.pathId, ' ',row.fileName, row.fileExt, row.fileSize)
        
        response = TRUE   
    except Exception:
        response = FALSE   
        print('Error [function baitExecQuerySelect]: fallo al consultar la tabla %s.' %
                    (GCP_BQTBL_BAIT_MIGRACION_ID))
        raise    
    dateEnd = datetime.now()
    print('=====Info [function baitExecQuerySelect]: finaliza consulta [%s], hora fin: %s.' %(GCP_BQTBL_BAIT_MIGRACION_ID, dateEnd))
    print('=====Info [function baitExecQuerySelect]: Tiempo de ejecución: ',dateEnd-dateStart)
    return response

#===============================================
csvFile = 'bucketHuawei_2022727331.csv'

print('Paso 0 [deleteBQTableTmp()]: ', GCP_BQTBL_BAIT_MIGRACION_ID)
#deleteBQTableTmp()
print('Paso 1 [createBQTableTmp()]: ', GCP_BQTBL_BAIT_MIGRACION_ID)
#createBQTableTmp()
print('Paso 2 [baitLoadCsvFileBQtableTmp()]: ',csvFile)
#baitLoadCsvFileBQtableTmp(GCP_BUCKET_GS_PATH, csvFile)
print('Paso 3 [baitExecQuerySelect()]: ', GCP_BQTBL_BAIT_MIGRACION_ID)
baitExecQuerySelect(csvFile)
print('=====Info [baitLoadCsvFileBqOHMigracion]: Finaliza...')



