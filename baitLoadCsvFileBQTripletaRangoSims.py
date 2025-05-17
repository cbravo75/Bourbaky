#***************************************************
#***Programa: baitLoadCsvFileBQTripletaRangoSims.py
#***Fecha de Creación: 20220713
#***Autor: CBV
#***Usuario: vn53w0m
#***Proyecto: Almacenamiento BAIT
#***Archivo a cargar: Tripleta_Rango_Sims_142_YYYYMMDD.csv
#***BigQuery tabla: BAIT_TRIPLETA_RANGO_SIMS
#***************************************************

import json
import pandas as pd
import gcsfs
import dask.dataframe as dd
from pickle import FALSE, TRUE
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from configBait import GCP_PROJECT_ID, GCP_PUBLIC_KEY_PATH, GCP_BQTBL_BAIT_TRIPLETA_RANGO_SIMS_ID, GCP_BUCKET_NAME, GCP_BUCKET_GS_PATH, TMP_TRIPLETA_RANGO_SIMS_ID
from datetime import datetime, timedelta, date
from google.cloud.bigquery.table import Row, Table

#========Parámetros de Conexión a GCP Storage==============
print('=====Info [baitLoadCsvFileBQTripletaRangoSims]: Inicia conexión BAIT BigQuery...')
fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID, token=GCP_PUBLIC_KEY_PATH)
with open(GCP_PUBLIC_KEY_PATH) as source:fileCredentialsBQ = json.load(source)
storage_credentials = service_account.Credentials.from_service_account_info(fileCredentialsBQ)
storage_client = storage.Client(project=GCP_PROJECT_ID, credentials=storage_credentials)
bigquery_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=storage_credentials)
print('=====Info [baitLoadCsvFileBQTripletaRangoSims]: Credenciales validas GS, BQ...')

Ayer = datetime.today() - timedelta(days= 1)
timestr = datetime.now().strftime("%Y%m%d%H%M%S")
fcargaMilitar = timestr
#======DECLARA FUNCIONES PYTHON===========================
def baitOpenCsvFileGS(pathBucketName, file):
    dateStart = datetime.datetime.now()
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
    dateEnd = datetime.datetime.now()
    print('=====Info [function baitOpenCsvFileGS]: finaliza lectura [%s], hora fin: %s.' %(file, dateEnd))
    print('=====Info [function baitOpenCsvFileGS]: Tiempo de ejecución: ',dateEnd-dateStart)
    return response

def createBQTableTmp():
    table_id = bigquery.Table.from_string(TMP_TRIPLETA_RANGO_SIMS_ID)
    schema = [        
        bigquery.SchemaField("BE_ID", "INTEGER"), 
        bigquery.SchemaField("IMSI", "INTEGER"), 
        bigquery.SchemaField("IMSI_RB1", "INTEGER"), 
        bigquery.SchemaField("IMSI_RB2", "INTEGER"), 
        bigquery.SchemaField("ICCID", "STRING"), 
        bigquery.SchemaField("MSISDN", "INTEGER"), 
        bigquery.SchemaField("PIN", "INTEGER"), 
        bigquery.SchemaField("PUK", "INTEGER"), 
        bigquery.SchemaField("SERIE", "STRING"), 
        bigquery.SchemaField("PRODUCTO", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = bigquery_client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

def deleteBQTableTmp():    
    bigquery_client.delete_table(TMP_TRIPLETA_RANGO_SIMS_ID, not_found_ok=True)
    print("Deleted table '{}'.".format(TMP_TRIPLETA_RANGO_SIMS_ID))
    
def baitLoadCsvFileBQtable(pathBucketName, file):
    dateStart = datetime.now()
    print('=====Info [function baitLoadCsvFileBQtable]: inicia carga [%s], hora inicio: %s.' %(file, dateStart))
    try:        
        job_config = bigquery.LoadJobConfig(
            schema=[        
                bigquery.SchemaField("BE_ID", "INTEGER"), 
                bigquery.SchemaField("IMSI", "INTEGER"), 
                bigquery.SchemaField("IMSI_RB1", "INTEGER"), 
                bigquery.SchemaField("IMSI_RB2", "INTEGER"), 
                bigquery.SchemaField("ICCID", "STRING"), 
                bigquery.SchemaField("MSISDN", "INTEGER"), 
                bigquery.SchemaField("PIN", "INTEGER"), 
                bigquery.SchemaField("PUK", "INTEGER"), 
                bigquery.SchemaField("SERIE", "STRING"), 
                bigquery.SchemaField("PRODUCTO", "STRING")
            ],
            skip_leading_rows=0, 
            field_delimiter = '|',
            source_format=bigquery.SourceFormat.CSV,
        )
        
        load_job = bigquery_client.load_table_from_uri(pathBucketName+file, TMP_TRIPLETA_RANGO_SIMS_ID, job_config=job_config)  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = bigquery_client.get_table(TMP_TRIPLETA_RANGO_SIMS_ID)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))
        response = TRUE   
    except Exception:
        response = FALSE   
        print('Error [function baitLoadCsvFileBQtable]: fallo al cargar el archivo %s a bucket de GCP %s.' %
                    (file, pathBucketName))
        raise    
    dateEnd = datetime.now()
    print('=====Info [function baitLoadCsvFileBQtable]: finaliza carga [%s], hora fin: %s.' %(file, dateEnd))
    print('=====Info [function baitLoadCsvFileBQtable]: Tiempo de ejecución: ',dateEnd-dateStart)
    return response

def baitExecQueryInsertInto(file):
    dateStart = datetime.now()
    print('=====Info [function baitExecQueryInsertInto]: inicia consulta [%s], hora inicio: %s.' %(GCP_BQTBL_BAIT_TRIPLETA_RANGO_SIMS_ID, dateStart))
    try:        
        QUERY = ("INSERT INTO {bqTable} "
                "SELECT A.BE_ID, A.IMSI, A.IMSI_RB1, A.IMSI_RB2, A.ICCID, A.MSISDN, A.PIN, "
                "A.PUK, A.SERIE, A.PRODUCTO, "
                "'{csvFile}' AS NOMBRE_ARCHIVO, DATE('{fechaCarga}') AS FECHA_CARGA "
                "FROM {bqTemp} A ").format(bqTable=GCP_BQTBL_BAIT_TRIPLETA_RANGO_SIMS_ID, bqTemp=TMP_TRIPLETA_RANGO_SIMS_ID,
                                   csvFile=file,fechaCarga=date.today())
        print('Query: ', QUERY)
        query_job = bigquery_client.query(QUERY)  
        rows = query_job.result() 
        #for row in rows:
        #    print(row.BE_ID,' ', row.IMSI, ' ',row.NOMBRE_ARCHIVO, row.FECHA_CARGA)
        
        response = TRUE   
    except Exception:
        response = FALSE   
        print('Error [function baitExecQueryInsertInto]: fallo al consultar la tabla %s.' %
                    (GCP_BQTBL_BAIT_TRIPLETA_RANGO_SIMS_ID))
        raise    
    dateEnd = datetime.now()
    print('=====Info [function baitExecQueryInsertInto]: finaliza consulta [%s], hora fin: %s.' %(GCP_BQTBL_BAIT_TRIPLETA_RANGO_SIMS_ID, dateEnd))
    print('=====Info [function baitExecQueryInsertInto]: Tiempo de ejecución: ',dateEnd-dateStart)
    return response
#===============================================
csvFile = 'tripleta_rango_sims_142_20220713.csv'
#Lee los primeros 5 registros del archivo y la lista de cabeceras.
#baitOpenCsvFileGS(GCP_BUCKET_GS_PATH, csvFile)
#Carga el archivo CSV a la tabla en BigQuery 

print('Paso 1 [createBQTableTmp()]: ', TMP_TRIPLETA_RANGO_SIMS_ID)
createBQTableTmp()
print('Paso 2 [baitLoadCsvFileBQtable()]: ',csvFile)
baitLoadCsvFileBQtable(GCP_BUCKET_GS_PATH, csvFile)
print('Paso 3 [baitExecQuery()]: ', GCP_BQTBL_BAIT_TRIPLETA_RANGO_SIMS_ID)
baitExecQueryInsertInto(csvFile)
print('Paso 4 [deleteBQTableTmp()]: ', TMP_TRIPLETA_RANGO_SIMS_ID)
deleteBQTableTmp()
print('=====Info [baitLoadCsvFileBQTripletaRangoSims]: Finaliza...')



