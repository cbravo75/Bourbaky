#***************************************************
#***Programa: Bait_1MigracionOHuaweiMultiProcessing.py
#***Fecha de Creación: 20220906
#***Autor: CBV
#***Usuario: vn53w0m
#***Proyecto: Almacenamiento BAIT
#***Archivo a cargar:  bucketHuawei_2022727331.csv
#***BigQuery tabla: BAIT_OHMIGRACION
#***************************************************

import logging
import json
import gc
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from asyncio.log import logger
from pickle import FALSE, TRUE
from obs import ObsClient
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from google.cloud.bigquery.table import Row, Table
from time import time
from sqlalchemy import false, null, true
from configBait import HUAWEI_AK, HUAWEI_SK, HUAWEI_SERVER, HUAWEI_BUCKET_NAME
from configBait import GCP_PROJECT_ID_DEV, GCP_PUBLIC_KEY_PATH_DEV, GCP_BUCKET_NAME_DEV
from configBait import GCP_PROJECT_ID_TMP, GCP_PUBLIC_KEY_PATH_TMP, GCP_BQTBL_BAIT_MIGRACION_ID, GCP_BUCKET_GS_PATH_TMP, PATH_ANIO2022

#========Parámetros de Conexión a GCP Storage Temporal==============
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
with open(GCP_PUBLIC_KEY_PATH_TMP) as source:fileCredentialsBQ = json.load(source)
storage_credentials = service_account.Credentials.from_service_account_info(fileCredentialsBQ)
storage_client = storage.Client(project=GCP_PROJECT_ID_TMP, credentials=storage_credentials)
bigquery_client = bigquery.Client(project=GCP_PROJECT_ID_TMP, credentials=storage_credentials)

#======DECLARA CLASS PYTHON===========================
def run_multiProcessing(_row):
    logger.info('Process row.objectId : %s row.fileSize: %s', _row[4], _row[3])
    try:
        get_specific_file_from_huawei(_row[0], _row[1], _row[2])
    except Exception:    
        logger.error('Process Exception run_multiProcessing %s ', _row[4]) 
        _row[4] = 0
    logger.info('Process Finished %s ', _row[4]) 
    return _row[4]
                            
#======DECLARA FUNCIONES PYTHON===========================
def bait_init_migration(bqtbl_bait_migracion_id, path_Id, directory_name):
    dateStart = datetime.now()
    print('=====Info [function bait_init_migration]: inicia consulta [%s], hora inicio: %s.' %(bqtbl_bait_migracion_id, dateStart))
    try:     
        isStatusMigration = bait_is_status_migration(bqtbl_bait_migracion_id, path_Id, directory_name)
        indexObjetos = 0
        iteraciones = 0       
        while isStatusMigration:   
            ts = time()
            iteraciones += 1
            QUERY = (
                "SELECT A.objectId, A.pathId, A.filePath, A.fileName, A.fileSize, A.fileExt, A.contentKey "
                "FROM {bqTable} A "
                "WHERE A.pathId = '{pathId}' AND A.filePath LIKE '{directoryName}' " 
                "AND NOT A.fileExt IS NULL "
                "AND A.statusMigration = 0 " #AND A.fileSize > 1000000000 "
                "ORDER BY A.fileSize DESC "
                "LIMIT 1000 "
                ).format(bqTable=bqtbl_bait_migracion_id, pathId=path_Id, directoryName=directory_name)
            print("QUERY: ",QUERY)
            query_job = bigquery_client.query(QUERY)  
            ids = []    
            registros = []     
            rows = query_job.result()                   
            for row in rows:      
                indexObjetos += 1
                ids.append(row.objectId)  
                reg = [row.contentKey, row.fileName, row.filePath, row.fileSize, row.objectId]             
                registros.append(reg)    
            bait_change_status_migration(ids, bqtbl_bait_migracion_id, 10) 
            ids = []
            with Pool() as p:
                ids = p.map(run_multiProcessing, registros)
            bait_change_status_migration(ids, bqtbl_bait_migracion_id, 2) 
            logger.info('Tiempo de Ejecución: %s', time() - ts)
            gc.collect()
            print('=====Iteraciones:'+str(iteraciones)+': Número de objetos migrados: ', str(indexObjetos))
            isStatusMigration = bait_is_status_migration(bqtbl_bait_migracion_id, path_Id, directory_name)
        print('=====Finaliza Iteraciones Totales:'+str(iteraciones)+': Número de objetos migrados: ', str(indexObjetos))
    except Exception: 
        logger.error("fallo al ejecutar el método bait_init_migration: %s", bqtbl_bait_migracion_id, exc_info=1)
        raise    
    dateEnd = datetime.now()
    print('=====Info [bait_init_migration]: inicia migración [%s], hora inicio: %s.' %(bqtbl_bait_migracion_id, dateStart))
    print('=====Info [bait_init_migration]: finaliza migración [%s], hora fin: %s.' %(bqtbl_bait_migracion_id, dateEnd))
    print('=====Info [bait_init_migration]: Tiempo de ejecución: ',dateEnd-dateStart)

def bait_is_status_migration(bqtbl_bait_migracion_id, path_Id, directory_name):
    try:     
        result = FALSE
        QUERY = (
            "SELECT CASE "
            "    WHEN COUNT(A.objectId)>0 THEN TRUE "
            "    ELSE FALSE "
            "    END "
            "    AS result "
            "FROM {bqTable}  A "
            "WHERE A.pathId = '{pathId}' AND A.filePath LIKE '{directoryName}' " 
            #"WHERE A.pathId='{pathId}' AND A.statusMigration=0 " 
            "AND A.statusMigration=0 AND NOT A.fileName IS NULL " # statusMigration = 0 Pendiente
            ).format(bqTable=bqtbl_bait_migracion_id, pathId=path_Id, directoryName=directory_name)
        query_job = bigquery_client.query(QUERY)  
        rows = query_job.result() 
        for row in rows:
            result = row.result 
    except Exception: 
        logger.info('Error [function bait_is_status_migration]: fallo al consultar la tabla %s.' %
                    (bqtbl_bait_migracion_id))
        raise    
    return result

def bait_change_status_migration(ids, bqtbl_bait_migracion_id, idStatus):
    dateStart = datetime.now()
    try:        
        if idStatus == 1:
            QUERY = ("UPDATE {bqTable} A "
                "SET A.statusMigration = {id_status}, " # statusMigration = 1 EnProceso
                "A.dateStart = '{fechaMigration}' "
                "WHERE A.objectId IN {objects_Ids} "
                #"WHERE A.contentKey = '{contentKey}' "
                ).format(objects_Ids=ids, bqTable=bqtbl_bait_migracion_id, 
                        fechaMigration=dateStart, id_status=idStatus)
        else:
            QUERY = ("UPDATE {bqTable} A "
                "SET A.statusMigration = {id_status}, " # statusMigration = 2 Migrado
                "A.dateEnd = '{fechaMigration}' "
                "WHERE A.objectId IN {objects_Ids} "
                #"WHERE A.contentKey = '{contentKey}' "
                ).format(objects_Ids=ids, bqTable=bqtbl_bait_migracion_id, 
                        fechaMigration=dateStart, id_status=idStatus)
        QUERY = QUERY.replace('[','(')
        QUERY = QUERY.replace(']',')')
        #print("QUERY2: ",QUERY)
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        query_job = bigquery_client.query(QUERY, job_config=job_config)
        rows = query_job.result() 
        for row in rows:
            result = row.result 
    except Exception: 
        logger.info('Error [bait_change_status_migration]: fallo al actualizar la tabla %s.' %
                    (bqtbl_bait_migracion_id))
        raise    

def get_specific_file_from_huawei(contentKey, fileName, destinationFolderName):
    obsClient = ObsClient(access_key_id=HUAWEI_AK,
                      secret_access_key=HUAWEI_SK,
                      server=HUAWEI_SERVER,
                      is_secure=False,
                      )
    try:
        resp = obsClient.getObject(HUAWEI_BUCKET_NAME, contentKey, loadStreamInMemory=true) 
        if resp.status < 300: 
            huawei_upload_stream_to_gs(resp.body.buffer, fileName, destinationFolderName, contentKey)
        else: 
            print('=====ErrorCode:', resp.errorCode) 
            print('=====ErrorMessage:', resp.errorMessage)   
    except:
        import traceback
        logger.info(traceback.format_exc())

def huawei_upload_stream_to_gs(dataStream, file, destinationFolderName, contentKey):
    with open(GCP_PUBLIC_KEY_PATH_DEV) as source:fileCredentialsGcpDev = json.load(source)
    storage_credentials_dev = service_account.Credentials.from_service_account_info(fileCredentialsGcpDev)
    storage_client_dev = storage.Client(project=GCP_PROJECT_ID_DEV, credentials=storage_credentials_dev)
    try:
        if len(file)>40000000:
            bait_bucket_dev = storage_client_dev.get_bucket(GCP_BUCKET_NAME_DEV)
            new_blob = bait_bucket_dev.blob(destinationFolderName+file)
            new_blob._MAX_MULTIPART_SIZE = 30 * 1024 * 1024  # 30 MB
            new_blob._DEFAULT_CHUNKSIZE = 30 * 1024 * 1024  # 30 MB
            new_blob.upload_from_string(data=dataStream, timeout=300) 
            new_blob.up
        else:
            bait_bucket_dev = storage_client_dev.get_bucket(GCP_BUCKET_NAME_DEV)
            new_blob = bait_bucket_dev.blob(destinationFolderName+file)
            new_blob.upload_from_string(data=dataStream, timeout=300)           
    except Exception:
        bait_change_status_to_fail(contentKey, new_blob.size)
        logger.info('Error [huawei_upload_stream_to_gs]: fallo al subir el archivo %s a bucket de GCP %s.' %
                    (file, GCP_BUCKET_NAME_DEV))
        raise    
    storage_client_dev.close

def bait_change_status_to_fail(contentKey, fileSizeMigration):
    dateEnd = datetime.now()
    try:        
        if len(contentKey)>0:
            QUERY = ("UPDATE {bqTable} A "
                    "SET A.statusMigration = 99, " # statusMigration = 99 = Fail
                    "A.dateEnd = '{fechaMigration}', "
                    "A.fileSizeMigration = {fileSizeMigration} "
                    "WHERE A.contentKey = '{contentKey}' "
                    ).format(contentKey=contentKey, bqTable=GCP_BQTBL_BAIT_MIGRACION_ID, 
                            fileSizeMigration=fileSizeMigration, fechaMigration=dateEnd)
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            query_job = bigquery_client.query(QUERY, job_config=job_config)
            rows = query_job.result() 
            for row in rows:
                result = row.result 
    except Exception: 
        logger.info('Error [bait_change_status_to_fail]: fallo al actualizar la tabla %s.' %
                    (GCP_BQTBL_BAIT_MIGRACION_ID))
        raise    

def main():
    #DIRECTORY_NAME = "AÑO 2022/ENERO 2022/%" TERMINO EL 20220913
    #DIRECTORY_NAME = "AÑO 2022/ENERO 2022/%" #INICIA EL 20220913, TERMINO EL 20220919
    #DIRECTORY_NAME = "AÑO 2021/DICIEMBRE 2021/%" #INICIA EL 20220920, TERMINO EL 20220925
    DIRECTORY_NAME = "AÑO 2022/JUNIO 2022/%" #INICIA EL 20220920, TERMINO EL 20220925
    logger.info('Started Bait_1MigraHuaweiToGsMultiProcessing')
    print("The number of cores in the system is: ", cpu_count())
    bait_init_migration(GCP_BQTBL_BAIT_MIGRACION_ID, PATH_ANIO2022, DIRECTORY_NAME) 
    logger.info('Finished Bait_1MigraHuaweiToGsMultiProcessing')

#===============================================
if __name__ == "__main__":
    main()
