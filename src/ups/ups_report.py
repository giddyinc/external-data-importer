import os
import sys
import logging
import pandas as pd
import re
import s3fs
import utils
import process_file
import psycopg2
from ftplib import FTP_TLS


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, \
    format='%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s', \
    datefmt="%Y-%m-%d %H:%M:%S", \
    stream=sys.stdout)


def get_ftp_connection(config):
    ftp = FTP_TLS()
    ftp.debugging = 0
    ftp.connect(config['ftp_url'])
    ftp.login(config['ftp_user'], config['ftp_password'])
    ftp.prot_p()
    return ftp

def get_files_list(ftp):
    files = ftp.nlst()
    return files

def download_file_from_ftp(ftp, ftp_path, ftp_filename, filepath):
    localfile = open(filepath, 'wb')
    ftp.cwd(ftp_path)
    ftp.retrbinary('RETR ' + ftp_filename, localfile.write, 1024)
    localfile.close()

def move_file_to_ftp_archive(ftp,file,ftp_archive_folder_path):
    ftp.rename(file, ftp_archive_folder_path+file)

def get_current_ftp_directory_path(ftp):
    ftp_path = ftp.pwd()
    return ftp_path

def close_ftp_connection(ftp):
    try:
        ftp.quit()
    except Exception as e:
        ftp.close()

def copy_into_redshift(config, s3_key, fieldnames):
    db_config = config['redshift']
    with psycopg2.connect(dbname=db_config["database"], user=db_config["user"], host=db_config["host"],port=db_config["port"],password=db_config["password"] ) as rs_conn:
        LOG.info("redshift connected to host %s with user %s" % (db_config["host"],db_config["user"]))
        LOG.info("copying to %s.%s" % (config['ups']['schema']['schema_name'],config['ups']['schema']['weekly_table_name']))
        with rs_conn.cursor() as rs_cur:
            query_template = "COPY %s.%s (%s)    FROM 's3://%s/%s' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' CSV DELIMITER ',' TIMEFORMAT 'YYYY-MM-DD HH:MI:SS' IGNOREBLANKLINES IGNOREHEADER 1 TRUNCATECOLUMNS"
            query = query_template % (
                    config['ups']['schema']['schema_name'],
                    config['ups']['schema']['weekly_table_name'],
                    ",".join(fieldnames),
                    config['ups']['S3']['BUCKET'],
                    s3_key,
                    config['AWS']['ACCESSKEY'],
                    config['AWS']['SECRETKEY'])
            LOG.info(query)
            try:
                rs_cur.execute( query )
                rs_conn.commit()
            except Exception as e:
                rs_conn.rollback()
                LOG.error("Error copying s3 file for %s to redshift with error %s and query:\n%s\n" % (str(e), config['ups']['schema']['weekly_table_name'], query))
                raise e
    rs_conn.close()


def get_data():
    LOG.info('App started')
    APP_HOME = os.environ['APP_HOME']
    APP_ENV = os.environ['APP_ENV']
    SECRET_PATH = os.environ['SECRET_PATH']
    secret_file_path = SECRET_PATH+"secrets."+APP_ENV+".json"
    config_file_path = APP_HOME+"src/config/config."+APP_ENV+".json"

    config = utils.load_config( secret_file_path,config_file_path )
    LOG.info("loaded config with database host:%s and user:%s" % (config['redshift']['host'],config['redshift']['user']))

    os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['ACCESSKEY']
    os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['SECRETKEY']

    ftp = get_ftp_connection(config["ups"]["ftp"])
    LOG.info("got ftp connection")
    ftp_path=get_current_ftp_directory_path(ftp)
    ftp_archive_folder_path = ftp_path+config["ups"]["ftp"]["archive_folder"]
    local_path = config["ups"]["FILE_DUMP_TEMP_DIR"]

    files = get_files_list(ftp)
    LOG.info("number of files found %s " % len(files))
    close_ftp_connection(ftp)

    if(len(files) < 2):
        LOG.info("No new files found")

    s3_bucket = config['ups']['S3']['BUCKET']
    s3_copy_folder = config['ups']['S3']['UPLOAD_DIR']
    fieldnames = config['ups']["required_file_cols"]
    fieldnames.extend(config['ups']["merge_file_columns"])
    fieldnames.extend(config['ups']["meta_columns"])
    merge_csv_path = config['ups']['merge_csv_path']

    for file in files:
        LOG.info("-------------  processing file %s ----------------------------" % file)
        ftp = get_ftp_connection(config["ups"]["ftp"])
        local_file_path = local_path+file
        s3_conn = utils.s3_connect(config['AWS']['ACCESSKEY'], config['AWS']['SECRETKEY'])

        if(file.startswith('QVD_OUT_BOXED')):
            download_file_from_ftp(ftp, ftp_path, file,local_file_path )
            utils.upload_file_to_s3(s3_conn,s3_bucket,s3_copy_folder+config['ups']['S3']['QVD_RAW']+"/", file, local_file_path)
            move_file_to_ftp_archive(ftp,file,ftp_archive_folder_path)
            os.remove(local_file_path)
        elif(file.startswith('UBD_SPK_WKY_BOXED')):
            ups_raw = config['ups']['S3']['UPS_WEEKLY_RAW']
            ups_processed = config['ups']['S3']['UPS_WEEKLY_PROCESSED']
            download_file_from_ftp(ftp, ftp_path, file,local_file_path )
            utils.upload_file_to_s3(s3_conn,s3_bucket,s3_copy_folder+ups_raw+"/", file, local_file_path)
            process_file.process_file(local_path,file,file+"_processed","s3://%s/%s%s/" % (s3_bucket,s3_copy_folder,ups_processed), merge_csv_path)
            LOG.info("processed_file")
            copy_into_redshift(config, s3_copy_folder+ups_processed+"/"+file+"_processed" , fieldnames)
            LOG.info("uploaded to redshift")
            move_file_to_ftp_archive(ftp,file,ftp_archive_folder_path)
            LOG.info("move ftp file to archive")
            os.remove(local_file_path)
        else:
            LOG.info("Unexpected file type %s found on FTP server" % (file))
        close_ftp_connection(ftp)
        LOG.info("-------------  processed file %s ----------------------------" % file)

if __name__=="__main__":
    get_data()
