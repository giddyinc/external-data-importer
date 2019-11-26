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

def download_file(ftp, ftp_path, ftp_filename, filepath):
    localfile = open(filepath, 'wb')
    ftp.cwd(ftp_path)
    ftp.retrbinary('RETR ' + ftp_filename, localfile.write, 1024)
    localfile.close()

def get_current_ftp_directory_path(ftp):
    ftp_path = ftp.pwd()
    return ftp_path

def close_ftp_connection(ftp):
    try:
        ftp.quit()
    except Exception as e:
        ftp.close()

def move_file_to_s3(local_path,file,ftp, ftp_path,ftp_archive_folder_path,config):
    local_filepath = local_path+file
    download_file(ftp, ftp_path, file, local_filepath)
    s3_key_name = os.path.join(config["ups"]['S3']['UPLOAD_DIR'],file)
    s3_conn = utils.s3_connect(config['AWS']['ACCESSKEY'], config['AWS']['SECRETKEY'])
    utils.s3_upload(s3_conn, config["ups"]['S3']['BUCKET'], s3_key_name, local_filepath)
    ftp.rename(file, ftp_archive_folder_path+file)
    LOG.info("Moved file - %s from ftp to s3 bucket %s at %s" % (file, config["ups"]['S3']['BUCKET'],s3_key_name ))


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

    if(len(files) < 2):
        LOG.info("No new files found")

    s3_bucket = config['ups']['S3']['BUCKET']
    s3_copy_folder = config['ups']['S3']['COPY_DIR']
    fieldnames = config['ups']["required_file_cols"]
    fieldnames.extend(config['ups']["merge_file_columns"])
    fieldnames.extend(config['ups']["meta_columns"])
    for file in files:
        if(file.startswith('QVD_OUT_BOXED')):
            move_file_to_s3(local_path,file,ftp, ftp_path,ftp_archive_folder_path,config)
        elif(file.startswith('UBD_SPK_WKY_BOXED')):
            move_file_to_s3(local_path,file,ftp, ftp_path,ftp_archive_folder_path,config)
            process_file.process_file(local_path,file,"s3://%s/%s" % (s3_bucket,s3_copy_folder))
            
            copy_into_redshift(config, s3_copy_folder+file, fieldnames)
            #os.remove(local_filepath)
        else:
            LOG.info("Unexpected file type %s found on FTP server" % (file))
        LOG.info("-------------  processed file ----------------------------")
    close_ftp_connection(ftp)

if __name__=="__main__":
    get_data()