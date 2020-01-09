import os
import sys
import logging
import utils
import boto3
import process_file
import psycopg2


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, \
    format='%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s', \
    datefmt="%Y-%m-%d %H:%M:%S", \
    stream=sys.stdout)


def s3_connect(accessKey, secretKey):
    client = boto3.client(
        's3',
        aws_access_key_id=accessKey,
        aws_secret_access_key=secretKey
    )
    return client

def get_s3_files_list(s3_conn,bucket,prefix):
    files = []
    theobjects = s3_conn.list_objects_v2(Bucket=bucket, Prefix=prefix )
    for object in theobjects['Contents']:
        directoryName = object['Key']
        filename = directoryName.split("/")[-1]
        LOG.info("Directory %s --File %s " % (directoryName,filename))
        files.append(filename)
    return files


def download_file_from_s3(s3_conn,bucket,prefix, filename, filepath):
    s3_conn.download_file(bucket, prefix+filename, filepath)
    LOG.info("download_file_from_s3 %s / %s" % (bucket, prefix+filename))

def upload_file_to_s3(s3_conn,bucket,prefix, filename, source_file):
    s3_conn.upload_file(source_file,bucket,prefix+filename)
    LOG.info("upload_file_to_s3 %s / %s" % (bucket, prefix+filename))

def copy_into_redshift(config, s3_key, fieldnames):
    db_config = config['redshift']
    with psycopg2.connect(dbname=db_config["database"], user=db_config["user"], host=db_config["host"],port=db_config["port"],password=db_config["password"] ) as rs_conn:
        LOG.info("redshift connected to host %s with user %s" % (db_config["host"],db_config["user"]))
        LOG.info("copying to %s.%s" % (config['ups']['schema']['schema_name'],config['ups']['schema']['weekly_table_name']))
        with rs_conn.cursor() as rs_cur:
            query_template = "COPY %s.%s (%s)    FROM 's3://%s/%s' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' CSV DELIMITER ',' IGNOREBLANKLINES IGNOREHEADER 1 TRUNCATECOLUMNS"
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

    bucket = config["ups"]['S3']['BUCKET']
    s3_copy_folder = config['ups']['S3']['COPY_DIR']
    prefix = "ups/collected_initial/"
    final_prefix = config['ups']['S3']['UPLOAD_DIR']

    fieldnames = config['ups']["required_file_cols"]
    fieldnames.extend(config['ups']["merge_file_columns"])
    fieldnames.extend(config['ups']["meta_columns"])

    local_path = config["ups"]["FILE_DUMP_TEMP_DIR"]
    s3_conn = s3_connect(config['AWS']['ACCESSKEY'], config['AWS']['SECRETKEY'])
    LOG.info("got s3 connection")

    files = get_s3_files_list(s3_conn,bucket,prefix)
    LOG.info("number of files found %s " % len(files))

    for file in files:
        LOG.info("-------------  processing file %s ----------------------------" % file)
        filepath = local_path+file
        if(file.startswith('QVD_OUT_BOXED')):
            download_file_from_s3(s3_conn,bucket,prefix, file, filepath)
            upload_file_to_s3(s3_conn,bucket,final_prefix+"QVD/", file, filepath)
            os.remove(filepath)
        elif(file.startswith('UBD_SPK_WKY_BOXED')):
            download_file_from_s3(s3_conn,bucket,prefix, file, filepath)
            upload_file_to_s3(s3_conn,bucket,final_prefix+"UPS_WEEKLY/", file, filepath)
            process_file.process_file(local_path,file,file+"_processed","s3://%s/%sUPS_WEEKLY/" % (bucket,final_prefix))
            copy_into_redshift(config, final_prefix+"UPS_WEEKLY/"+file+"_processed", fieldnames)
            os.remove(filepath)
        else:
            LOG.info("Unexpected file type %s found " % (file))
        LOG.info("-------------  processed file %s ----------------------------" % file)

if __name__=="__main__":
    get_data()