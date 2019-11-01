import os
import sys
import logging
import utils
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

def get_data():
    LOG.info('App started')
    APP_HOME = os.environ['APP_HOME']
    APP_ENV = os.environ['APP_ENV']
    SECRET_PATH = os.environ['SECRET_PATH']
    secret_file_path = SECRET_PATH+"secrets."+APP_ENV+".json"
    config_file_path = APP_HOME+"src/config/config."+APP_ENV+".json"

    config = utils.load_config( secret_file_path,config_file_path )
    LOG.info("loaded config with database host:%s and user:%s" % (config['redshift']['host'],config['redshift']['user']))

    ftp = get_ftp_connection(config["ups"]["ftp"])
    LOG.info("got ftp connection")
    ftp_path=get_current_ftp_directory_path(ftp)
    ftp_archive_folder_path = ftp_path+config["ups"]["ftp"]["archive_folder"]
    local_path = config["ups"]["FILE_DUMP_TEMP_DIR"]

    s3_conn = utils.s3_connect(config['AWS']['ACCESSKEY'], config['AWS']['SECRETKEY'])
    LOG.info("got s3 connection")

    files = get_files_list(ftp)

    if(len(files) < 2):
        LOG.info("No new files found")

    for file in files:
        if(file.startswith('QVD_OUT_BOXED') or file.startswith('UBD_SPK_WKY_BOXED')):
            local_filepath = local_path+file
            download_file(ftp, ftp_path, file, local_filepath)
            s3_key_name = os.path.join(config["ups"]['S3']['UPLOAD_DIR'],file)
            utils.s3_upload(s3_conn, config["ups"]['S3']['BUCKET'], s3_key_name, local_filepath)
            ftp.rename(file, ftp_archive_folder_path+file)
            os.remove(local_filepath)
            LOG.info("Moved file - %s from ftp to s3 bucket %s at %s" % (file, config["ups"]['S3']['BUCKET'],s3_key_name ))
        else:
            LOG.info("Unexpected file type %s found on FTP server" % (file))
    close_ftp_connection(ftp)

if __name__=="__main__":
    get_data()