from ftplib import FTP

#domain name or server ip:
config = {
    'ftp_url' : 'test.rebex.net',
    'ftp_user': 'demo',
    'ftp_password' : 'password'
}

def get_ftp_connection(config):
    ftp = FTP(config['ftp_url'])
    ftp.login(user=config['ftp_user'], passwd=config['ftp_password'])
    return ftp

def download_file(ftp):
    filename = 'readme.txt'
    localfile = open(filename, 'wb')
    ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
    localfile.close()

    
def close_ftp_connection(ftp):
    try:
        ftp.quit()
    except Exception as e:
        ftp.close()

ftp = get_ftp_connection(config)
download_file(ftp)
close_ftp_connection(ftp)
