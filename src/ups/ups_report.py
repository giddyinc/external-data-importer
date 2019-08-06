from ftplib import FTP

#domain name or server ip:
ftp_url = 'test.rebex.net'
ftp_user = 'demo'
ftp_password = 'password'

ftp = FTP(ftp_url)
ftp.login(user=ftp_user, passwd=ftp_password)

def grabFile():
    filename = 'readme.txt'
    localfile = open(filename, 'wb')
    ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
    ftp.quit()
    localfile.close()
    


with FTP(ftp_url) as ftp:    
    try:
        ftp.login(user=ftp_user, passwd=ftp_password)
        files = []
        ftp.dir(files.append)
        print(files)            
    except ftplib.all_errors as e:
        print('FTP error:', e)
