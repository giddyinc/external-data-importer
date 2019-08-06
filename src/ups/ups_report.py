from ftplib import FTP

#domain name or server ip:
ftp = FTP('ftp://test.rebex.net/')
ftp.login(user='demo', passwd = 'password')

def grabFile():
    filename = 'readme.txt'
    localfile = open(filename, 'wb')
    ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
    ftp.quit()
    localfile.close()
    
