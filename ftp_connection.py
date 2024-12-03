from ftplib import FTP


def connect_and_list_files(host, username, password, port=21, path='/'):
    """
    Connects to an FTP server and lists files in the specified directory.
    Arguments:
    - host: FTP server host.
    - username: FTP username.
    - password: FTP password.
    - port: FTP port (default is 21).
    - path: Directory path to list files (default is root '/').
    Returns:
    - Dictionary with success or error message and file list (if successful).
    """
    try:
        # Establish FTP connection
        ftp = FTP()
        ftp.connect(host, port)
        ftp.login(user=username, passwd=password)

        # Change to the specified directory
        ftp.cwd(path)

        # List files in the directory
        files = ftp.nlst()

        # Close the connection
        ftp.quit()

        return {"message": "Connection successful", "files": files}

    except Exception as e:
        return {"error": str(e)}
