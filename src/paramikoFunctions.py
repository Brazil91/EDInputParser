# -*- coding: utf-8 -*-
import stat  # Import the stat module for file type checking
from .__init__ import pp, paramiko, re

def listFilesRecursive(sftp, remote_dir):
    """
    Recursively list all files in a remote directory using SFTP.
    
    Parameters:
        sftp (paramiko.SFTPClient): The SFTP client object.
        remote_dir (str): The remote directory to list files from.
    
    Returns:
        list: A list of all file paths in the directory and its subdirectories.
    """
    files = []
    for entry in sftp.listdir_attr(remote_dir):
        remote_path = f"{remote_dir}/{entry.filename}"
        # Check if the entry is a directory
        if stat.S_ISDIR(entry.st_mode):
            # Recursively list files in subdirectories
            files.extend(listFilesRecursive(sftp, remote_path))
        else:
            # Add file to the list
            files.append(entry.filename)
    return files

def listDirs(sftp, remote_dir):
    """
    Recursively list all directories in a remote directory using SFTP.

    Parameters:
        sftp (paramiko.SFTPClient): The SFTP client object.
        remote_dir (str): The remote directory to list files from.
    
    Returns:
        list: A list of all directory paths in the directory and its subdirectories.
    """
    config = []
    for entry in sftp.listdir_attr(remote_dir):
        remote_path = f"{remote_dir}/{entry.filename}"
        # Check if the entry is a directory
        if stat.S_ISDIR(entry.st_mode):
            # Recursively list directories in subdirectories
            config.append(entry.filename)
    return config
    
def transportConnector(host, user, password, port=22):
    """
    Connect to the SFTP server.
    """
    print(f"Connecting to {host} as {user}")  # Debug-Ausgabe
    print(f"Password is None: {password is None}, Length: {len(password) if password else 0}")

    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=password)
    except Exception as e:
        print("\nFehler beim Verbinden:", e)
        import sys
        sys.exit(f"Fehler beim Verbinden:\n\n{e}")
    return transport

def getOJNsFromRemoteAusgang(sftp, remoteAusgang, fetchCount: int=None):
    ojnsDict = {}
    for n, filename in enumerate(sftp.listdir(remoteAusgang)):
        
        if fetchCount:
            if n + 1 > fetchCount:
                break
        
        if filename.endswith(".arc"):
            continue
        
        # print("Remote-Datei:", filename)
        # Beispiel: Datei nach ~/.local kopieren
        remote_path = f"{remoteAusgang}/{filename}"
        
        with sftp.open(remote_path, 'r') as remote_file:
            content = remote_file.read().decode(errors="ignore")
            for line in content.split("\n"):
                
                match = re.search(r"'A8(\d+)'", line)
                if match:
                    ojnsDict[match.group(1)] = match.group(1) + "_" +filename
                    continue
    return ojnsDict


def getWOsFromRemoteAusgang(sftp, remoteAusgang, fetchCount: int=None):
    wosDict = {}
    for n, filename in enumerate(sftp.listdir(remoteAusgang)):
        
        if fetchCount:
            if n + 1 > fetchCount:
                break
        
        if filename.endswith(".arc"):
            continue
        
        # print("Remote-Datei:", filename)
        # Beispiel: Datei nach ~/.local kopieren
        remote_path = f"{remoteAusgang}/{filename}"
        
        with sftp.open(remote_path, 'r') as remote_file:
            content = remote_file.read().decode(errors="ignore")
            for line in content.split("\n"):
                
                match = re.search(r"'Z1([A-Za-z0-9_.-]+)'", line)
                if match:
                    wosDict[match.group(1)] = match.group(1) + "_" +filename
                    continue
    return wosDict