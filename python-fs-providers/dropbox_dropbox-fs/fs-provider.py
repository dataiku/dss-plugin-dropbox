from dataiku.fsprovider import FSProvider

import os, shutil, re, logging
import dropbox

try:
    from BytesIO import BytesIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='confluence plugin %(levelname)s - %(message)s')

class DropboxFSProvider(FSProvider):
    
    CHUNK_SIZE = 4 * 1024 * 1024

    def __init__(self, root, config, plugin_config):
        """
        :param root: the root path for this provider
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        if len(root) > 0 and root[0] == '/':
            root = root[1:]
        self.root = root
        self.root_lnt = self.get_normalized_path(root)
        self.provider_root = '/'
        self.connection = plugin_config.get("dropbox_connection")
        self.access_token = self.connection['access_token']
        self.dbx = dropbox.Dropbox(self.access_token)

    # util methods
    def get_rel_path(self, path):
        if len(path) > 0 and path[0] == '/':
            path = path[1:]
        return path
    def get_normalized_path(self, path):
        if len(path) == 0 or path == '/':
            return '/'
        elts = path.split('/')
        elts = [e for e in elts if len(e) > 0]
        return '/' + '/'.join(elts)
    def get_full_path(self, path):
        path_elts = [self.provider_root, self.get_rel_path(self.root), self.get_rel_path(path)]
        path_elts = [e for e in path_elts if len(e) > 0]
        return os.path.join(*path_elts)

    def close(self):
        """
        Perform any necessary cleanup
        """
        logger.info('closing dropbox session')

    def stat(self, path):
        """
        Get the info about the object at the given path inside the provider's root, or None 
        if the object doesn't exist
        """
        full_path = self.get_full_path(path)

        item = None

        try:
            item = self.dbx.files_get_metadata(self.get_normalized_path(full_path))
        except Exception as error:
            logger.info("Dropbox API error :{}".format(error))
            return None

        if item is None:
            return None

        if full_path == "" or self.is_folder(item):
            return {'path': self.get_normalized_path(path), 'size':0, 'lastModified': 0, 'isDirectory':True}
        elif self.is_file(item):
            return {'path': self.get_normalized_path(path), 'size': item.size, 'isDirectory': False}
        else:
            return None

    def set_last_modified(self, path, last_modified):
        """
        Set the modification time on the object denoted by path. Return False if not possible
        """
        return False
        
    def browse(self, path):
        """
        List the file or directory at the given path, and its children (if directory)
        """
        full_path = self.get_full_path(path)
        item = None
        if full_path == '/':
            full_path = ""

        if full_path != "":
            try:
                item = self.dbx.files_get_metadata(full_path)
            except dropbox.exceptions.ApiError as error:
                logger.info("ALX:Dropbox API error {}".format(error))
        
        if full_path == "" or self.is_folder(item):
            children = []
            for sub in self.dbx.files_list_folder(full_path).entries:
                sub_path = self.get_normalized_path(os.path.join(path, sub.name))
                if self.is_folder(sub):
                    children.append({'fullPath': sub_path, 'exists':True, 'directory':True, 'size':0})
                else:
                    children.append({'fullPath': sub_path, 'exists':True, 'directory':False, 'size':sub.size})
            return {'fullPath' : self.get_normalized_path(path), 'exists' : True, 'directory' : True, 'children' : children}
        elif self.is_file(item):
            item = self.dbx.files_get_metadata(full_path)
            return {'fullPath' : self.get_normalized_path(path), 'exists' : True, 'directory' : False, 'size' : item.size}
        if item is None:
            return {'fullPath' : None, 'exists' : False}

    def is_file(self, item):
        return isinstance(item, dropbox.files.FileMetadata)

    def is_folder(self, item):
        return isinstance(item, dropbox.files.FolderMetadata)

    def enumerate(self, path, first_non_empty):
        """
        Enumerate files recursively from prefix. If first_non_empty, stop at the first non-empty file.
        
        If the prefix doesn't denote a file or folder, return None
        """
        full_path = self.get_full_path(path)

        item = None
        try:
            item = self.dbx.files_get_metadata(self.get_normalized_path(full_path))
        except dropbox.exceptions.ApiError as error:
            logger.info("Dropbox API error {}".format(error))
        if item == None:
            return None
        paths = []
        if self.is_file(item):
            paths = [{'path':self.get_lnt_path(path).split("/")[-1], 'size': item.size}]
        if self.is_folder(item):
            for sub in self.dbx.files_list_folder(full_path, recursive=True).entries:
                if self.is_file(sub):
                    sub_size = sub.size
                    paths.append({'path':self.get_normalized_path(path + self.substract_path_base(full_path, sub.path_display)), 'size':sub_size})
        return paths

    def substract_path_base(self, base, path):
        return re.sub(r'^' + base + r'([a-zA-Z0-9\-_/\.]+)', r'\1', path)

    def delete_recursive(self, path):
        """
        Delete recursively from path. Return the number of deleted files (optional)
        """
        full_path = self.get_full_path(path)
        try:
            self.dbx.files_delete(self.get_normalized_path(full_path))
            return 1
        except dropbox.exceptions.ApiError as error:
            if error.error.is_path() and error.error.get_path()._tag == 'not_found':
                return 0
            else:
                raise Exception('Error while deleting "{0}" : {1}'.format(path, error))

            
    def move(self, from_path, to_path):
        """
        Move a file or folder to a new path inside the provider's root. Return false if the moved file didn't exist
        """
        full_from_path = self.get_full_path(from_path)
        full_to_path = self.get_full_path(to_path)
        try:
            self.dbx.files_move(from_path=full_from_path, to_path=full_to_path)
        except dropbox.exceptions.ApiError as error:
            if error.error.is_path() and error.error.get_path()._tag == 'not_found':
                return False
            else:
                raise
        return True

    def read(self, path, stream, limit):
        """
        Read the object denoted by path into the stream. Limit is an optional bound on the number of bytes to send
        """
        full_path = self.get_full_path(path)
        metadata, res = self.dbx.files_download(full_path)
        sio = BytesIO(res.content)
        shutil.copyfileobj(sio, stream)

    def write(self, path, stream):
        """
        Write the stream to the object denoted by path into the stream
        """
        full_path = self.get_full_path(path)
        try:
            sio = BytesIO()
            shutil.copyfileobj(stream, sio)

            file_size = self.file_size(sio)
            sio.seek(0)

            if file_size <= self.CHUNK_SIZE:
                self.dbx.files_upload(sio.read(), full_path, mute=True)
            else:
                upload_session_start_result = self.dbx.files_upload_session_start(sio.read(self.CHUNK_SIZE))
                cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,
                                                        offset=sio.tell())
                commit = dropbox.files.CommitInfo(path=full_path)

                while sio.tell() < file_size:
                    if ((file_size - sio.tell()) <= self.CHUNK_SIZE):
                        self.dbx.files_upload_session_finish(sio.read(self.CHUNK_SIZE),
                                                        cursor,
                                                        commit)
                    else:
                        self.dbx.files_upload_session_append(sio.read(self.CHUNK_SIZE),
                                                        cursor.session_id,
                                                        cursor.offset)
                        cursor.offset = sio.tell()

        except dropbox.exceptions.ApiError as error:
            logger.error("Dropbox API error :{0}".format(error))

    def file_size(self, file_handle):
        file_handle.seek(0, 2)
        return file_handle.tell()