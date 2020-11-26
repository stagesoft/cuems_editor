import os
import shutil
import datetime
import uuid as uuid_module
from ..log import logger



def date_now_iso_utc():
    return datetime.datetime.utcnow().isoformat()


class StringSanitizer():

    @staticmethod
    def sanitize_text_size(_string):
        
        if _string and (len(_string) > 65535):
            _string = _string[0:65534] # return frist 255 characters
        return _string

    @staticmethod
    def sanitize_name(_string): #TODO: scape characters?
        if len(_string) > 255 :
            _string = _string[0:254] # return frist 255 characters
        return _string
    
    @staticmethod
    def sanitize_file_name(_string):
        if len(_string) >= 240 :
            _string = _string[0:236] + _string[-4:] # return frist 236 characters + last 4 chars = total 240 of max 255. Leave room for versioning and .tmp

        _string = _string.replace(' ', '_')
        _string = _string.replace('-', '_')
        keepcharacters = ('.','_')
        return "".join(c for c in _string if c.isalnum() or c in keepcharacters).rstrip().lower()

    @staticmethod
    def sanitize_dir_name(_string):
        if len(_string) >= 240 :
            _string = _string[0:236] + _string[-4:] # return frist 236 characters + last 4 chars = total 240 of max 255. Leave room for versioning and .tmp

        _string = _string.replace(' ', '_')
        _string = _string.replace('-', '_')
        keepcharacters = ('_')
        return "".join(c for c in _string if c.isalnum() or c in keepcharacters).rstrip().lower()
    
    @staticmethod
    def sanitize_dir_permit_increment(_string):
        if len(_string) >= 240 :
            _string = _string[0:236] + _string[-4:] # return frist 236 characters + last 4 chars = total 240 of max 255. Leave room for versioning and .tmp

        _string = _string.replace(' ', '_')
        keepcharacters = ('_', '-')
        return "".join(c for c in _string if c.isalnum() or c in keepcharacters).rstrip().lower()

class CopyMoveVersioned():

    @staticmethod
    def move(orig_path, dest_path, dest_filename=None):
        i = 0
        if dest_filename is None:
            dest_filename = os.path.basename(orig_path)
            
        (base, ext) = os.path.splitext(dest_filename)
        
        while True:     
            if not os.path.exists(os.path.join(dest_path, dest_filename)):
                logger.debug('moving file to: {}'.format(os.path.join(dest_path, dest_filename)))
                shutil.move( orig_path, os.path.join(dest_path, dest_filename))
                break
            else:
                i += 1
                dest_filename = base + "-{:03d}".format(i) + ext
                continue    
        return dest_filename

    @staticmethod
    def copy_dir(orig_path, dest_path, dest_dirname):
        i = 0
        orig_name = dest_dirname
        while True:     
            if not os.path.exists(os.path.join(dest_path, dest_dirname)):
                logger.debug('copyin dir to: {}'.format(os.path.join(dest_path, dest_dirname)))
                shutil.copytree( orig_path, os.path.join(dest_path, dest_dirname))
                break
            else:
                i += 1
                dest_dirname = orig_name + "-{:03d}".format(i)
                continue    
        return dest_dirname

class CuemsLibraryMaintenance():
    def __init__(self, library_path):
        self.library_path = library_path
