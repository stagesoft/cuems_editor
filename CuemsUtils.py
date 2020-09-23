import os
import shutil
from ..log import logger
LIBRARY_PATH = os.path.join('/home/ion/Documents', 'cuems_library')


class StringSanitizer():
    
    @staticmethod
    def sanitize(_string):
        if len(_string) >= 240 :
            _string = _string[0:236] + _string[-4:] # return frist 236 characters + last 4 chars = total 240 of max 255. Leave room for versioning and .tmp

        _string = _string.replace(' ', '_')
        _string = _string.replace('-', '_')
        keepcharacters = ('.','_')
        return "".join(c for c in _string if c.isalnum() or c in keepcharacters).rstrip().lower()

class MoveVersioned():

    @staticmethod
    def move(orig_path, dest_path, dest_filename):
        i = 0
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
# %%