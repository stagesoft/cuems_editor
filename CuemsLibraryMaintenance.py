import os
from cuemsutils.log import logged, Logger

class CuemsLibraryMaintenance():
    def __init__(self, library_path):
        self.library_path = library_path
        library_directories = ['media', 'media/thumbnails', 'media/waveforms', 'projects']
        trahsh_directories = [os.path.join('trash', directory) for directory in library_directories]
        library_directories.extend(trahsh_directories)
        library_directory_paths = [os.path.join(self.library_path, directory) for directory in library_directories]
        library_directory_paths.append(os.path.normpath(self.library_path))  # Ensure the root library path is also included
        self.ensure_directories(library_directory_paths)

    def ensure_directories(self, directories):
        for directory_path in directories:
            if not os.path.exists(directory_path):
                self.create_directory(directory_path)
            
    @logged
    def create_directory(self, directory_path):
        os.makedirs(directory_path)