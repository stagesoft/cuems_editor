import os
from cuemsutils.log import logged, Logger


class CuemsLibraryMaintenance():
    """Ensures the library directory tree exists before any DB or file operation.

    Creates the following structure under ``library_path`` on first run::

        <library_path>/
        ├── media/
        │   ├── thumbnails/
        │   └── waveforms/
        ├── projects/
        └── trash/
            ├── media/
            │   ├── thumbnails/
            │   └── waveforms/
            └── projects/

    Call once at service startup, before ``CuemsDBManager`` is constructed.

    Example:
        >>> CuemsLibraryMaintenance("/var/lib/cuems/library")
    """

    def __init__(self, library_path):
        """Bootstrap the library directory tree.

        Args:
            library_path: Absolute path to the root library directory.
                          Created if it does not exist.
        """
        self.library_path = library_path
        library_directories = ['media', 'media/thumbnails', 'media/waveforms', 'projects']
        trahsh_directories = [os.path.join('trash', directory) for directory in library_directories]
        library_directories.extend(trahsh_directories)
        library_directory_paths = [os.path.join(self.library_path, directory) for directory in library_directories]
        library_directory_paths.append(os.path.normpath(self.library_path))  # Ensure the root library path is also included
        self.ensure_directories(library_directory_paths)

    def ensure_directories(self, directories):
        """Create any missing directories from *directories*.

        Args:
            directories: Iterable of absolute directory paths to verify/create.
        """
        for directory_path in directories:
            if not os.path.exists(directory_path):
                self.create_directory(directory_path)

    @logged
    def create_directory(self, directory_path):
        """Create *directory_path* and all intermediate parents.

        Args:
            directory_path: Absolute path to create.
        """
        os.makedirs(directory_path)
