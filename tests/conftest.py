"""pytest bootstrap for cuems-editor.

Ensures the package under ``src/`` is importable when the editor is not
pip-installed, and falls back to a sibling ``cuems-utils/src`` checkout for
``cuemsutils`` in a dev tree (in CI / on a deployed host both are installed).
"""
import os
import sys

_here = os.path.dirname(__file__)
_src = os.path.abspath(os.path.join(_here, '..', 'src'))
if _src not in sys.path:
    sys.path.insert(0, _src)

try:
    import cuemsutils  # noqa: F401
except ImportError:
    _utils_src = os.path.abspath(
        os.path.join(_here, '..', '..', 'cuems-utils', 'src'))
    if os.path.isdir(_utils_src):
        sys.path.insert(0, _utils_src)

import shutil

import pytest

FIXTURE_XML = os.path.join(_here, 'fixtures', 'script_minimal.xml')

# Canned durations keyed by media unix_name (seconds). Used to monkeypatch
# probe_duration so tests need no ffprobe / real media bytes.
CANNED_SECONDS = {
    'file.ext': 83.456,          # AUDIO ref in the fixture
    'file_video.ext': 90.0,      # VIDEO ref in the fixture
    'tone_c.wav': 3.0,           # trashed media
}


class LibraryCtx:
    def __init__(self, root, db_path):
        self.root = root
        self.db_path = db_path
        self.projects_dir = os.path.join(root, 'projects')

    def connect(self):
        from cuemseditor.CuemsDBModel import database
        database.init(self.db_path)
        database.connect(reuse_if_open=True)
        return database

    def durations(self):
        """Return {unix_name: duration} snapshot (opens/closes its own conn)."""
        from cuemseditor.CuemsDBModel import Media, database
        self.connect()
        out = {m.unix_name: m.duration for m in Media.select()}
        database.close()
        return out


@pytest.fixture
def library(tmp_path):
    """A throwaway CueMS library: dir tree + seeded sqlite + one project XML.

    Leaves the DB connection CLOSED so the repair script's main() can open it.
    """
    from cuemseditor.CuemsDBModel import Project, Media, ProjectMedia, database
    from cuemsutils.helpers import new_uuid, new_datetime

    root = str(tmp_path)
    for sub in ('media', 'trash/media', 'projects/proj1', 'trash/projects'):
        os.makedirs(os.path.join(root, sub), exist_ok=True)
    # media files that "exist" (probe is mocked; bytes irrelevant)
    for name in ('file.ext', 'file_video.ext', 'pic.png'):
        open(os.path.join(root, 'media', name), 'wb').close()
    open(os.path.join(root, 'trash', 'media', 'tone_c.wav'), 'wb').close()
    shutil.copy(FIXTURE_XML, os.path.join(root, 'projects', 'proj1', 'script.xml'))

    db_path = os.path.join(root, 'project-manager.db')
    database.init(db_path)
    database.connect()
    database.create_tables([Project, Media, ProjectMedia], safe=True)

    Project.create(uuid=str(new_uuid()), name='Proj One', unix_name='proj1',
                   created=new_datetime(), modified=new_datetime(), in_trash=False)
    # file.ext: wrong duration -> CHANGED; file_video.ext: NULL -> NULL_FILLED
    Media.create(uuid=str(new_uuid()), name='file.ext', unix_name='file.ext',
                 created=new_datetime(), modified=new_datetime(),
                 duration='00:00:00.000', media_type='AUDIO', in_trash=False)
    Media.create(uuid=str(new_uuid()), name='file_video.ext', unix_name='file_video.ext',
                 created=new_datetime(), modified=new_datetime(),
                 duration=None, media_type='MOVIE', in_trash=False)
    Media.create(uuid=str(new_uuid()), name='pic.png', unix_name='pic.png',
                 created=new_datetime(), modified=new_datetime(),
                 duration=None, media_type='IMAGE', in_trash=False)
    Media.create(uuid=str(new_uuid()), name='tone_c.wav', unix_name='tone_c.wav',
                 created=new_datetime(), modified=new_datetime(),
                 duration='00:00:03.9', media_type='AUDIO', in_trash=True)
    Media.create(uuid=str(new_uuid()), name='gone.wav', unix_name='gone.wav',
                 created=new_datetime(), modified=new_datetime(),
                 duration='00:00:01.000', media_type='AUDIO', in_trash=False)
    database.close()

    yield LibraryCtx(root, db_path)

    if not database.is_closed():
        database.close()


@pytest.fixture
def canned_probe(monkeypatch):
    """Patch repair_durations.probe_duration with the CANNED_SECONDS map."""
    from cuemsutils.tools.CTimecode import CTimecode
    import cuemseditor.repair_durations as rd

    def _fake(path):
        name = os.path.basename(path)
        if name not in CANNED_SECONDS:
            from cuemseditor.CuemsErrors import NotTimeCodeError
            raise NotTimeCodeError(f'no canned duration for {name}')
        return CTimecode(start_seconds=CANNED_SECONDS[name])

    monkeypatch.setattr(rd, 'probe_duration', _fake)
    return _fake
