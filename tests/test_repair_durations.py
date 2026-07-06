"""Tests for the repair_durations script (Pass A DB + Pass B XML)."""
import hashlib
import os

import cuemseditor.repair_durations as rd
from cuemsutils.xml.XmlReaderWriter import XmlReaderWriter


def _xml_durations(path):
    d = XmlReaderWriter(schema_name='script', xmlfile=path).read()
    out = []

    def walk(n):
        if isinstance(n, dict):
            if n.get('file_name'):
                out.append(n.get('duration'))
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for i in n:
                walk(i)

    walk(d)
    return out


def _file_hash(path):
    with open(path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


def _script_path(library):
    return os.path.join(library.projects_dir, 'proj1', 'script.xml')


def test_dry_run_changes_nothing(library, canned_probe):
    before = library.durations()
    xml_before = _file_hash(_script_path(library))

    rc = rd.main(['--library-path', library.root])

    assert library.durations() == before          # DB untouched
    assert _file_hash(_script_path(library)) == xml_before  # XML untouched
    assert rc == 1                                  # gone.wav MISSING -> exit 1


def test_apply_fixes_db_and_xml(library, canned_probe):
    rc = rd.main(['--library-path', library.root, '--apply'])

    durations = library.durations()
    assert durations['file.ext'] == '00:01:23.456'      # CHANGED
    assert durations['file_video.ext'] == '00:01:30.000'  # NULL_FILLED
    assert durations['gone.wav'] == '00:00:01.000'       # MISSING -> untouched
    assert durations['pic.png'] is None                  # IMAGE skipped

    xml_durs = _xml_durations(_script_path(library))
    assert '00:00:00.000' not in xml_durs               # all rewritten
    assert set(xml_durs) == {'00:01:23.456', '00:01:30.000'}

    backups = [d for d in os.listdir(library.root)
               if d.startswith('duration_repair_backup_')]
    assert backups, 'a backup directory should have been created'
    assert os.path.exists(os.path.join(library.root, backups[0], 'project-manager.db'))
    assert rc == 1  # still 1 because of the MISSING file


def test_apply_is_idempotent(library, canned_probe):
    rd.main(['--library-path', library.root, '--apply'])
    # second run: nothing left to change
    durs_after_first = library.durations()
    rd.main(['--library-path', library.root, '--apply'])
    assert library.durations() == durs_after_first


def test_skip_trash(library, canned_probe):
    rd.main(['--library-path', library.root, '--apply', '--skip-trash', '--db-only'])
    # trashed tone_c.wav should be untouched by --skip-trash
    assert library.durations()['tone_c.wav'] == '00:00:03.9'


def test_trash_media_fixed_without_skip(library, canned_probe):
    rd.main(['--library-path', library.root, '--apply', '--db-only'])
    assert library.durations()['tone_c.wav'] == '00:00:03.000'


def test_skipped_invalid_xml_does_not_abort(library, canned_probe):
    # add a second project whose script.xml is malformed
    from cuemseditor.CuemsDBModel import Project, database
    from cuemsutils.helpers import new_uuid, new_datetime
    bad_dir = os.path.join(library.projects_dir, 'proj2')
    os.makedirs(bad_dir, exist_ok=True)
    with open(os.path.join(bad_dir, 'script.xml'), 'w') as f:
        f.write('<not-valid-xml>')
    library.connect()
    Project.create(uuid=str(new_uuid()), name='Proj Two', unix_name='proj2',
                   created=new_datetime(), modified=new_datetime(), in_trash=False)
    database.close()

    # full run (Pass A fixes the DB, Pass B rewrites XML)
    rc = rd.main(['--library-path', library.root, '--apply'])
    # proj1 still got fixed despite proj2 being invalid
    assert '00:00:00.000' not in _xml_durations(_script_path(library))
    assert rc == 1  # SKIPPED_INVALID (and the MISSING file) mark the run dirty


def test_missing_db_is_fatal(tmp_path):
    rc = rd.main(['--library-path', str(tmp_path)])
    assert rc == 2
