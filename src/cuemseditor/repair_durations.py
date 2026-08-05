"""Repair corrupted media durations in the CueMS library.

Historical ``CuemsDBMedia.get_duration`` reformatted ffprobe sexagesimal output
with a string-slicing hack that dropped zero-padding, so any duration whose
millisecond fraction ended in a zero was stored short (by up to ~0.9 s) and the
whole-second carry case overshot by minutes. Those corrupted values live in the
``media.duration`` column of ``project-manager.db`` and, because the editor
copies the DB value into every saved ``script.xml`` on save, in the project XML
files too.

This tool re-probes every media file with the fixed :func:`probe_duration` and:

- **Pass A** rewrites ``media.duration`` in the DB (the source of truth).
- **Pass B** rewrites ``<duration>`` in each project ``script.xml`` from the
  (now-corrected) DB, reusing the exact walk the editor's save path uses.

Dry-run is the default; ``--apply`` performs writes after backing up the DB and
every modified XML file. Run with the editor STOPPED (single-writer SQLite).

Invoke as a console script (``cuems-editor-repair-durations``) or module
(``python -m cuemseditor.repair_durations``).
"""
import argparse
import os
import re
import shutil
import sys
from datetime import datetime

from cuemseditor.cli import get_settings
from cuemseditor.CuemsDBModel import Project, Media, database
from cuemseditor.CuemsDBMedia import probe_duration
from cuemseditor.CuemsDBProject import (
    fix_media_durations_in_contents,
    db_duration_resolver,
)
from cuemseditor.CuemsErrors import NotTimeCodeError
from cuemsutils.tools.CTimecode import CTimecode
from cuemsutils.xml.Parsers import CuemsParser
from cuemsutils.xml.XmlReaderWriter import XmlReaderWriter

# Canonical ms-framerate timecode shape (what str(CTimecode) always produces).
TIMECODE_SHAPE = re.compile(r'^\d\d:\d\d:\d\d\.\d\d\d$')


def build_settings(args):
    settings = get_settings()
    if args.library_path:
        settings['library_path'] = args.library_path
    if args.database_name:
        settings['database_name'] = args.database_name
    return settings


def _media_file_path(settings, media):
    """Absolute path of a media file, honouring its trash state."""
    library = settings['library_path']
    if media.in_trash:
        return os.path.join(library, settings['trash_folder_name'],
                            settings['media_folder_name'], media.unix_name)
    return os.path.join(library, settings['media_folder_name'], media.unix_name)


def _project_xml_path(settings, project):
    library = settings['library_path']
    if project.in_trash:
        base = os.path.join(library, settings['trash_folder_name'],
                            settings['project_folder_name'])
    else:
        base = os.path.join(library, settings['project_folder_name'])
    return os.path.join(base, project.unix_name, settings['script_file_name'])


def _delta_ms(old_str, new_tc):
    """Best-effort millisecond delta new-old; None if old is unparseable."""
    try:
        old_ms = CTimecode(old_str).milliseconds_exact
    except Exception:
        return None
    return new_tc.milliseconds_exact - old_ms


def _scan_odd_timecodes(node, found):
    """Collect timecode-ish strings that don't match the canonical shape."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ('duration', 'in_time', 'out_time', 'offset',
                       'prewait', 'postwait') and isinstance(value, str):
                if value and not TIMECODE_SHAPE.match(value):
                    found.append(f'{key}={value!r}')
            else:
                _scan_odd_timecodes(value, found)
    elif isinstance(node, list):
        for item in node:
            _scan_odd_timecodes(item, found)


class Report:
    """Accumulates per-item statuses and drives the exit code."""

    def __init__(self):
        self.lines = []
        self.counts = {}
        self.dirty = False  # any MISSING / PROBE_FAILED / SKIPPED_INVALID / orphan

    def add(self, status, detail, dirty=False):
        self.counts[status] = self.counts.get(status, 0) + 1
        self.lines.append(f'  [{status}] {detail}')
        if dirty:
            self.dirty = True

    def dump(self):
        for line in self.lines:
            print(line)
        print('\n  --- summary ---')
        for status in sorted(self.counts):
            print(f'    {status}: {self.counts[status]}')


def backup_database(settings, backup_dir):
    db_path = os.path.join(settings['library_path'], settings['database_name'])
    os.makedirs(backup_dir, exist_ok=True)
    for suffix in ('', '-wal', '-shm'):
        src = db_path + suffix
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(backup_dir, os.path.basename(src)))
    print(f'  DB backed up to {backup_dir}')


def backup_file(settings, path, backup_dir):
    """Mirror a file under backup_dir preserving its library-relative path."""
    rel = os.path.relpath(path, settings['library_path'])
    dest = os.path.join(backup_dir, rel)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(path, dest)


def pass_a_db(settings, args, report):
    """Re-probe every media file, report/apply corrected durations."""
    print('\n== Pass A: database media.duration ==')
    updates = []  # (uuid, new_str)
    for media in Media.select():
        label = f'{media.unix_name} ({media.media_type})'
        if media.media_type == 'IMAGE':
            report.add('SKIP_IMAGE', label)
            continue
        if args.skip_trash and media.in_trash:
            report.add('SKIP_TRASH', label)
            continue
        path = _media_file_path(settings, media)
        if not os.path.exists(path):
            report.add('MISSING', f'{label} -> {path}', dirty=True)
            continue
        try:
            new_tc = probe_duration(path)
        except NotTimeCodeError as e:
            report.add('PROBE_FAILED', f'{label}: {e}', dirty=True)
            continue
        except Exception as e:  # defensive: never abort the whole pass
            report.add('PROBE_FAILED', f'{label}: unexpected {type(e).__name__}: {e}',
                       dirty=True)
            continue
        new_str = str(new_tc)
        old = media.duration
        if old is None:
            report.add('NULL_FILLED', f'{label}: -> {new_str}')
            updates.append((media.uuid, new_str))
        elif old == new_str:
            report.add('OK', label)
        else:
            delta = _delta_ms(old, new_tc)
            delta_txt = f'{delta:+.0f}ms' if delta is not None else 'Δ?(old unparseable)'
            flag = ' <<<>1s' if (delta is not None and abs(delta) > 1000) else ''
            report.add('CHANGED', f'{label}: {old} -> {new_str} ({delta_txt}){flag}')
            updates.append((media.uuid, new_str))

    if args.apply and updates:
        with database.atomic():
            for uuid, new_str in updates:
                Media.update(duration=new_str).where(Media.uuid == uuid).execute()
        print(f'  applied {len(updates)} DB duration update(s)')
    elif updates:
        print(f'  {len(updates)} DB duration update(s) pending (dry-run)')
    return updates


def pass_b_xml(settings, args, report, backup_dir):
    """Rewrite <duration> in each project script.xml from the corrected DB."""
    print('\n== Pass B: project script.xml <duration> ==')
    if args.xml_only:
        print('  WARNING: --xml-only trusts the CURRENT DB values; run Pass A '
              'first or corruption will be propagated verbatim.')
    schema = settings['script_schema_name']
    for project in Project.select():
        if args.skip_trash and project.in_trash:
            report.add('SKIP_TRASH_XML', project.unix_name)
            continue
        path = _project_xml_path(settings, project)
        label = f'{project.unix_name}'
        if not os.path.exists(path):
            report.add('MISSING_XML', f'{label} -> {path}', dirty=True)
            continue
        try:
            data = XmlReaderWriter(schema_name=schema, xmlfile=path).read()
        except Exception as e:
            report.add('SKIPPED_INVALID', f'{label}: {type(e).__name__}: {e}',
                       dirty=True)
            continue

        odd = []
        _scan_odd_timecodes(data, odd)
        if odd:
            report.add('ODD_TIMECODE', f'{label}: {odd}', dirty=True)

        contents = data.get('CuemsScript', {}).get('CueList', {}).get('contents', [])
        stats = fix_media_durations_in_contents(contents, db_duration_resolver)
        if stats.orphans:
            report.add('ORPHAN_MEDIA_REF',
                       f'{label}: {len(stats.orphans)} ref(s) not in DB: {stats.orphans}',
                       dirty=True)

        if stats.replacements == 0:
            report.add('XML_OK', label)
            continue

        report.add('XML_CHANGED', f'{label}: {stats.replacements} duration(s)')
        if args.apply:
            try:
                backup_file(settings, path, backup_dir)
                obj = CuemsParser(data).parse()
                XmlReaderWriter(schema_name=schema, xmlfile=path).write_from_object(obj)
            except Exception as e:
                report.add('WRITE_FAILED', f'{label}: {type(e).__name__}: {e}',
                           dirty=True)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='cuems-editor-repair-durations',
        description='Re-probe and repair corrupted media durations in the DB '
                    'and project XMLs. Dry-run by default; use --apply to write.')
    parser.add_argument('--library-path', help='override library_path (default /opt/cuems_library)')
    parser.add_argument('--database-name', help='override database file name')
    parser.add_argument('--apply', action='store_true', help='perform writes (default: dry-run)')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--db-only', action='store_true', help='run Pass A only')
    group.add_argument('--xml-only', action='store_true', help='run Pass B only')
    parser.add_argument('--skip-trash', action='store_true', help='skip trashed media and projects')
    parser.add_argument('--backup-dir', help='backup directory (default <library>/duration_repair_backup_<ts>)')
    args = parser.parse_args(argv)

    settings = build_settings(args)
    db_path = os.path.join(settings['library_path'], settings['database_name'])
    if not os.path.exists(db_path):
        print(f'FATAL: database not found at {db_path}', file=sys.stderr)
        return 2

    if not args.apply:
        print('*** DRY RUN — no changes will be written (use --apply to commit) ***')
    else:
        if os.path.exists(settings.get('editor_ipc', '')):
            print('*** WARNING: editor IPC socket present — cuems-editor may be '
                  'RUNNING. Stop it before --apply. ***')

    backup_dir = args.backup_dir or os.path.join(
        settings['library_path'],
        'duration_repair_backup_' + datetime.now().strftime('%Y%m%d-%H%M%S'))

    database.init(db_path)
    database.connect()
    if args.apply:
        backup_database(settings, backup_dir)

    report = Report()
    try:
        if not args.xml_only:
            pass_a_db(settings, args, report)
        if not args.db_only:
            pass_b_xml(settings, args, report, backup_dir)
    finally:
        database.close()

    print('\n== Report ==')
    report.dump()
    if args.apply:
        print(f'\n  backups: {backup_dir}')
    else:
        print('\n  (dry-run — re-run with --apply to write)')

    return 1 if report.dirty else 0


if __name__ == '__main__':
    sys.exit(main())
