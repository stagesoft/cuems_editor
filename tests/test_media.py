"""Tests for CuemsDBMedia intake None-guard and list() duration payload."""
import os
from unittest import mock

from cuemseditor.cli import get_settings
from cuemseditor.CuemsDBMedia import CuemsDBMedia
from cuemseditor.CuemsDBModel import Media, database
from cuemseditor.CuemsErrors import NotTimeCodeError


def _media_mgr(library):
    settings = get_settings()
    settings['library_path'] = library.root
    library.connect()
    return CuemsDBMedia(settings, database)


def test_list_payload_carries_duration(library):
    mgr = _media_mgr(library)
    try:
        entries = mgr.list()
        # each single-key {uuid: {...}} dict must carry a 'duration' key
        for entry in entries:
            (meta,) = entry.values()
            assert 'duration' in meta
        durations = {list(e.values())[0]['unix_name']: list(e.values())[0]['duration']
                     for e in entries}
        assert durations['file.ext'] == '00:00:00.000'
        assert durations['file_video.ext'] is None
    finally:
        database.close()


def test_list_trash_payload_carries_duration(library):
    mgr = _media_mgr(library)
    try:
        entries = mgr.list_trash()
        durations = {list(e.values())[0]['unix_name']: list(e.values())[0]['duration']
                     for e in entries}
        assert durations['tone_c.wav'] == '00:00:03.9'
    finally:
        database.close()


def test_new_none_duration_skips_audio_thumbnail_keeps_waveform(library, tmp_path):
    mgr = _media_mgr(library)
    try:
        upload = tmp_path / 'incoming.wav'
        upload.write_bytes(b'RIFF....')  # bytes irrelevant; probe is mocked

        with mock.patch.object(mgr, 'get_duration',
                               side_effect=NotTimeCodeError('boom')), \
             mock.patch.object(mgr, 'create_audio_thubnail') as thumb, \
             mock.patch.object(mgr, 'create_audio_waveform',
                               return_value='wave.dat') as wave:
            dest = mgr.new(str(upload), 'incoming.wav')

        thumb.assert_not_called()      # no duration -> no waveform *thumbnail*
        wave.assert_called_once()      # waveform data still generated
        row = Media.get(Media.unix_name == dest)
        assert row.duration is None    # row created, duration NULL
        assert row.media_type == 'AUDIO'
    finally:
        database.close()
