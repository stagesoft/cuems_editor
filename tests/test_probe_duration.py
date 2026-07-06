"""Unit tests for CuemsDBMedia.probe_duration (the ffprobe duration extractor).

subprocess.run is mocked so these run without ffprobe. One integration test
exercises the real binary when it is present.
"""
import shutil
import subprocess

import pytest

from cuemseditor.CuemsDBMedia import probe_duration
from cuemseditor.CuemsErrors import NotTimeCodeError


def _canned(stdout=b'', returncode=0, stderr=b'', timeout=False):
    def _run(*args, **kwargs):
        if timeout:
            raise subprocess.TimeoutExpired(args[0], kwargs.get('timeout', 30))
        return subprocess.CompletedProcess(args[0], returncode, stdout, stderr)
    return _run


@pytest.mark.parametrize('ffprobe_out, expected_ms, expected_str', [
    ('192.450000\n', 192450, '00:03:12.450'),   # the -405ms regression case
    ('192.400000\n', 192400, '00:03:12.400'),    # trailing-zero case
    ('192.999700\n', 193000, '00:03:13.000'),    # carry case (was +108s)
    ('0.033000\n', 33, '00:00:00.033'),
    ('5.900000\n', 5900, '00:00:05.900'),
    ('0.000000\n', 0, '00:00:00.000'),
])
def test_probe_duration_values(monkeypatch, ffprobe_out, expected_ms, expected_str):
    monkeypatch.setattr(subprocess, 'run', _canned(ffprobe_out.encode()))
    tc = probe_duration('/media/x.wav')
    assert tc.milliseconds_exact == expected_ms
    assert str(tc) == expected_str


@pytest.mark.parametrize('stdout, rc, stderr', [
    ('N/A\n', 0, b''),
    ('', 0, b''),
    ('', 1, b'moov atom not found'),
    ('nan\n', 0, b''),
    ('inf\n', 0, b''),
    ('-inf\n', 0, b''),
    ('garbage\n', 0, b''),
    ('-5\n', 0, b''),
])
def test_probe_duration_failures_raise(monkeypatch, stdout, rc, stderr):
    monkeypatch.setattr(subprocess, 'run', _canned(stdout.encode(), rc, stderr))
    with pytest.raises(NotTimeCodeError):
        probe_duration('/media/x.wav')


def test_probe_duration_timeout_raises(monkeypatch):
    monkeypatch.setattr(subprocess, 'run', _canned(timeout=True))
    with pytest.raises(NotTimeCodeError):
        probe_duration('/media/x.wav')


def test_probe_duration_stderr_included_in_error(monkeypatch):
    monkeypatch.setattr(subprocess, 'run', _canned(b'', 1, b'boom detail'))
    with pytest.raises(NotTimeCodeError) as exc:
        probe_duration('/media/x.wav')
    assert 'boom detail' in str(exc.value)


def test_probe_duration_passes_file_path_to_ffprobe(monkeypatch):
    seen = {}

    def _run(*args, **kwargs):
        seen['argv'] = args[0]
        return subprocess.CompletedProcess(args[0], 0, b'1.000000\n', b'')

    monkeypatch.setattr(subprocess, 'run', _run)
    probe_duration('/media/dir/clip.wav')
    assert seen['argv'][0] == 'ffprobe'
    assert seen['argv'][-1] == '/media/dir/clip.wav'
    assert '-sexagesimal' not in seen['argv']  # the old broken flag is gone


@pytest.mark.skipif(shutil.which('ffprobe') is None or shutil.which('ffmpeg') is None,
                    reason='ffprobe/ffmpeg not installed')
def test_probe_duration_real_file(tmp_path):
    wav = tmp_path / 'tone.wav'
    subprocess.run(
        ['ffmpeg', '-y', '-f', 'lavfi', '-i', 'sine=frequency=440:duration=2.5',
         '-loglevel', 'error', str(wav)], check=True)
    tc = probe_duration(str(wav))
    assert abs(tc.milliseconds_exact - 2500) <= 50
