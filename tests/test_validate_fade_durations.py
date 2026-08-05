# SPDX-FileCopyrightText: 2026 Stagelab Coop SCCL
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-FileContributor: Ion Reguera <ion@stagelab.coop>

"""Save-time FadeCue duration validation (validate_fade_durations_in_contents).

The parser (CuemsParser/GenericParser) bypasses FadeCue.set_duration, so the
editor must reject zero/missing/unparseable durations before parsing.  Parsing
delegates to the real CTimecode so the verdict can never diverge from what the
engine computes on load (literal ms semantics: '.3' is 3 ms; '00:00:03' raises).
"""

import pytest
from cuemsutils.tools.CTimecode import CTimecode

from cuemseditor.CuemsDBProject import (
    _fade_duration_ms,
    validate_fade_durations_in_contents,
)


def _fade(duration, name='Fade out', cue_id='fade-uuid-1'):
    """Build a FadeCue item in the server JSON shape the frontend sends."""
    return {
        'FadeCue': {
            'name': name,
            'id': cue_id,
            'duration': duration,
        }
    }


def _tc(value):
    return {'CTimecode': value}


# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------


def test_zero_duration_rejected_with_name_and_id():
    with pytest.raises(ValueError) as excinfo:
        validate_fade_durations_in_contents([_fade(_tc('00:00:00.000'))])
    msg = str(excinfo.value)
    assert 'FadeCue duration must be greater than zero' in msg
    assert 'Fade out' in msg
    assert 'fade-uuid-1' in msg


def test_none_duration_rejected():
    with pytest.raises(ValueError):
        validate_fade_durations_in_contents([_fade(None)])


def test_missing_duration_key_rejected():
    with pytest.raises(ValueError):
        validate_fade_durations_in_contents(
            [{'FadeCue': {'name': 'Fade', 'id': 'x'}}]
        )


def test_empty_dict_duration_rejected():
    with pytest.raises(ValueError):
        validate_fade_durations_in_contents([_fade({})])


def test_ctimecode_none_rejected():
    with pytest.raises(ValueError):
        validate_fade_durations_in_contents([_fade(_tc(None))])


def test_garbage_string_rejected():
    with pytest.raises(ValueError):
        validate_fade_durations_in_contents([_fade(_tc('garbage'))])


def test_three_component_timecode_rejected_cleanly():
    """'00:00:03' makes CTimecode raise IndexError internally; the validator
    must surface a clean ValueError instead (regression pin: no IndexError)."""
    assert _fade_duration_ms(_tc('00:00:03')) is None
    with pytest.raises(ValueError):
        validate_fade_durations_in_contents([_fade(_tc('00:00:03'))])


# ---------------------------------------------------------------------------
# Acceptances
# ---------------------------------------------------------------------------


def test_300ms_accepted():
    validate_fade_durations_in_contents([_fade(_tc('00:00:00.300'))])


def test_legacy_frames_format_accepted():
    """'0:0:3:0' is the legacy frames shape (3000 ms at 25 fps) — valid."""
    assert _fade_duration_ms(_tc('0:0:3:0')) == 3000
    validate_fade_durations_in_contents([_fade(_tc('0:0:3:0'))])


def test_short_ms_literal_semantics_parity_with_ctimecode():
    """CTimecode ms digits are literal, not scaled: '.3' is 3 ms (not 300).
    The validator must agree exactly with CTimecode for these shapes."""
    for raw in ('00:00:00.3', '00:00:00.30', '00:00:00.300'):
        assert _fade_duration_ms(_tc(raw)) == CTimecode(raw).milliseconds_rounded
    # 3 ms > 0 → accepted, matching what the engine will compute on load.
    validate_fade_durations_in_contents([_fade(_tc('00:00:00.3'))])


# ---------------------------------------------------------------------------
# Traversal
# ---------------------------------------------------------------------------


def test_nested_cuelist_recursion():
    nested = {
        'CueList': {
            'name': 'group',
            'contents': [_fade(_tc('00:00:00.000'), name='Inner fade')],
        }
    }
    with pytest.raises(ValueError) as excinfo:
        validate_fade_durations_in_contents([nested])
    assert 'Inner fade' in str(excinfo.value)


def test_foreign_and_flat_items_tolerated():
    contents = [
        {'AudioCue': {'name': 'a', 'id': 'a1',
                      'Media': {'duration': '00:00:00.000'}}},
        {'name': 'flat item without wrapper'},
        'not even a dict',
        _fade(_tc('00:00:00.300')),
    ]
    validate_fade_durations_in_contents(contents)


def test_multiple_offenders_listed_in_one_message():
    contents = [
        _fade(_tc('00:00:00.000'), name='Fade A', cue_id='id-a'),
        _fade(None, name='Fade B', cue_id='id-b'),
    ]
    with pytest.raises(ValueError) as excinfo:
        validate_fade_durations_in_contents(contents)
    msg = str(excinfo.value)
    assert 'Fade A' in msg and 'Fade B' in msg


def test_empty_and_none_contents_pass():
    validate_fade_durations_in_contents([])
    validate_fade_durations_in_contents(None)
