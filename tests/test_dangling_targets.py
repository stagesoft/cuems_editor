# SPDX-FileCopyrightText: 2026 Stagelab Coop SCCL
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-FileContributor: Ion Reguera <ion@stagelab.coop>

"""Dangling-target cleanup must know FadeCue.

Regression pins for the CUE_TYPES fix: before it, _collect_cue_ids never
collected FadeCue ids — so an ActionCue targeting a FadeCue was wrongly seen
as dangling and nullified on every save — and FadeCue's own dangling
action_target was never cleaned.
"""

from cuemseditor.CuemsDBProject import CuemsDBProject


def _walker():
    """CuemsDBProject instance without running __init__ (the walk methods only
    touch the CUE_TYPES class attribute, no DB/paths needed)."""
    return object.__new__(CuemsDBProject)


def _clean(contents):
    w = _walker()
    ids = set()
    w._collect_cue_ids(contents, ids)
    w._nullify_dangling_refs(contents, ids)
    return contents


def test_fadecue_ids_are_collected():
    w = _walker()
    ids = set()
    w._collect_cue_ids([{'FadeCue': {'id': 'fade-1'}}], ids)
    assert 'fade-1' in ids


def test_actioncue_targeting_fadecue_is_preserved():
    """Pre-fix behaviour: this action_target was nullified on every save."""
    contents = [
        {'FadeCue': {'id': 'fade-1', 'action_target': 'audio-1'}},
        {'AudioCue': {'id': 'audio-1'}},
        {'ActionCue': {'id': 'act-1', 'action_target': 'fade-1'}},
    ]
    _clean(contents)
    assert contents[2]['ActionCue']['action_target'] == 'fade-1'


def test_fadecue_dangling_action_target_is_cleared():
    contents = [
        {'FadeCue': {'id': 'fade-1', 'action_target': 'deleted-cue'}},
    ]
    _clean(contents)
    assert contents[0]['FadeCue']['action_target'] is None


def test_fadecue_valid_action_target_is_preserved():
    contents = [
        {'AudioCue': {'id': 'audio-1'}},
        {'FadeCue': {'id': 'fade-1', 'action_target': 'audio-1'}},
    ]
    _clean(contents)
    assert contents[1]['FadeCue']['action_target'] == 'audio-1'


def test_nested_cuelist_fadecue_ids_count():
    contents = [
        {'CueList': {'id': 'list-1', 'contents': [
            {'FadeCue': {'id': 'fade-deep'}},
        ]}},
        {'ActionCue': {'id': 'act-1', 'action_target': 'fade-deep'}},
    ]
    _clean(contents)
    assert contents[1]['ActionCue']['action_target'] == 'fade-deep'
