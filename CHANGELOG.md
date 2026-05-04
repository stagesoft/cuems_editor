# Changelog

## Unreleased

CTimecode hardening migration (closes ClickUp 869cyndtv PR #9). Pins `cuemsutils` to the PR #6 release (`0.1.0rc6`+) to consume the `.milliseconds_rounded` / `.milliseconds_exact` precision-split.

### Changed
- Pinned `cuemsutils` from `0.1.0rc1` to `>=0.1.0rc6` (ships transitively with `0.1.0rc7` from cuemsutils PR #10).
- Migrated the only `.milliseconds` call-site — the `audiowaveform -e` CLI argument in `CuemsDBMedia.generate_thumbnail` — to `.milliseconds_rounded`. Applied to both `CuemsDBMedia.py` copies in the repo (root-level and `src/cuemseditor/`); both had the same site.

### Notes
- The CLI arg semantics are seconds-equivalent (the `/1000` in the expression converts ms to seconds for `audiowaveform`'s `-e` flag). At integer framerates (which is what audio media uses in cuems) `.milliseconds_rounded` and the old `.milliseconds` are identical; at fractional framerates the new behavior rounds where the old truncated, a difference of at most 1 ms in the waveform endpoint. Visually undetectable in the rendered waveform.
- This branch (`fix/ctimecode-migration`) sits on `fix/mtc-bias-compensation`, which was created here from `rc1` (not `master` per the original 869cyndtv plan) since `rc1` is 86 commits ahead of `master` and is the active editor branch.
