<!--
SPDX-FileCopyrightText: 2026 Stagelab Coop SCCL
SPDX-License-Identifier: GPL-3.0-or-later
-->

# Contributing to cuems-editor

Thank you for your interest in cuems-editor. This service is the backend that every
CueMS operator interacts with at show time — a regression here can corrupt project
files, break media uploads, or sever the editor-engine IPC link mid-show. These
guidelines exist to protect that reliability while keeping the project genuinely open
to external contributions.

The authoritative governance document for the rules summarised here is
[`specs/planning/contributor-workflow.md`](specs/planning/contributor-workflow.md).
If this file and that document conflict, the planning document wins.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Development Setup](#2-development-setup)
3. [Contribution Tiers](#3-contribution-tiers)
4. [Branch Naming](#4-branch-naming)
5. [Spec-First Requirement](#5-spec-first-requirement)
6. [TDD Workflow — Non-Negotiable](#6-tdd-workflow--non-negotiable)
7. [Commit Hygiene](#7-commit-hygiene)
8. [Developer Certificate of Origin (DCO)](#8-developer-certificate-of-origin-dco)
9. [Pull Request Requirements](#9-pull-request-requirements)
10. [Acceptance Criteria](#10-acceptance-criteria)
11. [Review Process](#11-review-process)
12. [Changelog Line](#12-changelog-line)
13. [Dependency Governance](#13-dependency-governance)
14. [License](#14-license)

---

## 1. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.11+ | managed via [pyenv](https://github.com/pyenv/pyenv) |
| hatch | ≥ 1.9 | environment and build management |
| Git | any recent | DCO sign-off required (see §8) |
| ffmpeg | system package | required for thumbnail generation in tests |
| audiowaveform | system package | required for waveform generation in tests |

No other system-level packages are required for basic development. Tests that invoke
`cuems-videoindexer` or NNG IPC to `cuems-engine` are integration tests and can be
skipped locally with `-m "not integration"`.

---

## 2. Development Setup

```bash
# Clone and enter the repo
git clone https://github.com/stagesoft/cuems-editor.git
cd cuems-editor

# Install the correct Python version
pyenv install 3.11
pyenv local 3.11

# Editable install with dev dependencies
pip install -e ".[dev]"

# Verify the test suite is green before making any changes
cd src && pytest
```

Run lint checks:

```bash
ruff check .
```

---

## 3. Contribution Tiers

The review requirements depend on what you change.

### Tier 1 — Trivial

No change to any file under `src/cuemseditor/` beyond a single-line correction.
Covers: README edits, doc fixes, comment corrections, adding a test for
already-shipped behaviour, CI/CD config changes.

**Gates**: lint + CI green; one owner approval. No spec required.

### Tier 2 — Non-trivial

Any addition, modification, or deletion of logic in `src/cuemseditor/`. Includes
bug fixes that change branching behaviour, new WebSocket commands, new media type
support, schema changes, and refactors.

**Gates**: spec + plan on the branch; failing test before implementation; CI green;
constitution compliance declaration; one mandatory owner approval.

---

## 4. Branch Naming

```
feat/NNN-short-description       ← new feature  (NNN = spec number, e.g. 004)
fix/NNN-short-description        ← bug fix referencing a spec or issue number
chore/short-description          ← non-production changes (CI, tooling, docs)
```

The `NNN` prefix links the branch to `specs/NNN-feature/` artifacts. Branches
without a valid prefix will not be merged.

---

## 5. Spec-First Requirement

For **Tier 2** changes, before opening a PR for review you MUST commit these two
files on your feature branch:

```
specs/NNN-feature/spec.md    ← feature specification
specs/NNN-feature/plan.md    ← implementation plan with Constitution Check completed
```

If you are a first-time contributor unfamiliar with the spec format, open a
[Discussion](https://github.com/stagesoft/cuems-editor/discussions) first and the
maintainers will help you scope the work.

The PR description must link to both artifacts. A PR without them will be marked
as a draft and returned for pre-work.

---

## 6. TDD Workflow — Non-Negotiable

cuems-editor enforces Test-Driven Development for all Tier 2 changes. This is not
a style preference — it is a constitutional requirement.

The mandatory sequence:

```
1. Write a failing test that precisely describes the intended behaviour.
2. Confirm CI fails on that commit (or run pytest locally and record the failure).
3. Write the minimum production code required to make the test pass.
4. Refactor without changing observable behaviour, keeping all tests green.
```

Your git log on the feature branch MUST show this order. The PR template asks for
the commit SHA of your failing-test commit. Reviewers will check it.

```bash
# Run the full suite (from repo root)
cd src && pytest

# Run with coverage report
cd src && pytest --cov=cuemseditor

# Skip integration tests for a fast feedback loop
pytest -m "not integration"
```

---

## 7. Commit Hygiene

cuems-editor uses [Conventional Commits](https://www.conventionalcommits.org/) v1.0.

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
Signed-off-by: Your Name <your@email.com>
```

Allowed types: `feat`, `fix`, `test`, `refactor`, `docs`, `chore`, `ci`, `perf`, `patch`.

Breaking changes: append `!` after the type and include a `BREAKING CHANGE:` footer.

Rules:
- Each commit MUST represent one logical change.
- Do not squash unrelated changes into a single commit.
- Force-pushing to `main` is forbidden. Amending published commits on shared
  branches is forbidden.

---

## 8. Developer Certificate of Origin (DCO)

Every commit must carry a `Signed-off-by` line, asserting that you have the right to
submit the contribution under GPL-3.0, as per the
[Developer Certificate of Origin](https://developercertificate.org).

```bash
git commit -s -m "feat: add support for ..."
```

To add sign-off to all commits in a branch automatically, set:

```bash
git config --local format.signOff true
```

PRs that contain unsigned commits will not be merged.

---

## 9. Pull Request Requirements

Open your PR against `main` (or the current release candidate branch as directed
by maintainers). Use the PR template — it contains the full acceptance checklist.

Every PR MUST include in its description:

1. **Summary** — what changed and why (2–5 sentences).
2. **Changelog Line** — see §12.
3. **Spec links** (Tier 2 only) — links to `spec.md` and `plan.md` on the branch.
4. **Failing-test commit SHA** (Tier 2 only) — the commit where CI was red before
   implementation began.
5. **Completed PR checklist** — all items in the template ticked.

Draft PRs are welcome for early feedback on approach. Drafts will not be formally
reviewed. Convert to Ready when all gates pass.

---

## 10. Acceptance Criteria

A PR is ready to merge when ALL of the following are true:

| Criterion | How verified |
|---|---|
| `spec.md` and `plan.md` committed on branch (Tier 2) | Reviewer reads the files |
| Failing test committed before implementation (Tier 2) | SHA provided; reviewer checks git log |
| All tests pass | CI green |
| Coverage ≥ 80% | CI coverage gate |
| `ruff check` passes | CI lint gate |
| No new runtime dependency without justification | Reviewer checks `pyproject.toml` diff |
| SPDX header on all new source files | Reviewer inspects new files |
| DCO sign-off on all commits | GitHub DCO check |
| At least one owner approval | GitHub branch protection |

---

## 11. Review Process

All PRs to `main` require approval from at least one repository owner:

- **Ion Reguera** ([@ibiltari](https://github.com/ibiltari))
- **Adrià Masip** ([@backenv](https://github.com/backenv))

This is enforced by `.github/CODEOWNERS` and GitHub branch protection.

**What owners check:**
- Spec and plan are coherent with the implementation (Tier 2).
- TDD sequence is evidenced in the git log.
- All CI gates pass.
- Constitution checklist is ticked accurately, not perfunctorily.
- No new runtime dependency slipped in without justification.
- SPDX header present on all new source files.
- SOLID principles respected — single responsibility, no god classes, dependencies
  injected not constructed.
- WebSocket command handlers do not perform blocking I/O on the event loop.

Expect review turnaround within 5 business days. For urgent fixes, open an issue
first and tag a maintainer — that speeds triage.

---

## 12. Changelog Line

You do not edit `CHANGELOG.md` — that is the maintainers' responsibility at release
time. Instead, include a **Changelog Line** in your PR description. Maintainers copy
this line verbatim (or lightly edited) when cutting a release.

Format:

```
[TYPE] Past-tense sentence describing what changed and why it matters to users.
```

Types: `Added`, `Changed`, `Fixed`, `Removed`, `Security`, `Performance`.

Examples:

```
[Added] project_status WebSocket command returns current engine load state without mutating session.
[Fixed] CuemsDBProject.restore() now syncs unix_name in DB after CopyMoveVersioned renames the directory on collision.
[Changed] CuemsDBMedia.generate_thumbnail uses .milliseconds_rounded for the audiowaveform -e argument.
```

---

## 13. Dependency Governance

No new entry under `[project.dependencies]` in `pyproject.toml` may be introduced
without:

1. A written justification in the PR description explaining why the standard library
   and existing dependencies cannot solve the problem.
2. Explicit acknowledgement from a repository owner in the review.

`cuemsutils` is the preferred home for any new shared primitive (timecode, XML, NNG
helpers). Before adding a new dependency, check whether it can live in `cuemsutils`
instead and be consumed by the editor as a library import.

Dev-only dependencies (`[project.optional-dependencies] dev`) are lower friction but
still require a one-line justification in the PR description.

---

## 14. License

cuems-editor is licensed under the GNU General Public License v3.0 (GPL-3.0).
By contributing, you agree that your contributions will be licensed under GPL-3.0.

All new source files MUST carry the following SPDX header:

```python
# SPDX-FileCopyrightText: <year> <Your Name or Organisation>
# SPDX-License-Identifier: GPL-3.0-or-later
```
