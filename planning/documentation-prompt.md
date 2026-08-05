# Documentation Generation Prompt — CueMS / StageLab Repositories

**Use this document as a self-contained prompt when generating or updating documentation
for any repository in the CueMS / StageLab family.**

Read it in full before touching any file. It unifies three sequential tasks:
(1) README + CHANGELOG + docs site, (2) CONTRIBUTORS, (3) CI test + coverage workflow.
All three must be completed in one pass.

---

## 0. Prerequisite reads

Before writing anything, read these files in the **target repo** and the **reference repos**:

```
Target repo (the one you are working in):
  README.md                          — current state
  CHANGELOG.md                       — current state
  pyproject.toml  OR  CMakeLists.txt — package/build metadata, authors, version
  docs/index.md                      — current state (if it exists)
  docs/*.md                          — all existing submodule pages
  .github/workflows/*.yml            — every existing workflow
  mkdocs.yml                         — if it exists
  src/  OR  include/  OR  lib/       — module/class structure (scan, don't read in full)
  git log --oneline -40              — recent commit history
  git log --pretty=format:"=== %H %s ===%n%b" -20  — commit messages with bodies

Reference repos (read for structure and tone — paths are relative to the target repo):
  ../cuems-utils/README.md                   — canonical Python library README
  ../cuems-utils/CHANGELOG.md                — canonical CHANGELOG format
  ../cuems-utils/CONTRIBUTORS.md             — canonical CONTRIBUTORS format
  ../cuems-utils/docs/index.md               — canonical MkDocs index format
  ../cuems-utils/.github/workflows/tests.yml — canonical CI + coverage workflow
  ../cuems-engine/README.md                  — canonical Python service README
  ../cuems-engine/docs/index.md              — canonical service docs index
  ../gradient-motion-engine/README.md        — canonical C++ daemon README
```

Identify the repo's **ecosystem** from the build metadata:

| Indicator | Ecosystem |
|---|---|
| `pyproject.toml` with `hatch` build backend | Python library (`cuems-utils` pattern) |
| `pyproject.toml` with `poetry` build backend | Python service (`cuems-engine` pattern) |
| `CMakeLists.txt` | C++ daemon (`gradient-motion-engine` pattern) |

Ecosystem determines which badges, install instructions, test commands, and CI
steps apply. Adaptations are described in [§8 Ecosystem adaptations](#8-ecosystem-adaptations).

---

## 1. Deliverables checklist

Complete every item. Do not skip any.

- [ ] `README.md` — full overhaul (§2)
- [ ] `CHANGELOG.md` — add entry for current unreleased changes (§3)
- [ ] `docs/index.md` — architecture + design decisions (§4)
- [ ] `docs/*.md` submodule pages — ensure every public class is referenced (§5)
- [ ] `CONTRIBUTORS.md` — contributing guidelines (§6)
- [ ] `.github/workflows/tests.yml` — CI test + coverage upload (§7)
- [ ] Badge additions — Tests + Coverage badges in both `README.md` and `docs/index.md`
- [ ] `pyproject.toml` or build config — update `Documentation` URL if wrong (§8)
- [ ] `mkdocs.yml` — fix `repo_url` if it points to a wrong org (§8)

---

## 2. README.md

### 2.1 Required structure

Follow the exact section order below. Use the canonical examples as reference for
tone and depth.

```
<!--
***
SPDX-FileCopyrightText: <year> Stagelab Coop SCCL
SPDX-License-Identifier: GPL-3.0-or-later
***
-->

# <repo-name>

**Current release: vX.Y.ZrcN** — see [CHANGELOG.md](./CHANGELOG.md).

**<one-line description>**

<badges — see §2.2>

* **Source / issues:** [stagesoft/<repo>](https://github.com/stagesoft/<repo>) on GitHub
* **API reference (HTML):** [stagesoft.github.io/<repo>](https://stagesoft.github.io/<repo>/)

<2–4 sentence description of what the repo is and what it provides>

It is composed of:
  * **`<submodule>`** — <role>
  * ...

---

## Overview
  <architecture diagram (text/ASCII) or pipeline description>
  <bullet list of what each layer does>

---

## Architecture
  <per-submodule section: name, responsibilities, key classes>

---

## Core Concepts
  <5–8 key terms that a new reader must understand, one bullet each>

---

## Design Goals
  <5–8 principles the code enforces, one bullet each>

---

## Installation
  <§2.3 Installation section>

---

## Development
  <§2.4 Development section>

---

## Release notes
  See [CHANGELOG.md](./CHANGELOG.md) for the full history.
  <condensed summary of the last 3–4 releases, one paragraph each>

---

## Copyright notice
  <standard GPL-3.0 notice — see canonical example>

---

## License
  <GPL-3.0 summary section — see canonical example>
```

### 2.2 Required badges

**Python repos (hatch or poetry):**

```markdown
[![PyPI - Version](https://img.shields.io/pypi/v/<pkgname>.svg)](https://pypi.org/project/<pkgname>)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/<pkgname>.svg)](https://pypi.org/project/<pkgname>)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Tests](https://github.com/stagesoft/<repo>/actions/workflows/tests.yml/badge.svg)](https://github.com/stagesoft/<repo>/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/gh/stagesoft/<repo>/graph/badge.svg)](https://codecov.io/gh/stagesoft/<repo>)
[![Deploy MkDocs site](https://github.com/stagesoft/<repo>/actions/workflows/gh-pages.yml/badge.svg)](https://github.com/stagesoft/<repo>/actions/workflows/gh-pages.yml)
[![Upload Python Package](https://github.com/stagesoft/<repo>/actions/workflows/pypi-publish.yml/badge.svg)](https://github.com/stagesoft/<repo>/actions/workflows/pypi-publish.yml)
```

**C++ repos (no PyPI, no Python version):**

```markdown
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Tests](https://github.com/stagesoft/<repo>/actions/workflows/tests.yml/badge.svg)](https://github.com/stagesoft/<repo>/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/gh/stagesoft/<repo>/graph/badge.svg)](https://codecov.io/gh/stagesoft/<repo>)
```

Add a workflow CI badge only if the corresponding workflow file exists. Create
`tests.yml` (§7) before adding the Tests and Coverage badges.

### 2.3 Installation section

**Python library (hatch):**
```markdown
### PyPI
pip install <pkgname>
Optional extras: pip install "<pkgname>[systemd]" / "[all]"

### Debian package
git clone --branch debian/bookworm https://github.com/stagesoft/<repo>.git
dpkg-buildpackage -us -uc
sudo dpkg -i ../python3-<pkgname>_*.deb
```

**Python service (poetry):**
Two packages if applicable (core + mock/dev binaries). List system-package
dependencies installed automatically by the .deb.

**C++ daemon:**
```markdown
### Build from source
git clone https://github.com/stagesoft/<repo>.git
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
sudo make install

### Debian package
git clone --branch debian/bookworm https://github.com/stagesoft/<repo>.git
dpkg-buildpackage -us -uc
sudo dpkg -i ../<pkg>_*.deb

### systemd service
systemctl enable <daemon-name>
systemctl start <daemon-name>
```

### 2.4 Development section

**Python (hatch):**
```markdown
pip install -e ".[all]"
cd src && pytest
pytest --cov=<pkgname>
pytest -W error::DeprecationWarning    # if applicable
hatch test                             # full 3.11/3.12/3.13 matrix
ruff check .
```

**Python (poetry):**
```markdown
poetry install
poetry run pytest
poetry run pytest --cov=src
poetry run black --check src/ tests/
poetry run isort --check src/ tests/
```

**C++:**
```markdown
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON
make -j$(nproc)
ctest --output-on-failure
```

### 2.5 Architecture section depth

For each top-level module or subdirectory in `src/`, write:
- A `###` heading with the module path
- One bullet per exported class/function: `**ClassName**` — one-sentence role
- Cross-reference any class that is consumed by a sibling repo

Derive class descriptions from: docstrings, class names, and recent commit messages.
Do not invent behaviour — only document what the code demonstrably does.

### 2.6 Release notes section

Condense the last 3–4 CHANGELOG entries into one paragraph each (3–5 sentences max).
Keep the full history in CHANGELOG.md; the README section is a summary for new visitors.

---

## 3. CHANGELOG.md

### 3.1 Format

```markdown
# Changelog

## <version> — <YYYY-MM-DD>

<one-sentence summary of the release theme>.

### Added
- <feature> — <what it does and why it matters>.

### Changed (breaking)
- <class>.<method>: <old behaviour> → <new behaviour>. Migration: <steps>.

### Fixed
- <class>.<method>: <symptom>. Root cause: <explanation>.

### Removed
- <item> — deprecated since <version>. Zero callers confirmed; superseded by <replacement>.

### Notes
- <architectural note, layer split, or deferred item>.
```

### 3.2 Finding unreleased changes

Run:
```bash
git log --pretty=format:"=== %H %s ===%n%b" -20
```

Look for commits since the last tagged/versioned CHANGELOG entry. Group them by
`feat`, `fix`, `chore`, `patch`, `refactor` prefix into the appropriate sections.
Use commit message bodies — they contain the precise semantic detail needed.

Today's date is the release date. Read the version string from `__init__.py`,
`pyproject.toml`, or `CMakeLists.txt` (whichever applies).

---

## 4. docs/index.md

### 4.1 Required structure

Follow `../cuems-engine/docs/index.md` for a Python service and
`../cuems-utils/docs/index.md` for a Python library as the primary templates.

```
# <repo-name>

<tagline>

<same badges as README.md>

!!! note "Project README"
    For installation instructions, release history, and licensing, see the
    [project README](https://github.com/stagesoft/<repo>#readme) on GitHub.

---

## What is <repo-name>?
  <2–4 paragraph description, more detailed than README overview>
  <table: component → role, if multiple processes or layers>

---

## Signal flow  (or Data flow / Pipeline)
  <ASCII diagram showing how data moves through the system>
  <numbered sequence for the most important user-visible operation>

---

## Architecture
  <one section per submodule, each linking to its own docs page>
  <per-class bullets — same depth as README §2.5 but with mkdocs page links>

---

## Key design decisions
  <3–6 subsections, one per non-obvious design choice>
  <each explains: what the choice is, why it was made, what invariant it enforces>

---

## API reference
  <table or bullet list linking to each docs/*.md page>
```

### 4.2 Signal/data flow diagram

Capture the primary runtime path as an ASCII diagram. For multi-process systems,
show inter-process transport (NNG, OSC, D-Bus, …). For libraries, show how data
enters and exits the public API.

Example pattern (adapt to the actual repo):
```
Input ──► Component A ──► Component B ──► Output
              │                │
           (protocol)      (storage)
```

### 4.3 Key design decisions depth

Each decision section must contain:
- What the decision is (1 sentence)
- The concrete invariant it enforces (1 sentence, testable)
- Why the alternative was rejected or deferred (1 sentence)

Do not write generic "we value X" statements. Write specific, verifiable claims
about this codebase.

---

## 5. docs/*.md submodule pages

### 5.1 Python repos — mkdocstrings

Each existing `docs/<module>.md` page must contain a `:::` directive for every
public class in the corresponding source module. Audit by listing all `.py` files
in the module directory and comparing against the directives already present.

Template:
```markdown
# <Module Name>

::: <package>.<module>.<ClassName>
::: <package>.<module>.<AnotherClass>
```

Add missing directives; do not remove existing ones. Alphabetical order within
a module page is preferred.

### 5.2 C++ repos — architecture docs

C++ repos without MkDocs/Doxygen should have at minimum:
- `docs/index.md` (§4 above)
- `docs/architecture.md` — component diagram and dependency graph
- `docs/api.md` — public header synopsis (manually written, not generated)

If the repo has no `mkdocs.yml`, create one following `../cuems-utils/mkdocs.yml`
as a template. Omit the `mkdocstrings` plugin for C++ (no Python source to parse).

---

## 6. CONTRIBUTORS.md

### 6.1 Base structure

Use `../cuems-utils/CONTRIBUTORS.md` as the canonical template. It must contain
these 14 sections in order:

1. Prerequisites
2. Development Setup
3. Contribution Tiers (Tier 1 trivial / Tier 2 non-trivial)
4. Branch Naming
5. Spec-First Requirement
6. TDD Workflow — Non-Negotiable
7. Commit Hygiene (Conventional Commits v1.0)
8. Developer Certificate of Origin (DCO)
9. Pull Request Requirements
10. Acceptance Criteria
11. Review Process
12. Changelog Line
13. Dependency Governance
14. License

### 6.2 Ecosystem adaptations

**Python (hatch)** — copy `../cuems-utils/CONTRIBUTORS.md` verbatim, then change:
- Repo name/URL throughout
- Package name in the examples
- `hatch run test:run-cov` stays as the test command
- `ruff check` stays as the lint command
- Any pinned dependency footnotes specific to this repo (e.g. lxml CVE note)
- Acceptance criteria table: replace hatch-specific rows if the test runner differs

**Python (poetry)** — use `../cuems-utils/CONTRIBUTORS.md` as a base, then change:
- Test commands to `poetry run pytest` / `poetry run pytest --cov=src`
- Lint commands to `poetry run black`, `poetry run isort`, `poetry run flake8`
- Acceptance criteria: `black --check`, `isort --check`, `flake8` gates instead of `ruff`
- Prerequisites table: `poetry ≥ 1.7` instead of `hatch`

**C++** — use `../cuems-utils/CONTRIBUTORS.md` as a base, then change:
- Prerequisites: `cmake ≥ 3.20`, `gcc ≥ 12`, no Python tooling
- Development Setup: `cmake .. && make && ctest`
- Lint: `clang-format --dry-run` and/or `clang-tidy`
- Acceptance criteria: replace Python-specific gates with C++ equivalents (AddressSanitizer, valgrind if applicable)
- Remove the "Dependency Governance / pyproject.toml" section; replace with CMake dependency guidance

### 6.3 Mandatory invariants (all ecosystems)

These must not change between repos:
- Spec-first requirement for Tier 2 changes
- TDD sequence (failing test → implementation → refactor)
- DCO sign-off on every commit
- PR target is `main` (not `master`)
- Review by Ion Reguera ([@ibiltari](https://github.com/ibiltari)) or Adrià Masip ([@backenv](https://github.com/backenv))
- SPDX header on all new source files
- SPDX header in CONTRIBUTORS.md itself

---

## 7. .github/workflows/tests.yml

### 7.1 Python repos (hatch)

Create `.github/workflows/tests.yml` following `../cuems-utils/.github/workflows/tests.yml`
verbatim, replacing the package name and org references. The canonical file is:

```yaml
name: Tests

on:
  push:
    branches:
      - main
  pull_request:

permissions:
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install hatch and coverage
        run: pip install hatch coverage[toml]

      - name: Run tests with coverage
        run: hatch run test:run-cov

      - name: Generate coverage XML
        run: coverage xml

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v4
        with:
          files: coverage.xml
          fail_ci_if_error: false
        env:
          CODECOV_TOKEN: ${{ secrets.CODECOV_TOKEN }}
```

**Python repos (poetry):** replace the hatch steps with:
```yaml
      - name: Install dependencies
        run: poetry install

      - name: Run tests with coverage
        run: poetry run pytest --cov=src --cov-report=xml

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v4
        with:
          files: coverage.xml
          fail_ci_if_error: false
        env:
          CODECOV_TOKEN: ${{ secrets.CODECOV_TOKEN }}
```

### 7.2 C++ repos

```yaml
name: Tests

on:
  push:
    branches:
      - main
  pull_request:

permissions:
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Install build dependencies
        run: sudo apt-get install -y cmake gcc g++ lcov

      - name: Configure with coverage
        run: |
          cmake -B build \
            -DCMAKE_BUILD_TYPE=Debug \
            -DCMAKE_CXX_FLAGS="--coverage" \
            -DCMAKE_EXE_LINKER_FLAGS="--coverage"

      - name: Build
        run: cmake --build build -j$(nproc)

      - name: Run tests
        run: ctest --test-dir build --output-on-failure

      - name: Generate coverage report
        run: |
          lcov --capture --directory build \
               --output-file coverage.info \
               --ignore-errors inconsistent
          lcov --remove coverage.info '/usr/*' '*/tests/*' \
               --output-file coverage.info

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v4
        with:
          files: coverage.info
          fail_ci_if_error: false
        env:
          CODECOV_TOKEN: ${{ secrets.CODECOV_TOKEN }}
```

Adapt the `cmake` flags and `lcov` invocation to whatever build system the repo
actually uses. If it uses `meson`, `bazel`, or another tool, replace accordingly.

### 7.3 Existing workflow conflicts

If a `gh-pages.yml` already exists and uses `mkdocs gh-deploy --force`, do NOT
add a second workflow that also commits to the gh-pages branch — the `--force`
flag will wipe anything the second workflow committed. Keep CI (`tests.yml`) and
docs (`gh-pages.yml`) as separate concerns.

If the existing `gh-pages.yml` has a wrong `repo_url` in `mkdocs.yml` or deploys
to the wrong org, fix `mkdocs.yml` at the same time as adding `tests.yml`.

### 7.4 Badge wiring

After creating `tests.yml`, add both new badges to `README.md` and `docs/index.md`
immediately below the existing badges. Do not reorganise the existing badge order —
append the two new lines after the last current badge.

```markdown
[![Tests](https://github.com/stagesoft/<repo>/actions/workflows/tests.yml/badge.svg)](https://github.com/stagesoft/<repo>/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/gh/stagesoft/<repo>/graph/badge.svg)](https://codecov.io/gh/stagesoft/<repo>)
```

**One-time manual step:** the Codecov badge is dark until the repository is
activated at [codecov.io](https://codecov.io). Go to
`https://codecov.io/gh/stagesoft/<repo>`, sign in with GitHub, and click
"Activate". The first successful `tests.yml` run will populate the badge.

---

## 8. Ecosystem adaptations — common fixes

Regardless of ecosystem, always check and fix these if wrong:

### pyproject.toml (Python repos)

```toml
[project.urls]
Documentation = "https://stagesoft.github.io/<repo>/"
```

### mkdocs.yml (Python repos with MkDocs)

```yaml
site_name: <Human-readable repo name>
repo_url: https://github.com/stagesoft/<repo>    # not cuems/<repo> or any other org
```

The `admonition` markdown extension should be enabled if `docs/index.md` uses
`!!! note` blocks:

```yaml
markdown_extensions:
  - admonition
```

### SPDX headers (all repos, all new files)

Every new file created by this documentation pass must start with:

```
# SPDX-FileCopyrightText: <year> Stagelab Coop SCCL
# SPDX-License-Identifier: GPL-3.0-or-later
```

Use `<!--` / `-->` wrapping for Markdown files (see README.md canonical example).

---

## 9. Quality checks before finishing

Run these mentally against every output file:

1. **All relative links resolve** — every `[text](../path)` or `[text](./path)` points
   to a file that actually exists in this repo or a sibling repo.

2. **No placeholder text left** — search for `<repo>`, `<pkgname>`, `<year>`,
   `REPLACE-WITH-`, `TODO`, or any angle-bracket placeholder. Replace all.

3. **Badges match actual workflow file names** — the badge URL contains the exact
   filename of the workflow (e.g., `tests.yml` not `test.yml` or `ci.yml`).

4. **CHANGELOG version matches `__init__.py` / `pyproject.toml` / `CMakeLists.txt`**
   — they must agree on the current version string.

5. **docs/*.md class references are importable** — for Python repos, every
   `:::` directive must match a real importable path. Check against the actual
   source file structure, not memory.

6. **CONTRIBUTORS.md review links are live** — the GitHub handles
   `@ibiltari` and `@backenv` are unchanged; the repo-specific Discussion and
   Issues links use the correct `stagesoft/<repo>` org/name.

7. **No duplication in architecture diagram** — the ASCII box in README.md
   `Overview` must not list a class twice (see e.g. `CTimecodeTimer` appearing in
   both `tools/` and `CuemsScript` rows — a known regression to avoid).
