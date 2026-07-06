# Contributing

Thanks for your interest in contributing! This document explains how
this project is developed and released, and how to get your changes
accepted.

## Development and release model

This project follows a **develop-private, release-public** model:

- **Public releases and contributions live here on GitHub.** This
  repository is the canonical public surface: tagged releases, the
  issue tracker, and pull requests all happen here.
- **Day-to-day development happens on a private upstream.** The full
  engineering history is maintained privately and is published to this
  repository as curated public releases.

A practical consequence: when your pull request is accepted, it is
**replayed onto the private development line** — cherry-picked with your
original commit authorship and your `Signed-off-by` line preserved — and
then ships in a subsequent public release. Because of this replay, your
contribution may land as a cherry-picked commit rather than a GitHub
"merge" of your branch. Your authorship and sign-off are always
preserved through the replay.

This means:

- Base your work on the latest released state of this repository.
- Keep pull requests focused and self-contained so they replay cleanly.
- Expect the final published commit to carry your authorship and
  sign-off, even though the merge mechanics differ from a normal GitHub
  merge.

## How to contribute

1. **Open an issue first** for anything non-trivial — a bug report or an
   enhancement proposal — so the design can be discussed before code is
   written.
2. **Fork the repository and create a topic branch** for your change.
3. **Make your change** with tests where applicable, and make sure the
   existing checks pass locally.
4. **Sign off every commit** (see the DCO section below).
5. **Open a pull request** against the default branch. Describe what the
   change does and link the issue it addresses.

A maintainer will review your pull request here on GitHub. Once it is
accepted, it is replayed upstream (see the model above) and released.

## Contribution funnel

**GitHub Issues are the canonical public entry point.** Bug reports and
enhancement proposals filed here — via the Bug and Enhancement templates —
are triaged by the maintainers and pulled into the private development loop,
where the work is scheduled, implemented, and reviewed. This mirrors the
**develop-private / release-public** model described above: the public
GitHub repository is where contributions arrive and releases ship, while
day-to-day engineering happens on the private upstream.

Concretely:

- **File issues here.** GitHub Issues are canonical for the public; there is
  no separate public tracker to consult.
- **Triage flows inward.** A triaged public issue becomes a tracked work item
  in the private loop; status updates and the eventual fix flow back out as a
  public release.
- **Pull requests replay.** An accepted pull request is replayed onto the
  private development line with your authorship and `Signed-off-by` preserved,
  then ships in a subsequent `release-public` cut.

### Maintainers: replaying an accepted pull request

> This is a **maintainer-only** workflow that runs on the private development
> repository. You do not need it to contribute.

Once a pull request is accepted, a maintainer replays it with a single command:

```
make replay PR=<N>
```

This cherry-picks PR `<N>` onto a `contrib/<N>` branch — preserving the
contributor's authorship and lone `Signed-off-by` — and writes the generated
maintainer artifacts (for example a courtesy-close script) under
`tools/publish/out/`. `make replay` never writes to GitHub or GitLab on its own:
the maintainer reviews and applies those artifacts manually. To rehearse the
command end to end against a throwaway fixture, without touching any real pull
request, run:

```
make replay PR=1 REPLAY_DRYRUN=1
```

## Developer Certificate of Origin (DCO)

This project requires a **Developer Certificate of Origin** sign-off on
every commit. The DCO is a lightweight way for you to certify that you
wrote the patch or otherwise have the right to submit it under the
project's license. It is **not** a copyright assignment.

Add a `Signed-off-by` line to each commit by committing with `-s`:

```
git commit -s -m "your message"
```

This appends a line in the form:

```
Signed-off-by: Your Name <your.email@example.com>
```

Use your real name and an email you can be reached at. Pull requests
whose commits are missing a `Signed-off-by` line cannot be accepted;
sign-offs are verified during maintainer review.

The full text of the Developer Certificate of Origin, version 1.1,
follows.

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

## Code style and checks

- Match the conventions of the surrounding code.
- Run the project's build, vet, format, lint, and test steps locally
  before opening a pull request; the same checks run in CI.
- Keep generated files in sync with their generators rather than
  hand-editing the generated output.

The public pull-request gate (`.github/workflows/pr-gate.yml`) runs the
same code-quality checks as the private gate — lint, unit/envtest,
CRD-additive apidiff, govulncheck, chart lint/unittest, secret scans, DCO,
and the **changelog-tag** check (a change to the CRD `*types*.go` must add a
`docs/CHANGELOG.md` entry). Two GitLab-process gates are deliberately **not**
ported, because they enforce GitLab issue/MR conventions that don't apply to
the public GitHub funnel: **require-closes** (an MR must reference a closing
issue) and **ci-script-tests** (a guard test for the private release-image
pipeline). This omission is a recorded decision, not an oversight.

## Reporting security issues

Please do **not** report security vulnerabilities through public issues
or pull requests. See [`docs/SECURITY.md`](docs/SECURITY.md) for the
private disclosure process.

## Code of conduct

This project adopts a [Code of Conduct](CODE_OF_CONDUCT.md). By
participating, you are expected to uphold it.

## License

By contributing, you agree that your contributions will be licensed
under the same license as this project (see [`LICENSE`](LICENSE)), and
you certify the DCO sign-off described above.
