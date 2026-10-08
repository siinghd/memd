# Releasing memd

When you push a `v*` tag, two workflows publish the release. A third
workflow deploys the docs when you push to the default branch:

| workflow | publishes | credential |
|---|---|---|
| `.github/workflows/release-pypi.yml` | sdist + wheel to PyPI, and a GitHub Release with them | PyPI trusted publishing (OIDC, OpenID Connect) |
| `.github/workflows/release-npm.yml` | `memd-engine` (sdk-ts) to npm, with provenance | trusted publishing (OIDC), environment `npm` |
| `.github/workflows/docs.yml` | the docs site to GitHub Pages | the workflow's own token |

## Once, before the first release

### 0. Decisions only the owner can make

- **The PyPI name** is `memd-engine` (an unrelated project has `memd` on
  PyPI). The import name (`import memd`) and the `memd` command do not
  change. PyPI has a pending trusted publisher for `memd-engine` <-
  `siinghd/memd`, `release-pypi.yml`, environment `pypi`.
- **The npm name** is `memd-engine`, without a scope. It is the same name
  as on PyPI.
- **The GitHub owner and repository** are `github.com/siinghd/memd`. They
  are set in `pyproject.toml` (`[project.urls]`), `sdk-ts/package.json`
  (`repository`, `homepage`) and `mkdocs.yml` (`repo_url`, `repo_name`,
  `site_url`). npm provenance fails if `repository.url` in
  `sdk-ts/package.json` is not the repository that the workflow runs in. If
  the repository moves, update all three files.
- **The docs URL.** GitHub Pages serves `https://<owner>.github.io/<repo>/`.
  A custom domain needs a `docs/CNAME` file and a DNS (Domain Name System)
  record. Set `site_url` in `mkdocs.yml`. Add a `Documentation` entry to
  `[project.urls]`.

### 1. Create the repository and push

Create an empty GitHub repository (no README, license or .gitignore). Then
run these commands:

```bash
git remote add origin git@github.com:<owner>/<repo>.git
git push -u origin master
git push origin --tags
```

If you push the six existing tags in one push, no workflow starts. GitHub
creates no push events when you push more than three tags at once. That is
the result that you want. The tagged commits up to `v0.3.2` are older than
these workflows (they contain the old `release.yml`). Thus, the first release
that `release-pypi.yml` publishes is the first tag that you make after this
branch is merged.

### 2. PyPI: a trusted publisher

On pypi.org, open *Your account → Publishing → Add a new pending
publisher*. The project does not exist yet; the first upload creates it.
Enter these values:

| field | value |
|---|---|
| PyPI project name | the name from step 0 |
| Owner / Repository | `<owner>` / `<repo>` |
| Workflow name | `release-pypi.yml` |
| Environment name | `pypi` |

On GitHub, open *Settings → Environments → New environment* and make the
environment `pypi`. To make each publish wait for an approval, add required
reviewers.

### 3. npm: trusted publishing

npm publishes `memd-engine` with trusted publishing (OIDC). No npm token
is stored in the repository.

1. On npmjs.com, open the `memd-engine` package, then *Settings → Trusted
   publishing*.
2. Add a GitHub Actions publisher: user `siinghd`, repository `memd`,
   workflow `release-npm.yml`, environment `npm`.

npm cannot publish the first version of a package with OIDC. The first
version (0.2.0) was published with a token, and that token was then
revoked.

### 4. GitHub Pages

1. Set *Settings → Pages → Build and deployment → Source: GitHub Actions*.
2. Run the *Docs* workflow one time (*Actions → Docs → Run workflow*).

After this, the workflow deploys the docs on every push to the default
branch that changes the docs.

## Every release

1. **Versions.** Set `[project].version` in `pyproject.toml` and
   `__version__` in `src/memd/__init__.py` to the new version. The workflow refuses a tag if
   one of the two is not the version of the tag. The TypeScript SDK
   (software development kit) has its own version in `sdk-ts/package.json`.
   If the SDK changed, increase that version. A tag publishes the SDK only
   if npm does not have that version yet.
2. **Changelog.** Move `## [Unreleased]` in `CHANGELOG.md` to
   `## [X.Y.Z] - YYYY-MM-DD`.
3. **Check locally.**
   ```bash
   make test && make gate && make ten-min
   pip install -e ".[docs]" && mkdocs build --strict
   python -m build && twine check --strict dist/*
   (cd sdk-ts && npm ci && npm test && npm pack --dry-run)
   ```
4. **Tag and push**, one tag in each push:
   ```bash
   git commit -am "chore(release): X.Y.Z"
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin master vX.Y.Z
   ```
5. **Watch** *Release (PyPI)* and *Release (npm)*. If the `pypi`
   environment has reviewers, approve it.
6. **Verify** from outside the repository. In a new virtualenv, run
   `pip install "<name>==X.Y.Z"` and
   `python -c "import memd; print(memd.__version__)"`. Run
   `npm view memd-engine version`. Make sure that the docs site shows the
   new changelog.

PyPI never accepts the same file two times. If a publish failed after an
upload, fix the release with a new version. Do not tag the same version
again. If a run failed before the upload (a failed suite, a temporary
error), you can run it again from the Actions tab.
