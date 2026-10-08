# Releasing memd

When you push a `v*` tag, two workflows publish the release. A third
workflow deploys the docs when you push to the default branch:

| workflow | publishes | credential |
|---|---|---|
| `.github/workflows/release-pypi.yml` | sdist + wheel to PyPI, and a GitHub Release with them | PyPI trusted publishing (OIDC, OpenID Connect) |
| `.github/workflows/release-npm.yml` | `memd-engine` (sdk-ts) to npm, with provenance, as a staged version that the owner approves | trusted publishing (OIDC), environment `npm` |
| `.github/workflows/docs.yml` | the docs site to GitHub Pages | the workflow's own token |

## One-time setup

This setup is complete for `siinghd/memd`. Do these steps again only if the
repository moves, or for a fork that publishes its own packages.

### 0. Names and URLs

- **The PyPI name** is `memd-engine` (an unrelated project has `memd` on
  PyPI). The import name (`import memd`) and the `memd` command do not
  change. PyPI has a trusted publisher for `memd-engine`: `siinghd/memd`,
  workflow `release-pypi.yml`, environment `pypi`.
- **The npm name** is `memd-engine`, without a scope. It is the same name
  as on PyPI.
- **The GitHub owner and repository** are `github.com/siinghd/memd`. They
  are set in `pyproject.toml` (`[project.urls]`), `sdk-ts/package.json`
  (`repository`, `homepage`) and `mkdocs.yml` (`repo_url`, `repo_name`,
  `site_url`). npm provenance fails if `repository.url` in
  `sdk-ts/package.json` is not the repository that the workflow runs in. If
  the repository moves, update all three files.
- **The docs URL** is `https://siinghd.github.io/memd/` (GitHub Pages). It
  is `site_url` in `mkdocs.yml` and `Documentation` in `[project.urls]`. A
  custom domain needs a `docs/CNAME` file and a DNS (Domain Name System)
  record.

### 1. Create the repository and push

Create an empty GitHub repository (no README, license or .gitignore). Then
run these commands:

```bash
git remote add origin git@github.com:<owner>/<repo>.git
git push -u origin master
git push origin --tags
```

GitHub creates no push events when you push more than three tags at once.
Thus, this push does not publish the existing tags again.

### 2. PyPI: a trusted publisher

On pypi.org, add a trusted publisher. For a project that does not exist
yet, open *Your account → Publishing → Add a new pending publisher*. The
first upload then creates the project. For an existing project, open the
*Publishing* settings of the project. Enter these values:

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

The package also uses npm staged publishing. A version that the workflow
publishes does not go live at once. It waits on npmjs.com until the owner
approves it.

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
   environment has reviewers, approve it. If the workflow published a new
   SDK version, approve the staged version on npmjs.com. Until you approve
   it, npm does not serve it.
6. **Verify** from outside the repository. In a new virtualenv, run
   `pip install "<name>==X.Y.Z"` and
   `python -c "import memd; print(memd.__version__)"`. Run
   `npm view memd-engine version`. Make sure that the docs site shows the
   new changelog.

PyPI never accepts the same file two times. If a publish failed after an
upload, fix the release with a new version. Do not tag the same version
again. If a run failed before the upload (a failed suite, a temporary
error), you can run it again from the Actions tab.
