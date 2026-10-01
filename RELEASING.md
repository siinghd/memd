# Releasing memd

Two workflows publish on a pushed `v*` tag, and one deploys the docs on
pushes to the default branch:

| workflow | publishes | credential |
|---|---|---|
| `.github/workflows/release-pypi.yml` | sdist + wheel to PyPI, and a GitHub Release with them | PyPI trusted publishing (OIDC) |
| `.github/workflows/release-npm.yml` | `@memd/client` (sdk-ts) to npm, with provenance | `NPM_TOKEN` secret |
| `.github/workflows/docs.yml` | the docs site to GitHub Pages | the workflow's own token |

## Once, before the first release

### 0. Decisions only the owner can make

- **The PyPI name.** `memd` is taken on PyPI by an unrelated project
  (checked 2026-10-01); `memd-engine` was free that day. Set
  `[project].name` in `pyproject.toml` to the chosen name. The import name
  (`import memd`) and the `memd` command do not change. Then update the
  install lines (`grep -rn 'pip install memd\|"memd\[' --include=*.md .`).
- **The npm scope.** `@memd/client` needs an npm organization `memd`
  (create it on npmjs.com), or rename the package in `sdk-ts/package.json`
  and its README.
- **The GitHub owner and repository** are `github.com/siinghd/memd`, set in
  `pyproject.toml` (`[project.urls]`), `sdk-ts/package.json` (`repository`,
  `homepage`) and `mkdocs.yml` (`repo_url`, `repo_name`, `site_url`). npm
  provenance fails unless `repository.url` in `sdk-ts/package.json` is the
  repository the workflow runs in: update all three if the repo moves.
- **The docs URL.** GitHub Pages serves `https://<owner>.github.io/<repo>/`;
  a custom domain needs a `docs/CNAME` file and a DNS record. Set `site_url`
  in `mkdocs.yml` and add a `Documentation` entry to `[project.urls]`.

### 1. Create the repository and push

Create an empty GitHub repository (no README, license or .gitignore), then:

```bash
git remote add origin git@github.com:<owner>/<repo>.git
git push -u origin master
git push origin --tags
```

Pushing the six existing tags in one push triggers nothing: GitHub creates
no push events when more than three tags are pushed at once. That is what
you want. The tagged commits up to `v0.3.2` predate these workflows (they
carry the old `release.yml`), so the first release published by
`release-pypi.yml` is the first tag cut after this branch is merged.

### 2. PyPI: a trusted publisher

On pypi.org: *Your account → Publishing → Add a new pending publisher*
(the project does not exist yet; the first upload creates it):

| field | value |
|---|---|
| PyPI project name | the name from step 0 |
| Owner / Repository | `<owner>` / `<repo>` |
| Workflow name | `release-pypi.yml` |
| Environment name | `pypi` |

On GitHub, *Settings → Environments → New environment* `pypi`; add
required reviewers to make each publish wait for an approval.

### 3. npm: the token

On npmjs.com create a granular access token with read and write access to
the `@memd` scope (or an automation token), and store it as the
`NPM_TOKEN` secret: in the `npm` environment (*Settings → Environments*)
or as a repository secret. Without it, `release-npm.yml` skips the publish
with a warning. (npm's own trusted publishing for GitHub Actions can
replace the token later: drop `NODE_AUTH_TOKEN` from the publish step.)

### 4. GitHub Pages

*Settings → Pages → Build and deployment → Source: GitHub Actions*. Then
run the *Docs* workflow once (*Actions → Docs → Run workflow*); afterwards
it deploys on every push to the default branch that touches the docs.

## Every release

1. **Versions.** `[project].version` in `pyproject.toml` and `__version__`
   in `src/memd/__init__.py`: the workflow refuses a tag unless both say
   the tag's version. The TypeScript SDK has its own version in
   `sdk-ts/package.json`: bump it when the SDK changed; a tag publishes it
   only when that version is not on npm yet.
2. **Changelog.** Move `## [Unreleased]` in `CHANGELOG.md` to
   `## [X.Y.Z] - YYYY-MM-DD`.
3. **Check locally.**
   ```bash
   make test && make gate && make ten-min
   pip install -e ".[docs]" && mkdocs build --strict
   python -m build && twine check --strict dist/*
   (cd sdk-ts && npm ci && npm test && npm pack --dry-run)
   ```
4. **Tag and push** one tag per push:
   ```bash
   git commit -am "chore(release): X.Y.Z"
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin master vX.Y.Z
   ```
5. **Watch** *Release (PyPI)* (approve the `pypi` environment if it has
   reviewers) and *Release (npm)*.
6. **Verify** from outside: `pip install "<name>==X.Y.Z"` in a fresh
   virtualenv and `python -c "import memd; print(memd.__version__)"`;
   `npm view @memd/client version`; the docs site shows the new changelog.

PyPI never accepts the same file twice: a release whose publish failed
after an upload is fixed by a new version, not by re-tagging. A run that
failed before uploading (a red suite, a transient error) can be re-run
from the Actions tab.
