"""The PyPI page: pyproject's readme is the project's landing page, and every
link in it works on pypi.org. PyPI does not resolve a relative link (another
.md file, docs/, an image), so each link must be an absolute URL or an
anchor in the same page."""
import os
import re
import tomllib

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
# a Markdown link or image target, and an HTML src/href
_TARGETS = re.compile(r"\]\(\s*<?([^)\s>]+)|\b(?:src|href)=[\"']([^\"']+)[\"']")


def _readme() -> tuple[str, str]:
    with open(os.path.join(ROOT, "pyproject.toml"), "rb") as f:
        name = tomllib.load(f)["project"]["readme"]
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return name, f.read()


def test_the_pypi_readme_is_the_landing_page():
    assert _readme()[0] == "README.md"


def test_the_pypi_readme_has_no_relative_link():
    _name, text = _readme()
    text = re.sub(r"```.*?```", "", text, flags=re.S)  # code blocks are not links
    targets = [a or b for a, b in _TARGETS.findall(text)]
    assert targets, "no link found: the pattern is wrong"
    relative = [t for t in targets if not re.match(r"(?:https?:|mailto:|#)", t)]
    assert not relative, f"relative links break on pypi.org: {relative}"
