#!/usr/bin/env python3
"""Validate README snippets against immutable GitHub line permalinks."""

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

README = Path(__file__).resolve().parents[1] / "README.md"
ROOT = README.parent
SOURCE_BLOCK = re.compile(
    r"Source: \[`[^`]+`, lines (\d+)[–-](\d+)\]"
    r"\(https://github\.com/itsmekhoathekid/feature-store/blob/"
    r"([0-9a-f]{40})/([^#)]+)#L(\d+)-L(\d+)\)\n\n"
    r"```[^\n]*\n(.*?)\n```\n\n> \*\*Note\*\*",
    re.DOTALL,
)
FENCED_BLOCK = re.compile(r"```([^\n]*)\n.*?\n```", re.DOTALL)


def git_file(commit: str, relative_path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{commit}:{relative_path}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise ValueError(
            f"Cannot read {relative_path} at {commit}: {result.stderr.strip()}"
        )
    return result.stdout


def excerpt(text: str, start: int, end: int) -> str:
    lines = text.splitlines()
    if start < 1 or end < start or end > len(lines):
        raise ValueError(f"Invalid line range L{start}-L{end} for {len(lines)} lines")
    return "\n".join(lines[start - 1 : end])


def main() -> int:
    if not README.exists():
        print("README.md does not exist yet; references are checked after implementation commit.")
        return 0
    readme_text = README.read_text(encoding="utf-8")
    expected = 8
    blocks = list(SOURCE_BLOCK.finditer(readme_text))
    if len(blocks) < expected:
        print(f"Expected at least {expected} verified source blocks, found {len(blocks)}")
        return 1
    implementation_fences = [
        match for match in FENCED_BLOCK.finditer(readme_text)
        if match.group(1).strip().lower() != "mermaid"
    ]
    if len(implementation_fences) != len(blocks):
        print("Every non-Mermaid code block must have a Source permalink and an immediate Note")
        return 1

    commits = {match.group(3) for match in blocks}
    if len(commits) != 1:
        print("All implementation snippets must reference the same verified commit SHA")
        return 1

    try:
        for match in blocks:
            label_start, label_end = int(match.group(1)), int(match.group(2))
            commit = match.group(3)
            relative_path = unquote(match.group(4))
            url_start, url_end = int(match.group(5)), int(match.group(6))
            snippet = match.group(7)

            if (label_start, label_end) != (url_start, url_end):
                raise ValueError(f"Label and URL ranges differ for {relative_path}")

            current_path = (ROOT / relative_path).resolve()
            if ROOT not in current_path.parents or not current_path.is_file():
                raise ValueError(f"Invalid repository file path: {relative_path}")

            current_excerpt = excerpt(
                current_path.read_text(encoding="utf-8"),
                url_start,
                url_end,
            )
            committed_excerpt = excerpt(
                git_file(commit, relative_path),
                url_start,
                url_end,
            )
            if snippet != committed_excerpt:
                raise ValueError(
                    f"README snippet does not match {relative_path}#L{url_start}-L{url_end} "
                    f"at {commit}"
                )
            if snippet != current_excerpt:
                raise ValueError(
                    f"README snippet is stale for current {relative_path}"
                )
    except ValueError as error:
        print(error)
        return 1

    commit = next(iter(commits))
    print(f"Validated {len(blocks)} README snippets at implementation commit {commit}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
