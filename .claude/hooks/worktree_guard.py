"""PreToolUse guard (Bash|PowerShell): enforce "branch + worktree per task".

Blocks `git checkout -b` / `git switch -c` run outside a linked worktree,
and nudges to remove a worktree once its branch is deleted. See CLAUDE.MD's
Workflow section. Check docs/decisions.md, 2026-10-20 for more details.
"""

import contextlib
import json
import re
import subprocess
import sys


def run(*args, cwd=None):
    try:
        return subprocess.run(
            args, capture_output=True, text=True, timeout=10, cwd=cwd
        ).stdout.strip()
    except Exception:
        return ""


LEADING_CD = re.compile(r'^\s*cd\s+(?:"([^"]+)"|\'([^\']+)\'|(\S+))\s*(?:&&|;)')


def leading_cd_target(cmd):
    """If cmd opens with `cd <path> &&`/`;`, return <path>, else None.

    The Bash/PowerShell tools here reset the working directory back to the
    project root after every call, so a worktree is always entered via a
    `cd <worktree> && git ...` *within one command* rather than a `cd` that
    persists to the next call. The hook's own subprocess still starts in the
    project root regardless, so it has to read that leading `cd` out of the
    command text to know where the rest of the command will actually run.
    """
    m = LEADING_CD.match(cmd)
    if not m:
        return None
    return next(g for g in m.groups() if g)


def in_linked_worktree(cwd=None):
    gitdir = run("git", "rev-parse", "--git-dir", cwd=cwd)
    return "worktrees" + ("\\" if "\\" in gitdir else "/") in gitdir


def deny(reason):
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": reason,
                }
            }
        )
    )


def nudge(message):
    print(json.dumps({"systemMessage": message}))


CREATE_BRANCH = re.compile(r"\bgit\s+(checkout\s+-[bB]\b|switch\s+(-[cC]\b|--create\b))")
DELETE_BRANCH = re.compile(r"\bgit\s+branch\s+-[dD]\s+(\S+)|--delete[= ]+(\S+)")


def main():
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return
    cmd = (payload.get("tool_input") or {}).get("command", "") or ""
    if not cmd:
        return
    cwd = leading_cd_target(cmd)
    if run("git", "rev-parse", "--git-dir", cwd=cwd) == "":
        return  # not inside a git repo

    if CREATE_BRANCH.search(cmd) and not in_linked_worktree(cwd):
        deny(
            "This creates a branch directly in the main checkout, which changes "
            "the branch everyone sharing this working directory sees next "
            "(see docs/decisions.md, 2026-10-20: worktree mix-up). Use a "
            "worktree instead: git worktree add ../<worktree-dir> -b <branch>, "
            "then cd into it. CLAUDE.MD's Workflow section requires a "
            "worktree per task."
        )
        return

    m = DELETE_BRANCH.search(cmd)
    if m:
        branch = m.group(1) or m.group(2)
        for line in run("git", "worktree", "list").splitlines():
            if f"[{branch}]" in line:
                path = line.split()[0]
                nudge(
                    f"Reminder: worktree at {path} is still registered for "
                    f"branch '{branch}'. Once it's merged, run: "
                    f"git worktree remove {path}"
                )
                break


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        main()
