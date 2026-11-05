"""Check a source-only publication tree without displaying matched values."""
import ipaddress
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_SUFFIXES = {".go", ".mod", ".sum", ".md", ".py"}
ALLOWED_NAMES = {".gitignore", "config.example.json", "settings.example.json",
                 "requirements.txt", "LICENSE", "NOTICE", "COPYING"}
GENERATED_DIRS = {".git", "__pycache__", ".venv", "go-cache", "go-mod-cache",
                  "keys", "runtime", "comet_nodes", "results", "logs",
                  ".build", ".runtime", ".benchmark"}
PATTERNS = {
    "serialized private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----"),
    "cloud access identifier": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "service access token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|xox[baprs]-[A-Za-z0-9-]{20,})\b"),
    "credential in URL": re.compile(r"(?:https?|ssh)://[^/\s:@]+:[^/\s@]+@"),
    "absolute personal path": re.compile(r"\b[A-Za-z]:[\\/]|/(?:home|Users)/[^/\s]+"),
    "email address": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "literal password or API secret": re.compile(r'''(?i)\b(?:password|passwd|api[_-]?key|access[_-]?token|client[_-]?secret)\s*[=:]\s*["'][A-Za-z0-9+/=_-]{16,}["']'''),
}
IPV4 = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")


def main():
    findings = []
    files = []
    for path in sorted(ROOT.rglob("*")):
        relative = path.relative_to(ROOT)
        if path.is_symlink():
            findings.append((relative.as_posix(), 0, "symbolic link"))
            continue
        if path.is_dir():
            if path.name in GENERATED_DIRS:
                findings.append((relative.as_posix(), 0, "generated or private directory"))
            continue
        files.append(path)
        if any(part in GENERATED_DIRS for part in relative.parts):
            continue
        if path.suffix not in ALLOWED_SUFFIXES and path.name not in ALLOWED_NAMES:
            findings.append((relative.as_posix(), 0, "non-source or generated file"))
            continue
        try:
            content = path.read_text(encoding="utf-8-sig")
        except (UnicodeError, OSError):
            findings.append((relative.as_posix(), 0, "unreadable text or binary"))
            continue
        if "\x00" in content:
            findings.append((relative.as_posix(), 0, "binary content"))
        for category, pattern in PATTERNS.items():
            for match in pattern.finditer(content):
                line = content.count("\n", 0, match.start()) + 1
                findings.append((relative.as_posix(), line, category))
        for match in IPV4.finditer(content):
            try:
                address = ipaddress.IPv4Address(match.group())
            except ValueError:
                continue
            if not address.is_loopback and not address.is_unspecified:
                line = content.count("\n", 0, match.start()) + 1
                findings.append((relative.as_posix(), line, "non-loopback endpoint"))
    if findings:
        for path, line, category in findings:
            print(f"CHECK {path}:{line}: {category}")
        print(f"Publication check failed: {len(findings)} findings (matched values suppressed).")
        return 1
    print(f"Publication check passed: {len(files)} source/configuration/documentation files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
