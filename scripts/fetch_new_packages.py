#!/usr/bin/env python3
"""Fetch PyPI packages created between the previous list and now.

New packages are detected with the PyPI changelog API at pypi.org/pypi: every
``create`` journal entry is inspected through the JSON API and kept when its
timestamp falls inside the requested window. The journal serial of the last
processed entry and unresolved package names are stored in the manifest so
the next run can resume without losing records.
"""

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

XMLRPC_URL = "https://pypi.org/pypi"
PACKAGE_URL = "https://pypi.org/pypi/{name}/json"
DEFAULT_USER_AGENT = (
    "new-pypi-packages/1.0 (https://github.com/GHLists/new-pypi-packages)"
)

CHANGELOG_LIMIT = 50000
MAX_PENDING_ATTEMPTS = 5
SUMMARY_LIMIT = 200
CSV_HEADER = ("created_at", "package", "version", "author", "license", "size", "summary")


class NotFound(Exception):
    pass


class Transport(xmlrpc.client.Transport):
    def __init__(self, user_agent):
        super().__init__()
        self.user_agent = user_agent

    def send_headers(self, connection, headers):
        headers.append(("User-Agent", self.user_agent))
        super().send_headers(connection, headers)


def iso(moment):
    moment = moment.astimezone(dt.timezone.utc)
    if moment.microsecond:
        fraction = f"{moment.microsecond:06d}".rstrip("0")
        return moment.strftime("%Y-%m-%dT%H:%M:%S") + f".{fraction}Z"
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_timestamp(value):
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    moment = dt.datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.timezone.utc)
    return moment.astimezone(dt.timezone.utc)


def timestamp_filename(moment):
    moment = moment.astimezone(dt.timezone.utc)
    stamp = moment.strftime("%Y-%m-%dT%H-%M-%S")
    if moment.microsecond:
        stamp += "-" + f"{moment.microsecond:06d}".rstrip("0")
    return stamp + "Z"


def fetch_json(url, user_agent, retries=3, backoff=5.0):
    last_error = None
    for attempt in range(1, retries + 1):
        request = urllib.request.Request(
            url,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                raise NotFound(url) from error
            last_error = error
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
        if attempt < retries:
            print(f"attempt {attempt} failed ({last_error}), retrying", file=sys.stderr)
            time.sleep(backoff * attempt)
    raise RuntimeError(f"failed to fetch {url}: {last_error}")


def connect(user_agent):
    return xmlrpc.client.ServerProxy(
        XMLRPC_URL, transport=Transport(user_agent), allow_none=True
    )


def call_changelog(proxy, serial, retries=3, backoff=5.0):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return proxy.changelog_since_serial(serial)
        except (xmlrpc.client.Error, OSError, TimeoutError) as error:
            last_error = error
        if attempt < retries:
            print(f"attempt {attempt} failed ({last_error}), retrying", file=sys.stderr)
            time.sleep(backoff * attempt)
    raise RuntimeError(f"failed to read PyPI changelog: {last_error}")


def fetch_changelog(proxy, start_serial):
    entries = []
    serial = start_serial
    for _ in range(1000):
        batch = call_changelog(proxy, serial)
        if not isinstance(batch, list):
            raise RuntimeError("PyPI changelog response is not a list")
        if not batch:
            return entries, serial, True
        try:
            batch_serial = max(int(entry[4]) for entry in batch)
        except (IndexError, TypeError, ValueError) as error:
            raise RuntimeError("PyPI changelog contains an invalid serial") from error
        if batch_serial <= serial:
            raise RuntimeError("PyPI changelog serial did not advance")
        entries.extend(batch)
        serial = batch_serial
        if len(batch) < CHANGELOG_LIMIT:
            return entries, serial, True
    return entries, serial, False


def normalized_name(value):
    return re.sub(r"[-_.]+", "-", str(value or "")).lower()


def fetch_package(name, user_agent, retries):
    url = PACKAGE_URL.format(name=urllib.parse.quote(name))
    try:
        document = fetch_json(url, user_agent, retries=retries)
    except NotFound:
        return None
    if not isinstance(document, dict):
        return None
    info = document.get("info")
    if not isinstance(info, dict) or normalized_name(info.get("name")) != normalized_name(
        name
    ):
        return None
    version = info.get("version")
    releases = document.get("releases")
    if not isinstance(version, str) or not version:
        return None
    if not isinstance(releases, dict) or version not in releases:
        return None
    return document


def release_size(files):
    total = 0
    seen = False
    for file in files or []:
        size = file.get("size")
        if isinstance(size, int):
            total += size
            seen = True
    return total if seen else ""


def clean_text(value, limit=SUMMARY_LIMIT):
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "\u2026"
    return text


def build_row(name, doc, created):
    info = doc.get("info") or {}
    releases = doc.get("releases") or {}
    version = info.get("version") or ""
    author = (
        info.get("author")
        or info.get("maintainer")
        or info.get("author_email")
        or info.get("maintainer_email")
        or ""
    )
    license_name = info.get("license_expression") or info.get("license") or ""
    return {
        "created_at": iso(created),
        "package": info.get("name") or name,
        "version": version,
        "author": clean_text(author, 100),
        "license": clean_text(license_name, 100),
        "size": release_size(releases.get(version)),
        "summary": clean_text(info.get("summary")),
    }


def write_csv(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def load_manifest(path):
    manifest_path = Path(path)
    try:
        text = manifest_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError as error:
        raise RuntimeError(f"could not read manifest {manifest_path}: {error}") from error
    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"manifest {manifest_path} is not valid JSON") from error
    if not isinstance(data, dict):
        raise RuntimeError(f"manifest {manifest_path} must contain a JSON object")
    version = data.get("state_version", 1)
    if version != 1:
        raise RuntimeError(f"manifest {manifest_path} has an unsupported state version")
    return data


def save_manifest(path, manifest):
    manifest_path = Path(path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = manifest_path.with_name(f".{manifest_path.name}.tmp")
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, manifest_path)


def load_pending(manifest):
    pending = {}
    raw_pending = manifest.get("pending", [])
    if not isinstance(raw_pending, list):
        raise RuntimeError("manifest pending must be a list")
    for item in raw_pending:
        if not isinstance(item, dict):
            raise RuntimeError("manifest pending entries must be objects")
        name = item.get("package")
        if not isinstance(name, str) or not name:
            raise RuntimeError("manifest pending entry has an invalid package")
        if name in pending:
            raise RuntimeError(f"manifest contains duplicate pending package {name}")
        try:
            created = parse_timestamp(item.get("created_at"))
            candidate_since = parse_timestamp(item.get("since"))
        except (TypeError, ValueError) as error:
            raise RuntimeError(
                f"manifest pending entry for {name} is invalid"
            ) from error
        attempts = item.get("attempts", 0)
        if not isinstance(attempts, int) or attempts < 0:
            raise RuntimeError(
                f"manifest pending entry for {name} has an invalid attempts count"
            )
        if created <= candidate_since:
            raise RuntimeError(f"manifest pending entry for {name} is inconsistent")
        pending[name] = {
            "package": name,
            "created": created,
            "since": candidate_since,
            "attempts": attempts,
        }
    return pending


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since",
        help="UTC start timestamp as ISO 8601 (default: end of the last list)",
    )
    parser.add_argument(
        "--until",
        help="UTC end timestamp as ISO 8601 (default: now)",
    )
    parser.add_argument(
        "--since-serial",
        type=int,
        help="changelog serial to resume from; requires --since",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="parallel package metadata requests (default: 8)",
    )
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--manifest", default="latest.json")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=1.0,
        help="window length when no previous list exists (default: 1)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    now = dt.datetime.now(dt.timezone.utc)
    until = parse_timestamp(args.until) if args.until else now
    manifest = load_manifest(args.manifest)

    if args.since_serial is not None and args.since is None:
        raise RuntimeError("--since-serial requires an explicit --since timestamp")
    if args.since:
        since = parse_timestamp(args.since)
        if "window" in manifest and args.since_serial is None:
            stored_window = parse_timestamp(manifest["window"])
            if since < stored_window:
                raise RuntimeError(
                    "timestamp-only backfill cannot move the changelog cursor; "
                    "provide --since-serial"
                )
    elif "window" in manifest:
        since = parse_timestamp(manifest["window"])
    else:
        since = until - dt.timedelta(hours=args.lookback_hours)

    if args.since_serial is not None:
        cursor = args.since_serial
    elif "serial" in manifest:
        cursor = manifest["serial"]
    else:
        raise RuntimeError("no stored changelog serial; provide --since-serial")
    try:
        cursor = int(cursor)
    except (TypeError, ValueError) as error:
        raise RuntimeError("manifest contains an invalid changelog serial") from error
    if cursor < 0:
        raise RuntimeError("changelog serial cannot be negative")

    pending = load_pending(manifest)
    if since >= until:
        print(f"nothing to do ({iso(since)} >= {iso(until)})", file=sys.stderr)
        return 0

    proxy = connect(args.user_agent)
    entries, end_serial, exhausted = fetch_changelog(proxy, cursor)
    created = {}
    for entry in entries:
        try:
            name, _version, timestamp, action, _serial = entry
            name = str(name or "")
            moment = dt.datetime.fromtimestamp(int(timestamp), dt.timezone.utc)
        except (TypeError, ValueError) as error:
            raise RuntimeError("PyPI changelog contains an invalid entry") from error
        if action == "create" and not name:
            raise RuntimeError("PyPI create entry is missing its package name")
        if action != "create" or moment <= since:
            continue
        candidate = {
            "package": name,
            "created": moment,
            "since": since,
            "attempts": 0,
        }
        current = created.get(name)
        if current is None or candidate["created"] < current["created"]:
            created[name] = candidate
    for name, candidate in created.items():
        pending.setdefault(name, candidate)
    print(
        f"scanned {len(entries)} changelog entries in serial {cursor}..{end_serial}; "
        f"{len(created)} new candidates and {len(pending)} pending candidates"
    )

    manifest["serial"] = end_serial
    manifest["source_truncated"] = not exhausted
    if not exhausted:
        manifest["window"] = iso(since)
        manifest["pending"] = [
            {
                "package": name,
                "created_at": iso(pending[name]["created"]),
                "since": iso(pending[name]["since"]),
                "attempts": pending[name]["attempts"],
            }
            for name in sorted(pending)
        ]
        save_manifest(args.manifest, manifest)
        print(
            "PyPI changelog reached its page limit; candidates were persisted "
            "for the next run",
            file=sys.stderr,
        )
        return 0

    candidates = sorted(pending)
    eligible = [name for name in candidates if pending[name]["created"] <= until]
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        documents = list(
            executor.map(
                lambda name: fetch_package(name, args.user_agent, args.retries),
                eligible,
            )
        )

    rows = []
    next_pending = {
        name: pending[name] for name in candidates if pending[name]["created"] > until
    }
    missing = 0
    dropped = 0
    for name, doc in zip(eligible, documents):
        if doc is None:
            missing += 1
            candidate = pending[name]
            candidate["attempts"] += 1
            if candidate["attempts"] < MAX_PENDING_ATTEMPTS:
                next_pending[name] = candidate
            else:
                dropped += 1
            continue
        rows.append(build_row(name, doc, pending[name]["created"]))
    rows.sort(key=lambda row: row["created_at"])
    if missing:
        print(
            f"kept {missing} packages pending without PyPI metadata",
            file=sys.stderr,
        )
    if dropped:
        print(
            f"dropped {dropped} packages unresolved after {MAX_PENDING_ATTEMPTS} "
            "attempts",
            file=sys.stderr,
        )
    if next_pending:
        print(f"kept {len(next_pending)} packages pending for a later run")

    manifest["window"] = iso(until)
    manifest["source_truncated"] = False
    manifest["pending"] = [
        {
            "package": name,
            "created_at": iso(next_pending[name]["created"]),
            "since": iso(next_pending[name]["since"]),
            "attempts": next_pending[name]["attempts"],
        }
        for name in sorted(next_pending)
    ]
    if rows:
        output = Path(args.output_dir) / f"new-packages-{timestamp_filename(until)}.csv"
        write_csv(output, rows)
        manifest["list"] = {
            "path": output.as_posix(),
            "from": iso(since),
            "to": iso(until),
            "count": len(rows),
        }
        print(
            f"wrote {len(rows)} packages created between {iso(since)} "
            f"and {iso(until)} to {output}"
        )
    else:
        print(f"no new packages between {iso(since)} and {iso(until)}")
    save_manifest(args.manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
