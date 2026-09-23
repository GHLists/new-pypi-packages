#!/usr/bin/env python3
"""Fetch PyPI packages created between the previous list and now.

New packages are detected with the PyPI changelog API at pypi.org/pypi: every
``create`` journal entry is inspected through the JSON API and kept when its
timestamp falls inside the requested window. The journal serial of the last
processed entry is stored in the manifest so the next run can resume exactly
where the previous one stopped.
"""

import argparse
import csv
import datetime as dt
import json
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
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_timestamp(value):
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    moment = dt.datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.timezone.utc)
    return moment.astimezone(dt.timezone.utc).replace(microsecond=0)


def timestamp_filename(moment):
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


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
        if not batch:
            break
        entries.extend(batch)
        serial = max(int(entry[4]) for entry in batch)
        if len(batch) < CHANGELOG_LIMIT:
            break
    return entries, serial


def fetch_package(name, user_agent, retries):
    url = PACKAGE_URL.format(name=urllib.parse.quote(name))
    try:
        return fetch_json(url, user_agent, retries=retries)
    except NotFound:
        return None


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
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)


def load_manifest(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_manifest(path, manifest):
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    Path(path).write_text(text, encoding="utf-8")


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
        help="changelog serial to resume from (default: stored in the manifest)",
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
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    until = parse_timestamp(args.until) if args.until else now
    manifest = load_manifest(args.manifest)

    if args.since:
        since = parse_timestamp(args.since)
    else:
        try:
            since = parse_timestamp(manifest["window"])
        except (KeyError, TypeError, ValueError):
            since = until - dt.timedelta(hours=args.lookback_hours)
    if since >= until:
        print(f"nothing to do ({iso(since)} >= {iso(until)})", file=sys.stderr)
        return 0

    proxy = connect(args.user_agent)
    cursor = (
        args.since_serial if args.since_serial is not None else manifest.get("serial")
    )
    try:
        cursor = int(cursor)
    except (TypeError, ValueError):
        cursor = None
    if cursor is None:
        cursor = proxy.changelog_last_serial()
        print(f"no stored serial; starting at changelog serial {cursor}")

    entries, end_serial = fetch_changelog(proxy, cursor)
    created = {}
    for name, _version, timestamp, action, _serial in entries:
        if action != "create" or not name:
            continue
        moment = dt.datetime.fromtimestamp(int(timestamp), dt.timezone.utc)
        if name not in created or moment < created[name]:
            created[name] = moment
    print(
        f"scanned {len(entries)} changelog entries in serial {cursor}..{end_serial}; "
        f"{len(created)} packages created"
    )

    candidates = [
        (name, moment)
        for name, moment in created.items()
        if moment > since and (not args.until or moment <= until)
    ]
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        documents = list(
            executor.map(
                lambda item: fetch_package(item[0], args.user_agent, args.retries),
                candidates,
            )
        )

    rows = []
    missing = 0
    for (name, moment), doc in zip(candidates, documents):
        if doc is None:
            missing += 1
            continue
        rows.append(build_row(name, doc, moment))
    rows.sort(key=lambda row: row["created_at"])
    if missing:
        print(f"skipped {missing} packages without metadata", file=sys.stderr)

    manifest["serial"] = end_serial
    manifest["window"] = iso(until)
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
