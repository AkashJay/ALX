"""Immutable local JSONL receipts with atomic file checkpoint and quarantine."""

import json
from datetime import UTC, datetime
from pathlib import Path

from equiprag_system2.common.config_loader import SOURCES, source_registry
from equiprag_system2.common.state import State, digest, dumps
from equiprag_system2.contracts.events import validate_event


def ingest(state: State, landing: Path, root: Path | None = None) -> int:
    count = 0
    paths_by_source: dict[str, str] = (
        {row.name: row.relative_path for row in source_registry(root)}
        if root
        else {s: s for s in SOURCES}
    )
    for source in SOURCES:
        directory = landing / paths_by_source[source]
        paths = sorted(directory.rglob("*.jsonl")) if directory.is_dir() else []
        if (landing / f"{source}.jsonl").is_file():
            paths.append(landing / f"{source}.jsonl")
        for path in paths:
            raw_file = path.read_bytes()
            uri, checksum = str(path.resolve()), digest(raw_file.hex())
            with state.db:
                previous = state.db.execute(
                    "SELECT hash FROM files WHERE source=? AND uri=?", (source, uri)
                ).fetchone()
                if previous:
                    if previous[0] != checksum:
                        raise ValueError("IMMUTABLE_FILE_CHANGED")
                    continue
                for line_no, raw_bytes in enumerate(raw_file.splitlines(), 1):
                    raw = raw_bytes.decode("utf-8", errors="replace")
                    try:
                        event, rescued, reason = validate_event(source, json.loads(raw))
                    except (ValueError, TypeError):
                        event, rescued, reason = None, {}, "MALFORMED_JSON"
                    state.db.execute(
                        "INSERT INTO bronze VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            digest([source, uri, line_no]),
                            source,
                            uri,
                            line_no,
                            datetime.now(UTC).isoformat(),
                            raw,
                            dumps(event),
                            dumps(rescued),
                            reason,
                        ),
                    )
                    count += 1
                state.db.execute("INSERT INTO files VALUES (?,?,?)", (source, uri, checksum))
    return count
