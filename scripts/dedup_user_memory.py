#!/usr/bin/env python3
"""
One-off compaction for a `user_memory` table written before C2 was fixed.

Until upsert_local_block did a real upsert, every memory write-back appended a
row. A store that has been used for a while therefore holds many superseded
revisions of the same handful of block ids, one data fragment and one manifest
version per write. This keeps the newest revision of each id (by `updated_at`,
falling back to file order) and drops the rest, then compacts the fragments.

THIS IS NOT RUN AUTOMATICALLY AND NOTHING IN THE APP CALLS IT.
Run it by hand, once, per store. It defaults to a dry run.

    # report only, touches nothing
    python scripts/dedup_user_memory.py

    # inspect a specific store
    python scripts/dedup_user_memory.py --db-path /path/to/akili_db

    # actually rewrite, after taking a copy
    python scripts/dedup_user_memory.py --apply

The --apply path rewrites the table in place. Take a copy of the directory
first; there is no undo.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lancedb  # noqa: E402

from app_local.config import settings  # noqa: E402
from app_local.storage.lance_driver import list_table_names  # noqa: E402

TABLE = "user_memory"


def _backup(db_path: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = f"{db_path.rstrip('/')}.backup-{stamp}"
    shutil.copytree(db_path, dest)
    return dest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=settings.LANCE_DB_PATH)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="rewrite the table. Without this the script only reports.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="skip the automatic copy taken before rewriting.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        print(f"No LanceDB at {args.db_path}")
        return 1

    db = lancedb.connect(args.db_path)
    if TABLE not in list_table_names(db):
        print(f"No '{TABLE}' table at {args.db_path} — nothing to do.")
        return 0

    table = db.open_table(TABLE)
    df = table.to_pandas()
    total = len(df)
    distinct = df["id"].nunique()

    print(f"store:    {args.db_path}")
    print(f"rows:     {total}")
    print(f"distinct: {distinct}")
    print(f"to drop:  {total - distinct}")

    if total == distinct:
        print("\nAlready one row per id — nothing to do.")
        return 0

    dupes = df.groupby("id").size()
    print("\nrevisions per id:")
    for block_id, count in dupes[dupes > 1].sort_values(ascending=False).items():
        print(f"  {block_id:<28} {count}")

    if not args.apply:
        print("\nDry run. Re-run with --apply to rewrite. Take a copy first.")
        return 0

    if "updated_at" in df.columns:
        keep = (
            df.sort_values("updated_at", ascending=False, kind="stable")
            .drop_duplicates(subset="id", keep="first")
        )
    else:
        print("\nNo 'updated_at' column — keeping the last row per id by file order.")
        keep = df.drop_duplicates(subset="id", keep="last")

    if not args.no_backup:
        dest = _backup(args.db_path)
        print(f"\nbackup:   {dest}")

    keep = keep.drop(columns=[c for c in ("_distance", "_rowid") if c in keep.columns])
    db.drop_table(TABLE)
    db.create_table(TABLE, data=keep)

    print(f"\nRewrote '{TABLE}': {total} rows -> {len(keep)} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
