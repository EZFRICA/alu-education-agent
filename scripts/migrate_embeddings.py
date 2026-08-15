#!/usr/bin/env python3
"""
Re-embed the local `user_memory` store with the currently configured embedder.

RUN THIS BY HAND, ONCE, AFTER CHANGING THE EMBEDDING MODEL.

`user_memory` rows keep their `content`, so they can be re-embedded on-device
with no network at all — unlike the course registry, which has to be regenerated
and re-downloaded. Until this is run, lance_driver refuses to search a store
whose vectors are in a different space (see EmbeddingMismatch), because querying
across vector spaces returns confidently ranked noise rather than an error.

    # report only, touches nothing
    python scripts/migrate_embeddings.py

    # inspect a specific store
    python scripts/migrate_embeddings.py --db-path /path/to/akili_db

    # actually re-embed, after taking a copy
    python scripts/migrate_embeddings.py --apply

The --apply path rewrites the table in place and updates the embedding stamp.
A backup copy is taken first unless --no-backup is given.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lancedb  # noqa: E402

import embedding_config  # noqa: E402
from app_local.config import settings  # noqa: E402
from app_local.storage import lance_driver  # noqa: E402
from llm_provider import get_embedder  # noqa: E402

TABLE = "user_memory"


def _backup(db_path: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = f"{db_path.rstrip('/')}.backup-{stamp}"
    shutil.copytree(db_path, dest)
    return dest


async def _run(args) -> int:
    if not os.path.exists(args.db_path):
        print(f"No LanceDB at {args.db_path}")
        return 1

    # The stamp belongs to the store being migrated. Without this, passing
    # --db-path would read and write the stamp of the DEFAULT store instead --
    # migrating one store and restamping another.
    if os.path.abspath(args.db_path) != os.path.abspath(settings.LANCE_DB_PATH):
        settings.EMBEDDING_STAMP_PATH = os.path.join(
            os.path.dirname(os.path.abspath(args.db_path.rstrip("/"))),
            "embedding_stamp.json",
        )
    print(f"stamp file:     {settings.EMBEDDING_STAMP_PATH}")

    db = lancedb.connect(args.db_path)
    if TABLE not in lance_driver.list_table_names(db):
        print(f"No '{TABLE}' table at {args.db_path} — nothing to migrate.")
        return 0

    table = db.open_table(TABLE)
    df = table.to_pandas()
    stored_dim = len(df.iloc[0]["vector"]) if len(df) else None
    stamp = lance_driver.read_stamp(TABLE)

    print(f"store:          {args.db_path}")
    print(f"rows:           {len(df)}")
    print(f"stored dim:     {stored_dim}")
    print(f"stored model:   {(stamp or {}).get('model', '<never recorded>')}")
    print(f"configured:     {embedding_config.EMBEDDING_MODEL}")
    print(f"configured dim: {embedding_config.EMBEDDING_DIM}")

    already_current = (
        stored_dim == embedding_config.EMBEDDING_DIM
        and (stamp or {}).get("model") == embedding_config.EMBEDDING_MODEL
    )
    if already_current:
        print("\nAlready in the configured vector space — nothing to do.")
        return 0

    if not len(df):
        print("\nTable is empty; writing the stamp only.")
        if args.apply:
            lance_driver.write_stamp(TABLE)
        return 0

    missing = df["content"].isna() | (df["content"].astype(str).str.len() == 0)
    if missing.any():
        print(
            f"\n{int(missing.sum())} row(s) have no content and cannot be "
            f"re-embedded. They will be dropped:"
        )
        for rid in df.loc[missing, "id"]:
            print(f"  {rid}")

    if not args.apply:
        print("\nDry run. Re-run with --apply to rewrite. Take a copy first.")
        return 0

    keep = df.loc[~missing].copy()
    embedder = get_embedder()
    print(f"\nRe-embedding {len(keep)} row(s)...")
    vectors = await embedder.aembed_documents([str(c) for c in keep["content"]])
    keep["vector"] = [embedding_config.normalize_vector(v) for v in vectors]
    keep["updated_at"] = datetime.now().isoformat()

    if not args.no_backup:
        print(f"backup:         {_backup(args.db_path)}")

    keep = keep.drop(columns=[c for c in ("_distance", "_rowid") if c in keep.columns])
    db.drop_table(TABLE)
    db.create_table(TABLE, data=keep)
    lance_driver.write_stamp(TABLE)

    print(
        f"\nRewrote '{TABLE}': {len(df)} row(s) -> {len(keep)} row(s) at "
        f"{embedding_config.EMBEDDING_DIM} dimensions."
    )
    print("Stamp updated. The course registry must be regenerated separately.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=settings.LANCE_DB_PATH)
    parser.add_argument("--apply", action="store_true",
                        help="rewrite the table. Without this, only reports.")
    parser.add_argument("--no-backup", action="store_true",
                        help="skip the copy taken before rewriting.")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
