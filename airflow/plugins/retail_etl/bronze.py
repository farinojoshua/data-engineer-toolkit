"""Bronze Layer: Raw CSV ingestion with metadata tracking."""
import json
import shutil
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def get_bronze_state(state_dir: str) -> dict:
    """Load the bronze ingestion state from disk, or return a blank state."""
    state_file = Path(state_dir) / "bronze_state.json"
    if state_file.exists():
        try:
            with open(state_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[Bronze] Warning: could not read state file ({exc}). Starting fresh.")
    return {"ingested_files": [], "last_run": None, "new_files_this_run": 0}


def save_bronze_state(state_dir: str, state: dict) -> None:
    """Persist the bronze ingestion state to disk."""
    Path(state_dir).mkdir(parents=True, exist_ok=True)
    state_file = Path(state_dir) / "bronze_state.json"
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Main ingestion function
# ---------------------------------------------------------------------------

def ingest_to_bronze(
    raw_dir: str,
    bronze_dir: str,
    state_dir: str,
    unique_id: str,
) -> list:
    """
    Scan the raw directory for new CSV files and copy them to the bronze
    landing zone, writing a JSON metadata sidecar next to each copied file.

    Incremental: already-ingested files (tracked in bronze_state.json) are
    skipped so re-runs are idempotent.

    Parameters
    ----------
    raw_dir    : root raw data directory   (/opt/airflow/data/raw)
    bronze_dir : bronze landing zone       (/opt/airflow/data/bronze)
    state_dir  : state persistence folder  (/opt/airflow/data/state)
    unique_id  : user-specific sub-folder  (e.g. "JoshuaFarino")

    Returns
    -------
    list of relative paths that were ingested in this run
    """
    state = get_bronze_state(state_dir)
    already_ingested: set = set(state.get("ingested_files", []))

    raw_base = Path(raw_dir) / unique_id
    bronze_base = Path(bronze_dir)

    if not raw_base.exists():
        print(
            f"[Bronze] Raw directory not found: {raw_base}. "
            "Run the data generator first."
        )
        return []

    new_files: list = []
    ingested_at = datetime.now().isoformat()

    for csv_file in sorted(raw_base.rglob("*.csv")):
        relative_path = str(csv_file.relative_to(raw_base))

        if relative_path in already_ingested:
            print(f"[Bronze] Skip (already ingested): {relative_path}")
            continue

        table_name = csv_file.parent.name
        target_dir = bronze_base / table_name
        target_dir.mkdir(parents=True, exist_ok=True)

        target_file = target_dir / csv_file.name
        shutil.copy2(csv_file, target_file)

        # Write metadata sidecar
        meta = {
            "source_file": str(csv_file),
            "table": table_name,
            "ingested_at": ingested_at,
            "file_size_bytes": csv_file.stat().st_size,
            "unique_id": unique_id,
        }
        meta_path = target_dir / (csv_file.stem + ".meta.json")
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

        already_ingested.add(relative_path)
        new_files.append(relative_path)
        print(
            f"[Bronze] Ingested: {relative_path}  "
            f"({meta['file_size_bytes']:,} bytes)  →  {target_file}"
        )

    # Persist updated state
    state["ingested_files"] = sorted(already_ingested)
    state["last_run"] = ingested_at
    state["new_files_this_run"] = len(new_files)
    save_bronze_state(state_dir, state)

    if new_files:
        print(f"[Bronze] Done. {len(new_files)} new file(s) ingested this run.")
    else:
        print("[Bronze] Done. No new files found.")

    return new_files
