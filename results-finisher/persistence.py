import json
import os
import time
import uuid
from typing import Dict, Generator, Optional


class FinisherPersistence:
    """Handles durable storage for pending batches and query states."""

    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = base_dir or "/tmp/results_finisher_state"
        self.batches_dir = os.path.join(self.base_dir, "batches")
        self.manifest_dir = os.path.join(self.base_dir, "manifests")
        os.makedirs(self.batches_dir, exist_ok=True)
        os.makedirs(self.manifest_dir, exist_ok=True)

    def save_batch(self, metadata: Dict[str, str], body: bytes) -> Dict[str, str]:
        """Persist a batch payload plus metadata. Returns stored metadata."""
        batch_id = f"{int(time.time() * 1000)}_{uuid.uuid4().hex}"
        data_filename = f"{batch_id}.bin"
        data_path = os.path.join(self.batches_dir, data_filename)

        tmp_data = f"{data_path}.tmp"
        with open(tmp_data, "wb") as fh:
            fh.write(body)
        os.replace(tmp_data, data_path)

        stored_meta = {
            "id": batch_id,
            "data_file": data_filename,
            **metadata,
        }
        stored_meta["_data_path"] = data_path
        return stored_meta

    def iter_batches(self) -> Generator[Dict[str, str], None, None]:
        """Yield metadata for all persisted batches using manifests."""
        if not os.path.exists(self.manifest_dir):
            return
        for manifest_file in sorted(os.listdir(self.manifest_dir)):
            if not manifest_file.endswith(".json"):
                continue
            manifest_path = os.path.join(self.manifest_dir, manifest_file)
            try:
                with open(manifest_path, "r", encoding="utf-8") as fh:
                    entries = json.load(fh)
            except Exception:
                continue

            for entry in entries:
                data_file = entry.get("data_file")
                if not data_file:
                    continue
                data_path = os.path.join(self.batches_dir, data_file)
                try:
                    yield {
                        "id": os.path.splitext(data_file)[0],
                        "data_file": data_file,
                        "_data_path": data_path,
                        "_manifest_path": manifest_path,
                    }
                except Exception:
                    continue

    def load_batch_bytes(self, batch_meta: Dict[str, str]) -> Optional[bytes]:
        data_path = batch_meta.get("_data_path")
        if not data_path or not os.path.exists(data_path):
            return None
        with open(data_path, "rb") as fh:
            return fh.read()

    def delete_batch(self, batch_meta: Dict[str, str]):
        """Remove persisted data/meta files for a batch."""
        data_path = batch_meta.get("_data_path")
        if data_path and os.path.exists(data_path):
            os.remove(data_path)

    def append_manifest(self, client_id: str, query_id: str, meta_record: Dict[str, str]):
        """Append batch metadata entry to the per-query manifest."""
        filename = f"{client_id}__{query_id}.json"
        path = os.path.join(self.manifest_dir, filename)
        entries = []
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    entries = json.load(fh)
            except Exception:
                entries = []
        entries.append(
            {
                "data_file": meta_record.get("data_file"),
            }
        )
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(entries, fh)
        os.replace(tmp_path, path)

    def load_manifest(self, client_id: str, query_id: str) -> list[Dict[str, str]]:
        filename = f"{client_id}__{query_id}.json"
        path = os.path.join(self.manifest_dir, filename)
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return []

    def delete_manifest(self, client_id: str, query_id: str):
        filename = f"{client_id}__{query_id}.json"
        path = os.path.join(self.manifest_dir, filename)
        if os.path.exists(path):
            os.remove(path)
