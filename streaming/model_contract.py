"""Start-up checks of the Spark ML model handed over by Reda (contract section 4).

The hot path refuses to start unless the manifest matches: schema_version 1, the requested
model_version, the expected HDFS URI, the 125-cycle cap and the exact feature-schema hash
computed from contracts/feature_columns.json. There is no heuristic fallback.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

HDFS = "hdfs://namenode:8020/smartpredict"
FEATURE_COLUMNS_FILE = Path("contracts/feature_columns.json")  # Reda's
EXPECTED_FEATURES = (
    "cycle", "op_setting_1", "op_setting_2", "op_setting_3",
    *[f"sensor_{i:02d}" for i in range(1, 22)],
)


class ModelContractError(RuntimeError):
    """The model or its metadata do not match the agreed contract."""


def model_uri(version: str) -> str:
    return f"{HDFS}/models/rul_pipeline/{version}/"


def manifest_uri(version: str) -> str:
    return f"{HDFS}/models/metadata/{version}/manifest.json"


def feature_schema_hash(features: list[str]) -> str:
    """Canonical serialisation fixed by the contract: UTF-8, compact separators, list only."""
    canonical = json.dumps(list(features), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_feature_columns(path: Path = FEATURE_COLUMNS_FILE) -> list[str]:
    if not path.is_file():
        raise ModelContractError(f"{path} missing: Reda's contracts PR must be merged before inference")
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise ModelContractError(f"{path}: schema_version must be 1")
    features = document.get("features")
    if list(features or []) != list(EXPECTED_FEATURES):
        raise ModelContractError(f"{path}: features differ from the 25 contract features in strict order")
    return list(features)


def _normalise_uri(uri: str) -> str:
    return uri.rstrip("/") + "/"


def _normalise_hash(value: str) -> str:
    value = str(value or "").strip().lower()
    return value.split(":", 1)[1] if value.startswith("sha256:") else value


def check_manifest(manifest: dict, version: str, features: list[str]) -> None:
    problems = []
    if manifest.get("schema_version") != 1:
        problems.append(f"schema_version={manifest.get('schema_version')!r}, expected 1")
    if manifest.get("model_version") != version:
        problems.append(f"model_version={manifest.get('model_version')!r}, expected {version!r}")
    if _normalise_uri(str(manifest.get("model_hdfs_uri", ""))) != model_uri(version):
        problems.append(f"model_hdfs_uri={manifest.get('model_hdfs_uri')!r}, expected {model_uri(version)!r}")
    if manifest.get("target_name") != "target_rul":
        problems.append(f"target_name={manifest.get('target_name')!r}, expected 'target_rul'")
    if manifest.get("target_cap_cycles") != 125:
        problems.append(f"target_cap_cycles={manifest.get('target_cap_cycles')!r}, expected 125")
    expected_hash = feature_schema_hash(features)
    if _normalise_hash(manifest.get("feature_schema_hash")) != expected_hash:
        problems.append(f"feature_schema_hash={manifest.get('feature_schema_hash')!r}, expected {expected_hash}")
    if problems:
        raise ModelContractError("model manifest rejected: " + "; ".join(problems))
