"""Markdown audit reporting."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any


def _bytes(value: float) -> str:
    return f"{value / 1024 / 1024:.2f} MiB"


def build_report(
    stats: dict[str, Any],
    schema: dict[str, Any],
    source: str,
    compressed_size: int,
    parquet_bytes: int,
    archive_date: date | None,
    hour: int | None,
) -> str:
    valid = stats["valid"]
    unknown_rate = stats["by_type"].get("UNKNOWN", 0) / valid if valid else 0
    block_range = f"{stats['block_min'] or 'n/a'} / {stats['block_max'] or 'n/a'}"
    lines = [
        "# AtlasPump audit",
        "",
        "## Informations generales",
        f"- Source: `{source}`",
        f"- Date/heure: `{archive_date or 'fichier local'} {hour if hour is not None else ''}`",
        f"- Archive compressee: {_bytes(compressed_size)}",
        f"- SHA-256: `{stats['sha256']}`",
        f"- Duree de traitement: {stats['elapsed']:.2f} s",
        f"- Parquet produits: {_bytes(parquet_bytes)}",
        "",
        "## Statistiques",
        f"- Lignes lues: {stats['total']}",
        f"- JSON valides: {valid}",
        f"- JSON invalides: {stats['invalid']}",
        f"- Evenements inconnus: {unknown_rate:.2%}",
        f"- Signatures uniques: {stats['signatures']}",
        f"- Tokens uniques: {stats['tokens']}",
        f"- Wallets uniques: {stats['wallets']}",
        f"- Blocks uniques: {stats['blocks']}",
        f"- Blocks min/max: {block_range}",
        "",
        "### Categories",
    ]
    lines += [f"- {name}: {count}" for name, count in sorted(stats["by_type"].items())]
    lines += [
        "",
        "## Qualite",
        f"- Doublons detectes: {stats['duplicates']}",
        f"- Sans signature: {stats['missing_signature']}",
        f"- Sans block: {stats['missing_block']}",
        "- Hypothese: les alias de champs sont configures dans `config/default.yaml`; "
        "les champs absents restent nuls.",
        "- Limite: aucune correspondance Pump.fun/PumpSwap n'est affirmee sans champ "
        "de protocole observe.",
        "",
        "## Schema observe",
    ]
    for name, info in schema["fields"].items():
        observed = schema["events_observed"]
        lines.append(
            f"- `{name}`: {info['present']}/{observed} ({info['frequency']:.1%}; "
            f"{', '.join(info['types'])})"
        )
    lines.append("")
    lines.append("### Candidats de type d'evenement")
    for name, values in schema["event_type_candidates"].items():
        top_values = sorted(values.items(), key=lambda item: (-item[1], item[0]))[:20]
        values_text = ", ".join(f"{value}: {count}" for value, count in top_values)
        lines.append(f"- `{name}`: {values_text}")
    lines += ["", "## Projection de stockage"]
    for label, factor in (("24 heures", 24), ("7 jours", 24 * 7), ("30 jours", 24 * 30)):
        archive_projection = _bytes(compressed_size * factor)
        parquet_projection = _bytes(parquet_bytes * factor)
        lines.append(f"- {label}: archive {archive_projection}, Parquet {parquet_projection}")
    if archive_date:
        historic_hours = max(0, (archive_date - date(2026, 4, 18)).days * 24 + (hour or 0) + 1)
        archive_projection = _bytes(compressed_size * historic_hours)
        parquet_projection = _bytes(parquet_bytes * historic_hours)
        lines.append(
            f"- Depuis le 18 avril 2026: archive {archive_projection}, Parquet {parquet_projection}"
        )
    return "\n".join(lines) + "\n"


def write_report(path: Path, report: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")
