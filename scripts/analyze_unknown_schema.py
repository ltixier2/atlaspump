"""Streaming schema analysis for the UNKNOWN Parquet output of one audit."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

INPUT = Path("data/parquet/events_unknown.parquet")
OUTPUT = Path("data/reports/schema_analysis_01.md")
ADDRESS = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,48}$")
MAX_EXAMPLES = 3


def shape(value: Any) -> Any:
    if isinstance(value, dict):
        keys = list(value)
        if keys and all(ADDRESS.match(key) for key in keys):
            values = sorted({json.dumps(shape(item), sort_keys=True) for item in value.values()})
            return {"<address-key>": [json.loads(item) for item in values]}
        return {key: shape(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        variants = sorted({json.dumps(shape(item), sort_keys=True) for item in value})
        return [json.loads(item) for item in variants]
    return type(value).__name__


def walk(value: Any, path: str, keys: Counter[str], depths: Counter[int]) -> None:
    depths[path.count(".") + path.count("[]")] += 1
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{path}.{key}" if path else key
            keys[child] += 1
            walk(item, child, keys, depths)
    elif isinstance(value, list):
        for item in value:
            walk(item, f"{path}[]", keys, depths)


def truncate(value: Any) -> Any:
    if isinstance(value, str):
        return value if len(value) <= 100 else value[:97] + "..."
    if isinstance(value, list):
        return [truncate(item) for item in value[:4]]
    if isinstance(value, dict):
        return {key: truncate(item) for key, item in list(value.items())[:20]}
    return value


def markdown_table(rows: list[tuple[str, int]], total: int) -> list[str]:
    return [f"| `{name}` | {count:,} | {count / total:.4%} |" for name, count in rows]


def main() -> None:
    parquet = pq.ParquetFile(INPUT)
    total = parquet.metadata.num_rows
    structures: Counter[str] = Counter()
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    roots: Counter[str] = Counter()
    nested: Counter[str] = Counter()
    depths: Counter[int] = Counter()
    values: dict[str, Counter[str]] = defaultdict(Counter)
    discriminants = ("txType", "pool", "poolCreatedBy", "tokenProgram")

    for group in range(parquet.metadata.num_row_groups):
        table = parquet.read_row_group(group, columns=["raw_payload_json"])
        for raw in table.column(0).to_pylist():
            payload = json.loads(raw)
            signature = json.dumps(shape(payload), sort_keys=True, separators=(",", ":"))
            structures[signature] += 1
            if len(examples[signature]) < MAX_EXAMPLES:
                examples[signature].append(truncate(payload))
            for key in payload:
                roots[key] += 1
            walk(payload, "", nested, depths)
            for field in discriminants:
                if field in payload:
                    values[field][str(payload[field])] += 1

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Analyse de schema des evenements UNKNOWN",
        "",
        f"- Source: `{INPUT}`",
        f"- Evenements analyses: {total:,}",
        f"- Structures distinctes (cles, types et imbrication): {len(structures):,}",
        "",
        "## 30 structures les plus frequentes",
        "",
    ]
    for index, (signature, count) in enumerate(structures.most_common(30), start=1):
        definition = json.loads(signature)
        lines.extend(
            [
                f"### Structure {index} - {count:,} ({count / total:.4%})",
                "",
                "**Cles de premier niveau:** `" + "`, `".join(definition) + "`",
                "",
                "**Forme imbriquee:**",
                "```json",
                json.dumps(definition, indent=2)[:12000],
                "```",
                "",
                "**Exemples tronques:**",
            ]
        )
        for example in examples[signature]:
            lines.extend(["```json", json.dumps(example, indent=2, ensure_ascii=False), "```"])
    lines.extend(
        [
            "",
            "## Cles observees",
            "",
            "### Premier niveau",
            "",
            "| Cle | Occurrences | Pourcentage |",
            "| --- | ---: | ---: |",
            *markdown_table(roots.most_common(50), total),
            "",
            "### Principales cles imbriquees",
            "",
            "| Chemin | Occurrences | Pourcentage |",
            "| --- | ---: | ---: |",
            *markdown_table(nested.most_common(80), total),
            "",
            "### Profondeur",
            "",
        ]
    )
    lines.extend(f"- Profondeur {depth}: {count:,}" for depth, count in sorted(depths.items()))
    lines.extend(["", "## Valeurs discriminantes", ""])
    for field in discriminants:
        lines.extend(
            [
                f"### `{field}`",
                "",
                "| Valeur | Occurrences | Pourcentage |",
                "| --- | ---: | ---: |",
                *markdown_table(values[field].most_common(40), total),
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation prudente",
            "",
            "- `txType` est le meilleur candidat observe pour le type: les valeurs `buy`, `sell` "
            "et `transfer` sont explicitement presentes.",
            "- `pool` et `poolCreatedBy` sont les meilleurs candidats observes pour le protocole: "
            "`pump-amm` et `pump` apparaissent dans les memes objets que `txType=buy|sell`.",
            "- `signature`, `block`, `timestamp`, `localTimestamp`, `mint`, `txSigner`, "
            "`solAmount` et `tokenAmount` apparaissent dans les objets de swap observes.",
            "- Les objets `postBalances` et `tradersInvolved` utilisent des adresses comme cles; "
            "les transferts sont dans `transfers` avec `from`, `to`, `mint`, `amount` "
            "et `isSolana`",
            "",
            "## Zones ambiguës",
            "",
            "- Une structure `transfer` peut etre un transfert SOL ou token et ne prouve pas "
            "a elle seule une action Pump.fun/PumpSwap.",
            "- `pool=pump-amm` est une evidence de contexte, mais l'appartenance definitive "
            "Pump.fun/PumpSwap doit etre validee avec plus d'exemples et une specification source.",
            "- Les valeurs de `txType` autres que celles listees et les structures minoritaires "
            "restent a conserver en UNKNOWN jusqu'a validation.",
        ]
    )
    OUTPUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(OUTPUT)


if __name__ == "__main__":
    main()
