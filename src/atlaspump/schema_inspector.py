"""Schema observation without presuming a provider contract."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any


def _walk(payload: dict[str, Any], prefix: str = "") -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = []
    for key, value in payload.items():
        path = f"{prefix}.{key}" if prefix else key
        found.append((path, value))
        if isinstance(value, dict):
            found.extend(_walk(value, path))
    return found


@dataclass
class SchemaInspector:
    examples_per_field: int = 3
    events: int = 0
    fields: Counter[str] = field(default_factory=Counter)
    types: dict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    examples: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    event_values: dict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))

    def observe(self, payload: dict[str, Any], event_field_candidates: list[str]) -> None:
        self.events += 1
        for path, value in _walk(payload):
            self.fields[path] += 1
            self.types[path][type(value).__name__] += 1
            rendered = repr(value)[:180]
            if (
                len(self.examples[path]) < self.examples_per_field
                and rendered not in self.examples[path]
            ):
                self.examples[path].append(rendered)
        for candidate in event_field_candidates:
            value = payload.get(candidate)
            if value is not None:
                self.event_values[candidate][str(value)[:100]] += 1

    def summary(self) -> dict[str, Any]:
        return {
            "events_observed": self.events,
            "fields": {
                field: {
                    "present": count,
                    "frequency": count / self.events if self.events else 0,
                    "types": dict(self.types[field]),
                    "examples": self.examples[field],
                }
                for field, count in sorted(self.fields.items())
            },
            "event_type_candidates": {
                field: dict(values) for field, values in self.event_values.items()
            },
        }
