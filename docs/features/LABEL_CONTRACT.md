# Contrat des labels RFC-008

Les labels sont séparés des tables de features et ne sont pas matérialisés dans
ce jalon.

## `label_migrated`

- `1` : migration observée;
- `0` : aucune migration observée **et** fenêtre de suivi compatible complète;
- `null` : suivi insuffisant, censure incompatible ou vérité indéterminable.

Un token censuré n'est jamais converti silencieusement en faux négatif.

## Labels temporels proposés

`migrated_within_5m`, `migrated_within_15m`, `migrated_within_60m`,
`migrated_within_6h` et `migrated_within_24h` devront chacun préciser horizon,
durée minimale de suivi, traitement de censure, exclusions et instant de vérité
avant toute matérialisation RFC-009.

## Sémantique RFC-009 des horizons (contrat v2)

`label_horizon_seconds` n'est pas un champ numérique universel. Il est interprété
avec `label_horizon_type`, qui est obligatoire et non nul dans toute future
cohorte.

| Label | Type | `label_horizon_seconds` | Suivi minimum | Censure |
| --- | --- | ---: | ---: | --- |
| `migration_observed_rfc_v1` | `OBSERVATION_PERIOD` | `null` | `null` (période réellement observée) | gauche/droite : indéterminé, exclu |
| `migration_within_5m_rfc_v1` | `FIXED_WINDOW` | 300 | 300 | suivi inférieur ou censure incompatible : exclu |
| `migration_within_15m_rfc_v1` | `FIXED_WINDOW` | 900 | 900 | suivi inférieur ou censure incompatible : exclu |
| `migration_within_60m_rfc_v1` | `FIXED_WINDOW` | 3600 | 3600 | suivi inférieur ou censure incompatible : exclu |

Pour `migration_observed_rfc_v1`, `null` signifie **non applicable** : la vérité
porte sur l'événement observé pendant la période effectivement disponible du
corpus, et non « dans 0 seconde » ou une durée implicite. Un négatif reste
interdit lorsqu'une censure rend cette période indéterminable. Un horizon nul
sans type explicite, ou pour un label `FIXED_WINDOW`, est invalide.
