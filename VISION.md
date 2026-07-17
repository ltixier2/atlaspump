# Vision AtlasPump — DRAFT

AtlasPump est une plateforme de recherche sur Pump.fun et PumpSwap. Elle
construit un patrimoine de données historiques et temps réel pour étudier les
lifecycles de tokens, les créateurs, les wallets et leurs relations, puis
évaluer des modèles et des stratégies de manière reproductible.

La référence est [RFC-001 — Vision et périmètre](docs/rfc/RFC-001-vision-et-perimetre.md).
Ce document en est une synthèse lisible ; la RFC reste `DRAFT`.

## Priorité

Les données sont l'actif principal : raw immuable, transformations rejouables,
dérivés versionnés et lineage par manifestes. Les faits observés, données
dérivées et inférences doivent rester distincts.

## Périmètre

- Collecte et replay, normalisation, quality control et reconstruction de
  lifecycles.
- Datasets versionnés pour analyses tabulaires, temporelles et relationnelles.
- Baselines, modèles ML/DL, backtests réalistes et préparation de l'inférence.
- Paper trading et exécution réelle uniquement comme étapes futures soumises à
  RFC et contrôles spécifiques.

## Principes de décision

- Une baseline simple précède tout modèle complexe.
- Les données censurées ne sont pas assimilées à des échecs.
- Une métrique de modèle ne suffit pas : la robustesse temporelle et les
  frictions de backtest comptent.
- Les fournisseurs, machines et chemins ne doivent pas créer de verrouillage.
- Les données volumineuses restent hors Git ; `ATLAS_DATA_DIR` configure les
  emplacements physiques.
- Aucune brique structurante n'est implémentée avant l'acceptation de sa RFC.

## Mesure du succès

Le projet doit démontrer la couverture et la qualité de ses données, la
reproductibilité de ses datasets, une valeur robuste face aux baselines, et des
résultats simulés après frais, latence, liquidité et slippage. Il ne promet
aucun rendement financier.

## Décisions ouvertes

La rétention raw, la profondeur historique, les labels de succès, la politique
de censure, la dépendance à PumpApi, le passage au live, l'introduction du
graphe et les seuils avant paper trading restent à trancher dans les RFC
suivantes.
