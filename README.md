# AtlasPump

AtlasPump est une plateforme de recherche consacrée à l'écosystème Pump.fun et
PumpSwap. Elle vise à produire des données fiables et reproductibles pour la
reconstruction de cycles de vie de tokens, l'analyse et l'apprentissage
automatique.

## Vision

Les données sont l'actif principal du projet. Chaque fait observé doit rester
traçable vers sa source, et chaque donnée dérivée doit pouvoir être rejouée et
versionnée.

## Pipeline cible

```text
Événements Pump.fun / PumpSwap
  -> collecte et replay historique
  -> normalisation Parquet
  -> reconstruction des lifecycles
  -> contrôle qualité
  -> Feature Lab
  -> Dataset Factory versionnée
  -> modèles et évaluation
  -> backtests réalistes
  -> inférence, paper trading, puis trading éventuel
```

## Statut

Le projet est en **phase de conception**. Aucune fonctionnalité métier, aucun
collecteur, aucun pipeline exécutable ni modèle ne sont encore implémentés.

## RFC

Les décisions structurantes sont définies et revues dans les
[RFC](docs/rfc/README.md). Une fonctionnalité ne peut être implémentée qu'après
l'acceptation de sa RFC structurante.

## Données

Les données volumineuses, modèles, checkpoints et bases locales ne doivent
jamais être stockés dans Git. Les chemins physiques sont configurables avec
`ATLAS_DATA_DIR` ; voir [.env.example](.env.example).
