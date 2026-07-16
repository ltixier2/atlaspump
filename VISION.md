# Vision AtlasPump — DRAFT

> Document de travail. Il ne constitue pas encore une décision d'architecture.

## Objectifs

- Construire une base de données rejouable des événements Pump.fun et PumpSwap.
- Distinguer faits observés, données dérivées et inférences de modèles.
- Produire des datasets versionnés pour la recherche, l'évaluation et
  l'inférence.
- Comparer des baselines simples avant d'adopter des modèles complexes.

## Non-objectifs

- Déployer immédiatement un système de trading réel.
- Considérer une donnée incomplète ou censurée comme un échec observé.
- Coupler les données, les modèles et les chemins de stockage à une machine.

## Principes

- Les données brutes sont immuables.
- Toute transformation est déterministe, traçable et rejouable.
- Toute donnée dérivée est versionnée.
- Les modèles sont interchangeables et évalués contre une baseline.
- Les décisions structurantes passent par RFC.

## Utilisateurs du système

- Chercheurs et développeurs de modèles.
- Opérateurs de collecte et de préparation des données.
- Opérateurs de backtesting, paper trading et, à terme, trading.

## Questions de recherche

- Quels signaux précoces décrivent les trajectoires des tokens ?
- Comment représenter la temporalité, les relations et la censure sans fuite
  d'information ?
- Quels modèles améliorent réellement des baselines tabulaires simples ?

## Critères de réussite

- Un dataset peut être recréé à partir de son manifeste et de ses sources.
- Les mesures hors échantillon respectent la disponibilité temporelle réelle.
- Les résultats de backtest sont auditables et reproductibles.

## Risques

- Données API incomplètes, tardives ou sémantiquement ambiguës.
- Fuite temporelle et biais de sélection dans les labels et datasets.
- Coût de stockage, transfert et entraînement entre machines hétérogènes.
- Risque opérationnel si recherche et exécution sont insuffisamment séparées.

## Décisions encore ouvertes

- Contrat canonique des événements et politique d'évolution de schéma.
- Format des manifests et stratégie de versionnement des datasets.
- Partitionnement, rétention et orchestration sur Cerebro.
- Définition des labels, de la censure et des règles de backtest.
