# Token Lifecycle Engine — RFC-006

## Objectif

Définir la reconstruction versionnée des lifecycles : fenêtres, états multidimensionnels, censure, faits, inférences, anomalies et sorties, sans choisir d'algorithme.

## Entrées nécessaires

- RFC-003, événements canoniques, couverture source et exigences de provenance.

## Livrables attendus

- RFC-006 au statut `DRAFT`, index, synthèse data et glossaire mis à jour.

## Critères d'acceptation

- Dimensions d'état, transitions, censure, événements tardifs, anomalies et versionnement sont définis.

## Interdictions

- Ne pas développer le builder ni déduire un fait depuis une absence d'événement.
- Ne pas mélanger qualité, censure, complétude et activité dans une seule enum.

**À compléter avant exécution.**
