# Collecte live et replay — RFC-004

## Objectif

Définir le contrat de collecte historique et live, le raw, la provenance, les
checkpoints, la reprise, l'idempotence et les manifestes, sans implémenter de
collecteur.

## Entrées nécessaires

- RFC-002, RFC-003, capacités observées des sources et contraintes Cerebro.

## Livrables attendus

- RFC-004 au statut DRAFT et index mis à jour.

## Critères d'acceptation

- Idempotence, reprise, recouvrement, couverture, conservation du brut,
  provenance et états de manifeste sont définis.

## Interdictions

- Ne pas implémenter de collecteur.

La RFC sépare adaptateur, enveloppe de capture, raw, normalisation, checkpoint
et monitoring. Elle ne choisit pas Kafka/Redis et ne définit pas le
partitionnement détaillé de RFC-005.
