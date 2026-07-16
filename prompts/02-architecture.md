# Architecture globale — RFC-002

## Objectif

Définir composants, contrats, flux, frontières batch/live, répartition des machines, reprise, versionnement et observabilité sans imposer de framework.

## Entrées nécessaires

- RFC-001, preuves de faisabilité et contraintes Cerebro/Mac/Windows.
- Exigence de portabilité des packs via `ATLAS_DATA_DIR`.

## Livrables attendus

- RFC-002 au statut `DRAFT`, index et vue d'architecture mis à jour.

## Critères d'acceptation

- Responsabilités, flux, manifestes, alternatives et questions ouvertes sont définis.

## Interdictions

- Ne pas écrire de code, service, collecteur, pipeline ou modèle.
- Ne pas imposer Kafka, Redis, RDS ou un cloud sans RFC acceptée.

**À compléter avant exécution.**
