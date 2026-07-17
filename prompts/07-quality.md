# Qualité, censure et complétude — RFC-007

## Objectif

Définir les contrôles de qualité, la couverture, la censure, la complétude,
les sévérités, les rapports et la propagation vers les couches aval dans
RFC-007.

## Entrées nécessaires

- RFC-003, RFC-004, RFC-006, contrats de données et règles lifecycle.

## Livrables attendus

- RFC-007 au statut DRAFT et index mis à jour.

## Critères d'acceptation

- Qualité, censure, complétude, sévérités, couverture, propagation et rapports
  sont mesurables et versionnés.

## Interdictions

- Ne pas assimiler une donnée censurée à un échec.

Ne jamais assimiler une donnée censurée, absente ou partielle à un échec sans
politique explicite. Ne pas supprimer ni corriger silencieusement le raw.
