# Stockage et partitionnement — RFC-005

## Objectif

Définir les couches, formats, chemins, partitions, publications atomiques,
compaction, manifests, rétention et transfert des packs dans RFC-005.

## Entrées nécessaires

- RFC-002, RFC-003, RFC-004, volumes observés et contraintes de Cerebro.

## Livrables attendus

- RFC-005 au statut DRAFT et index mis à jour.

## Critères d'acceptation

- Couches, partitionnement, formats, manifests, compaction, rétention,
  restauration, transfert et observabilité sont définis.

## Interdictions

- Ne pas créer de base ou de données.

Ne pas créer de base, déplacer les données existantes ni lancer de migration
volumineuse. Les seuils chiffrés doivent rester à calibrer par benchmark et la
politique destructive de rétention doit attendre une décision explicite.
