# Vue d'ensemble de l'architecture

> Synthèse de [RFC-002](../rfc/RFC-002-architecture-globale.md), au statut `DRAFT`.

```mermaid
flowchart LR
  Sources --> Discovery --> Detail --> Raw[Cerebro raw local] --> Normalized --> Quality --> Lifecycles --> Features --> Packs
  Discovery --> Neon[Neon : checkpoints et catalogue]
  Detail --> Neon
  Raw --> R2[R2 : archive distante]
  Packs --> Mac[ML sur Mac]
  Packs --> Windows[DL sur Windows]
  Mac --> Registry[Registre]
  Windows --> Registry
  Registry --> Cerebro[Inférence Cerebro]
```

Cerebro est le nœud de données : collecte, replay, traitements, DuckDB, packs et opérations sous `ATLAS_DATA_DIR`. Neon catalogue checkpoints, états de run, index et manifestes sans stocker l'historique massif ; R2 archive les publications validées et permet la restauration. Le raw n'est jamais modifié ; les couches dérivées sont liées par manifestes, versions et hashes. Batch et micro-batch sont privilégiés avant toute infrastructure de streaming complexe.
