# Vue d'ensemble de l'architecture

> Synthèse de [RFC-002](../rfc/RFC-002-architecture-globale.md), au statut `DRAFT`.

```mermaid
flowchart LR
  Sources --> Raw --> Normalized --> Quality --> Lifecycles --> Features --> Packs
  Packs --> Mac[ML sur Mac]
  Packs --> Windows[DL sur Windows]
  Mac --> Registry[Registre]
  Windows --> Registry
  Registry --> Cerebro[Inférence Cerebro]
```

Cerebro est le nœud de données : collecte, replay, traitements, packs et opérations. Mac et Windows entraînent à partir de packs immuables, versionnés et vérifiés. Le raw n'est jamais modifié ; les couches dérivées sont liées par manifestes, versions et hashes. Batch et micro-batch sont privilégiés avant toute infrastructure de streaming complexe.
