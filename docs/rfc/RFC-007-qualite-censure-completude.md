# RFC-007 — Qualité, censure et complétude

| Champ | Valeur |
| --- | --- |
| Numéro | RFC-007 |
| Titre | Qualité, censure et complétude |
| Statut | DRAFT |
| Auteur | Équipe AtlasPump |
| Date | 2026-07-17 |
| Version | 0.1 |

## Résumé

Cette RFC définit le Quality Layer d'AtlasPump et la manière dont la qualité,
la couverture, la censure et la complétude sont mesurées et propagées. Elle
fixe des contrôles, des statuts, des sévérités et des règles de décision pour
les couches dérivées, sans transformer une absence d'observation en échec
économique.

Elle complète RFC-003 et RFC-006. Elle ne définit ni les formules de features,
ni les labels métier définitifs, ni les algorithmes du Lifecycle Engine.

## Contexte

Les sources peuvent fournir des événements invalides, inconnus, dupliqués,
tardifs ou partiels. Une fenêtre peut commencer après la création réelle d'un
token ou finir avant sa migration. Un lifecycle complet pour une politique de
fenêtre ne signifie donc pas que la vie économique complète est connue.

La qualité doit rester un attribut mesuré et versionné. Les événements bruts et
les événements normalisés ne sont jamais supprimés parce qu'ils échouent à un
contrôle aval.

## Problème

Un simple booléen valide/invalide mélange des dimensions différentes : format,
identité, couverture, ordre, cohérence, complétude de fenêtre et utilisabilité
pour un cas d'usage. Cette confusion crée des suppressions silencieuses, des
labels biaisés et des erreurs de survivant.

## Objectifs

1. Mesurer qualité, couverture, censure et complétude séparément.
2. Conserver les faits et anomalies avec leur provenance.
3. Définir des statuts et sévérités utilisables par les couches aval.
4. Propager les limitations aux lifecycles, features, labels et datasets.
5. Distinguer absence, inconnu, invalide, non-observé et censuré.
6. Rendre les contrôles rejouables, idempotents et versionnés.
7. Produire des rapports et manifests auditables.
8. Empêcher qu'une donnée censurée soit assimilée automatiquement à un échec.

## Non-objectifs

- supprimer ou corriger le raw ;
- inventer un événement absent ;
- déduire la mort d'un token d'un silence non couvert ;
- fixer les seuils définitifs de migration inférée ;
- définir un label universel success/failure ;
- remplacer les décisions du Lifecycle Engine ;
- rendre toutes les données PARTIAL inutilisables par défaut ;
- créer une base de qualité séparée obligatoire.

## Proposition

### Dimensions distinctes

La qualité est multidimensionnelle. Une seule enum ne doit pas mélanger qualité,
censure, activité et complétude.

| Dimension | Valeurs proposées | Question |
| --- | --- | --- |
| format_status | VALID, INVALID, UNKNOWN | Le payload est-il exploitable syntaxiquement ? |
| identity_status | UNIQUE, DUPLICATE, COLLISION | L'identité est-elle stable et non contradictoire ? |
| temporal_status | ORDERED, LATE, OUT_OF_ORDER, UNKNOWN | L'ordre temporel est-il fiable ? |
| coverage_status | COMPLETE, PARTIAL, MISSING | La source couvre-t-elle la fenêtre déclarée ? |
| quality_status | VALID, PARTIAL, INVALID | La sortie est-elle utilisable selon la politique ? |
| censoring_status | NONE, LEFT, RIGHT, BOTH | Quelles bornes de connaissance manquent ? |
| completeness_status | COMPLETE, PARTIAL, INCOMPLETE | Le contrat de fenêtre est-il satisfait ? |

Les valeurs exactes peuvent évoluer avec le schéma, mais leur séparation est
obligatoire. Une sortie peut être PARTIAL et néanmoins utilisable pour une
analyse descriptive, tout en étant exclue d'un label de survie.

### Niveaux de contrôle

Les contrôles sont exécutés par couche :

| Niveau | Contrôles principaux |
| --- | --- |
| Capture | fichier lisible, hash, manifeste, partition et comptages |
| SourceEvent | payload, provenance, identifiant, timestamp, doublons |
| CanonicalEvent | type, unités, montants, références raw, ordre possible |
| Entité | clés, relations token/wallet/pool, cohérence inter-événements |
| Lifecycle | transitions, fenêtres, censure, anomalies, complétude |
| Dataset | cutoff, labels, splits, fuite temporelle, population |
| Pack | manifest, schéma, hashes, statistiques, transfert |

Chaque contrôle produit une observation de qualité, une règle versionnée, une
sévérité et une référence aux lignes, événements, fichiers ou manifests
concernés.

### Sévérités

| Sévérité | Signification | Effet par défaut |
| --- | --- | --- |
| INFO | constat sans limitation importante | publier |
| WARNING | limitation explicite mais sortie potentiellement exploitable | publier avec statut |
| ERROR | sortie dégradée ou couverture insuffisante | exclure de certains usages |
| BLOCKING | contradiction ou intégrité compromise | ne pas publier comme complète |

Une sévérité ne supprime jamais l'entrée. La politique du dataset décide si un
statut PARTIAL ou une anomalie WARNING est acceptable pour son usage.

### Contrôles obligatoires

Les contrôles minimaux comprennent :

- JSON illisible ou payload vide ;
- champs obligatoires absents ou de type inattendu ;
- hash ou manifeste manquant ;
- doublon identique et collision d'identité divergente ;
- timestamp invalide, futur ou hors fenêtre ;
- valeurs négatives, non finies ou unités incompatibles ;
- token, wallet ou pool mal référencé ;
- événement hors ordre ou événement tardif ;
- partition source manquante ou fenêtre incomplète ;
- création multiple, transition impossible ou migration contradictoire ;
- UNKNOWN et INVALID_JSON ;
- fuite de données au-delà de observation_cutoff ;
- split non temporel ou chevauchement train/test.

Les contrôles doivent conserver le fait observé et publier l'anomalie à côté.

### Censure

La censure est déterminée par la fenêtre effectivement couverte et par le
contrat de suivi, jamais par une supposition économique.

| Situation | Statut minimal |
| --- | --- |
| Activité observée avant le début de la fenêtre | LEFT_CENSORED |
| Token encore observable à la fin de la fenêtre | RIGHT_CENSORED |
| Les deux bornes sont hors couverture | BOTH_CENSORED |
| Fenêtre et suivi satisfaits | NONE, sous réserve de qualité |
| Partition requise manquante | coverage PARTIAL ou MISSING |

LEFT_CENSORED ne signifie pas que la création est inconnue dans l'histoire,
seulement qu'elle n'est pas observée dans cette fenêtre. RIGHT_CENSORED ne
signifie pas que le token a échoué ou qu'il est encore économiquement actif.

### Complétude

La complétude est relative à un contrat déclaré : source, fenêtre, cohorte,
suivi, période de grâce et qualité minimale. Un manifest doit identifier ce
contrat.

Un lifecycle peut être COMPLETE pour une fenêtre d'une heure et PARTIAL pour
une fenêtre de suivi de 60 minutes. Une création seule ne suffit pas à
conclure à un succès ou à un échec.

La politique de complétude indique au minimum :

- observation_start et observation_end ;
- source et partitions attendues ;
- cohorte et critères d'inclusion ;
- fenêtre de suivi et grâce ;
- événements requis ;
- seuils de qualité ;
- traitement de la censure ;
- version de la politique.

### Propagation aval

Les couches aval reçoivent les statuts et leurs raisons :

| Consommateur | Règle |
| --- | --- |
| Lifecycle | conserve censure, couverture, anomalies et qualité |
| Feature Lab | associe cutoff, statut et raison aux features |
| Label | porte censoring_status et label_policy_version |
| Dataset | définit explicitement filtres et tolérances |
| Backtest | exclut toute observation indisponible au cutoff |
| Rapport | affiche les limites au lieu de les masquer |

Une politique de dataset peut exclure INVALID ou certaines PARTIAL, mais elle
doit compter les exclus, conserver le manifest et ne pas modifier l'entrée.

### Rapports et manifests

Chaque run de qualité publie un manifest et un rapport contenant au minimum :

    quality_run_id
    input_manifest_ids
    quality_policy_version
    rule_versions
    row_counts_by_status
    anomaly_counts_by_severity
    coverage_by_partition
    censoring_counts
    completeness_counts
    excluded_counts_by_reason
    output_files
    output_hashes
    status

Les taux ne remplacent pas les comptages. Les rapports doivent préciser le
dénominateur, la fenêtre et la population concernés.

### Règles de décision

Une publication COMPLETE exige intégrité des fichiers, couverture déclarée,
absence d'anomalie BLOCKING et respect des seuils de la politique. Une
publication PARTIAL est possible si ses limitations sont quantifiées. Une
publication INVALID indique une contradiction ou une corruption qui empêche
un usage fiable, sans supprimer la donnée originale.

Les seuils numériques sont propres à la couche et à l'usage. Ils sont versionnés
et ne doivent pas être cachés dans le code ou un notebook.

## Modèle de données concerné

RFC-003 reste propriétaire de quality_status, censoring_status, lifecycle_status,
lifecycle_complete, des nulls et des manifests. RFC-006 reste propriétaire de
la reconstruction et de ses anomalies. RFC-007 ajoute les contrats de contrôle,
les règles et les rapports ; elle ne remplace aucun fait observé.

## Impacts opérationnels

- Les contrôles sont rejouables depuis les manifests d'entrée.
- Un run de qualité est idempotent à version, entrée et configuration égales.
- Les données brutes et les sorties précédentes restent disponibles.
- Les rapports doivent être consultables par run, fenêtre, couche et règle.
- Un échec BLOCKING bloque la publication complète, pas la conservation du raw.

## Impacts sécurité

Les rapports ne doivent pas exposer de secrets. Les contrôles d'intégrité et de
hash détectent les modifications non autorisées. Les permissions séparent la
lecture du raw, l'exécution des contrôles et la publication dérivée.

## Impacts performance

Les contrôles doivent fonctionner en streaming ou par partition lorsque possible.
Les contrôles coûteux peuvent produire une table d'anomalies dédiée, mais leur
version, couverture et coût sont manifestés. Un contrôle rapide ne doit pas
remplacer un contrôle exhaustif sans le signaler.

## Compatibilité et migration

Une nouvelle règle ou un changement de seuil produit une nouvelle
quality_policy_version et une nouvelle publication. Les anciens rapports et
statuts restent lisibles. Une migration recalcule les sorties depuis les
entrées, sans mutation silencieuse.

## Observabilité

Les métriques minimales sont lignes contrôlées, taux VALID/PARTIAL/INVALID,
UNKNOWN, doublons, collisions, erreurs par règle, anomalies par sévérité,
partitions manquantes, taux de censure, complétude par cohorte, durée, mémoire,
reprises, manifests publiés et exclusions par motif.

## Tests

L'implémentation devra démontrer :

1. conservation des entrées invalides et UNKNOWN ;
2. distinction doublon identique/collision divergente ;
3. calcul correct de LEFT, RIGHT et BOTH censored ;
4. absence de succès ou d'échec déduit d'un silence ;
5. complétude relative à une fenêtre explicitement déclarée ;
6. propagation des statuts vers lifecycle, labels et datasets ;
7. blocage d'une fuite au-delà du cutoff ;
8. production de rapports avec dénominateurs et manifests ;
9. rejeu idempotent d'un run de qualité ;
10. révision d'une politique sans mutation des anciennes sorties.

## Critères d'acceptation

La RFC pourra être ACCEPTED lorsque les dimensions qualité/censure/complétude,
les contrôles, sévérités, rapports, règles de propagation et décisions de
publication seront cohérents avec RFC-003, RFC-004 et RFC-006 ; lorsque la
censure ne sera jamais assimilée par défaut à un échec ; et lorsque les règles
seront testables et versionnées.

Aucune implémentation structurante du Quality Layer ne démarre avant ACCEPTED.

## Questions ouvertes

- Quels seuils de couverture rendent une cohorte exploitable par type de label ?
- Quelle période de grâce retenir par fournisseur et protocole ?
- Quelles anomalies doivent être BLOCKING par couche ?
- Faut-il conserver une table de contrôles au niveau événement et au niveau
  fichier, ou seulement le niveau le plus détaillé utile ?
- Quelle politique accepter pour les événements tardifs après publication ?
- Comment calibrer les taux d'erreur sans confondre absence de source et
  absence d'activité économique ?

## Décision finale

En attente de revue. Cette RFC reste DRAFT et n'autorise aucune suppression,
correction silencieuse ou implémentation structurante du Quality Layer.

## Historique des modifications

| Date | Version | Modification | Auteur |
| --- | --- | --- | --- |
| 2026-07-17 | 0.1 | Création du brouillon | Équipe AtlasPump |
