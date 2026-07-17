# RFC-005 — Stockage et partitionnement

| Champ | Valeur |
| --- | --- |
| Numéro | RFC-005 |
| Titre | Stockage et partitionnement |
| Statut | DRAFT |
| Auteur | Équipe AtlasPump |
| Date | 2026-07-17 |
| Version | 0.2 |

## Résumé

Cette RFC définit l'organisation physique et logique des données AtlasPump sur
Cerebro. Elle précise les couches de stockage, les formats, les conventions de
chemins, le partitionnement, les écritures atomiques, la compaction, les
manifestes, la rétention et l'usage de DuckDB.

Elle ne crée pas de base, ne déplace pas les données existantes et ne fixe pas
les schémas métier déjà définis par RFC-003. Les choix restent compatibles
avec le raw immuable de RFC-004 et avec les packs versionnés destinés aux autres
machines.

## Contexte

AtlasPump traite des archives horaires volumineuses et produit des couches
successives : raw, normalized, curated, lifecycles, features, datasets, packs,
rapports et manifestes. Parquet avec compression Zstandard est le format
analytique principal ; DuckDB est le moteur local de lecture, d'audit et de
traitement.

Cerebro dispose du volume dédié /mnt/atlaspump. Ce chemin est une convention
locale et doit rester configurable par ATLAS_DATA_DIR. Les données volumineuses
ne sont pas versionnées dans Git. Cloudflare R2 est la sauvegarde distante durable des publications et Neon PostgreSQL en indexe les métadonnées et états, sans héberger les lignes événementielles massives.

## Problème

Sans conventions partagées, les jobs créent des chemins incompatibles, trop de
petits fichiers, des partitions difficiles à relire ou des sorties impossibles
à relier à leurs entrées. Une politique de rétention trop agressive détruit la
rejouabilité ; une rétention illimitée rend l'exploitation fragile.

## Objectifs

1. Organiser les couches par responsabilité et cycle de vie.
2. Utiliser JSONL.ZST pour le raw et Parquet/Zstandard pour l'analytique.
3. Définir un partitionnement stable, temporel et limité en cardinalité.
4. Garantir des publications atomiques et lisibles par manifeste.
5. Éviter les petits fichiers et rendre la compaction rejouable.
6. Séparer données publiées, temporaires, caches, rapports et logs.
7. Définir une rétention par couche avec exceptions d'audit.
8. Permettre les lectures locales DuckDB sans base centrale.
9. Rendre les packs transférables avec hashes et chemins relatifs.
10. Surveiller capacité, intégrité, âge et croissance du stockage.

## Non-objectifs

- créer ou déployer une base de données centrale ;
- introduire un data warehouse, un lakehouse ou un système distribué ;
- modifier le raw en place ou le convertir comme unique archive ;
- définir les champs canoniques de RFC-003 ;
- définir les règles de qualité ou de lifecycle de RFC-006 et RFC-007 ;
- stocker datasets volumineux ou modèles dans Git ;
- optimiser une requête particulière au détriment de la lisibilité des couches.

## Proposition

### Racine et arborescence

La racine logique est ATLAS_DATA_DIR. L'organisation proposée est :

    raw/
    normalized/
    curated/
    daily/
    lifecycles/
    quality/
    features/
    datasets/
    graph/
    sequences/
    packs/
    models/
    backtests/
    manifests/
    reports/
    logs/
    cache/
    tmp/

Les chemins absolus ne figurent ni dans les manifests portables ni dans les
datasets. Un manifest référence des chemins relatifs à ATLAS_DATA_DIR, des clés objet R2 et des hashes.

### Responsabilité des couches

| Couche | Format principal | Propriété |
| --- | --- | --- |
| raw | JSONL.ZST | immuable, append-only, provenance source |
| normalized | Parquet/Zstandard | événements canoniques, rejets, version de normalisation |
| curated | Parquet/Zstandard | qualité, entités et vues dérivées publiées |
| lifecycles | Parquet/Zstandard | sorties versionnées du Lifecycle Engine |
| quality | Parquet, JSON, Markdown | contrôles, anomalies et rapports |
| features | Parquet | features versionnées et cutoff explicite |
| datasets | Parquet et formats spécialisés | cohortes, labels, splits et versions |
| packs | Parquet et formats graphes/séries | frontière data/entraînement |
| manifests | JSON | provenance, hashes, comptages, statut |
| reports | Markdown/JSON | audit humain et synthèses |
| cache/tmp | format local | supprimable, jamais source canonique |

Le stockage local de travail (`tmp`, `cache`, journaux et files durables) est distinct de la publication locale finalisée. R2 conserve l'archive/sauvegarde distante des raw et Parquet publiés. Neon indexe publications, objets, hashes, versions, états et dépendances ; il ne stocke pas les lignes événementielles massives.

Une couche dérivée ne remplace jamais son entrée. Les rejets et UNKNOWN sont
conservés ou référencés selon la politique de la couche ; ils ne sont pas
effacés parce qu'ils ne servent pas à un dataset donné.

### Partitionnement

Le partitionnement doit suivre les dimensions nécessaires aux lectures
habituelles sans créer une partition par token, wallet ou événement.

| Couche | Partition recommandée |
| --- | --- |
| raw | fournisseur, mode, date, heure, partition source |
| normalized | date blockchain ou réception, protocole si stable |
| curated | version de politique, date d'observation ou de publication |
| lifecycles | lifecycle_version, date de cohorte ou d'observation |
| quality | date de run, type de contrôle |
| features | feature_set_version, date de cutoff |
| datasets | dataset_version, split ou date de cohorte |
| packs | pack_version, type de pack |
| manifests | manifest_type, date de création |

Les partitions temporelles sont en UTC et utilisent une granularité cohérente
avec le volume. Une partition doit rester raisonnablement lisible en mémoire
et ne doit pas être subdivisée par une dimension à forte cardinalité sans
mesure préalable.

Les clés de partition ne sont pas nécessairement des colonnes métier : elles
doivent toutefois être présentes dans les métadonnées du fichier et le
manifest doit décrire la fenêtre couverte.

### Fichiers et compaction

Les producteurs écrivent un fichier temporaire dans la destination, le
ferment, le valident, calculent sa taille et son hash, puis le renomment
atomiquement. Un fichier finalisé est immutable.

La compaction regroupe des fichiers compatibles de même couche, schéma,
partition et politique. Elle produit une nouvelle publication versionnée et un
manifest qui référence les anciens fichiers ; elle ne modifie pas le raw et ne
supprime pas une publication sans respecter sa rétention.

Les seuils exacts de taille, le nombre de fichiers et la cadence de compaction
seront calibrés par benchmark. Le système doit néanmoins détecter les petits
fichiers, les fichiers orphelins et les partitions déséquilibrées avant de
publier une nouvelle version.

### Publication, réplication et manifests

L'état de publication est indépendant des statuts de qualité RFC-007 : `LOCAL_PROVISIONAL`, `LOCAL_COMPLETE`, `REMOTE_PENDING`, `REMOTE_VERIFIED`, `REMOTE_FAILED`. Une sortie est localement publiée quand son manifeste local est valide et son état `LOCAL_COMPLETE` ou `REMOTE_PENDING`; une couverture `PARTIAL` ou une utilisabilité limitée restent des attributs séparés. Le manifest doit
référencer :

    manifest_id
    manifest_type
    layer
    relative_input_files
    input_hashes
    relative_output_files
    output_hashes
    row_counts
    byte_counts
    covered_window
    partition_spec
    schema_version
    normalization_version
    quality_policy_version
    lifecycle_version
    feature_set_version
    dataset_version
    split_version
    code_commit
    environment_fingerprint
    publication_status
    r2_object_keys
    remote_verification

Les manifests sont eux-mêmes append-only et peuvent être indexés par date et type dans Neon. Les trous et limites d'usage sont portés par `coverage_status`, `contract_status` et `usability_status`, pas par l'état de publication. Un fichier sans manifest correspondant n'est pas une publication analytique fiable.

Pour toute publication durable, le workflow est : (1) écriture temporaire locale ; (2) validation ; (3) hash et comptages ; (4) renommage atomique ; (5) création du manifeste local ; (6) indexation dans Neon ; (7) upload asynchrone vers R2 ; (8) vérification distante du hash ou de l'intégrité ; (9) mise à jour de l'état de réplication. Un échec R2 ne rend pas invalide une publication locale valide : il devient `REMOTE_FAILED`, est visible et retenté. Si Neon est indisponible, l'indexation est journalisée localement et réconciliée ultérieurement.

### DuckDB

DuckDB est utilisé comme moteur local pour les audits, jointures, tris,
compactions, vues et contrôles de comptage. Les bases DuckDB temporaires ou
de travail peuvent être recréées depuis Parquet et ne sont pas la source
canonique.

Les requêtes doivent préférer les chemins relatifs configurés, les scans
partitionnés et les opérations déterministes. Une vue matérialisée n'est
publiée que comme couche dérivée avec version et manifest ; elle ne doit pas
introduire une divergence silencieuse avec les fichiers Parquet.

### Rétention

La rétention initiale est une politique par couche, configurable et manifestée :

| Couche | Politique initiale |
| --- | --- |
| raw | conservation longue durée ; suppression seulement après décision explicite |
| normalized | conservation tant que les raw et versions de normalisation sont disponibles |
| curated/lifecycles | conservation des publications et versions utilisées par les datasets |
| quality/reports/manifests | conservation longue durée pour audit et provenance |
| features/datasets/packs | conservation des versions publiées et consommées |
| cache/tmp/logs | rotation et suppression opérationnelle contrôlée |

Cette RFC ne fixe pas un nombre de jours sans mesure du volume, du coût et de la capacité de restauration. La politique de sauvegarde locale et distante est versionnée et mesurée avant de fixer les durées. Toute suppression doit être précédée d'un rapport listant les fichiers, hashes, manifests dépendants et impact sur la rejouabilité ; elle exige une politique explicite, une vérification distante et un contrôle des dépendances. Le raw et les manifests de référence bénéficient d'une protection renforcée.

Une restauration depuis R2 reconstruit les chemins relatifs sous `ATLAS_DATA_DIR`, vérifie hashes et manifeste, puis réindexe ou réconcilie Neon ; elle ne repose jamais sur un chemin absolu ancien.

### Politique C v1 : quarantaine non destructive

La première application est strictement planificatrice : `--dry-run` est actif par défaut et aucune suppression physique n'est autorisée. Elle conserve intégralement lifecycles, outcomes et anomalies, tous les événements des tokens migrés ou anormaux, et un échantillon déterministe de 3 % des tokens ordinaires calculé par hash stable de `token_mint`. Raw et normalized restent `HOLD` dans une fenêtre glissante de sept jours; hors fenêtre ils deviennent seulement `DELETE_CANDIDATE`. Les événements sans mint forment une catégorie séparée `HOLD` dans cette version.

Les états sont `DISCOVERED`, `HOLD`, `DELETE_CANDIDATE`, `DELETE_APPROVED`, `DELETED` et `FAILED`. Une transition directe de `DISCOVERED` à `DELETED` est interdite. Chaque manifest de rétention contient chemin relatif, taille, date logique, catégorie, justification, état et checksum pré-action. Une future action destructive exigera `DELETE_APPROVED`, dépendances vérifiées, journal et contrôle de restauration.

### Transfert des packs

Un pack est un répertoire logique versionné contenant données, schéma, splits,
manifest, statistiques et hashes. Son manifest ne dépend pas du chemin
physique. Le transfert vers Mac ou Windows se fait par copie vérifiée ; la
machine destinataire valide les hashes et le manifest avant entraînement.

Un pack incomplet, dont le hash diffère ou dont le manifest est absent reste
non publiable. Les modèles et résultats d'entraînement référencent la version
du pack et ne deviennent pas une source de données.

## Modèle de données concerné

RFC-003 reste propriétaire des schémas conceptuels, identités, temps, nulls et
manifests. RFC-005 définit uniquement leur matérialisation et leur placement.
Les clés de partition ne remplacent pas les clés logiques des tables.

Toute ligne ou fichier dérivé doit pouvoir remonter à un manifest, à sa couche
d'entrée et à sa version de schéma. Les chemins sont relatifs ; les hashes et
comptages sont les preuves d'intégrité.

## Impacts opérationnels

- ATLAS_DATA_DIR doit être vérifié au démarrage des jobs.
- Les jobs utilisent tmp puis publication atomique.
- Les locks ou conventions d'exclusivité empêchent deux compactions
  concurrentes sur la même publication.
- Un job reprend depuis les manifests et checkpoints plutôt que d'examiner
  uniquement la présence d'un fichier.
- La capacité libre, les fichiers orphelins et l'âge des publications sont
  surveillés.
- Les files locales de réplication, les échecs R2 et les retards d'indexation Neon sont surveillés et rejouables.

## Impacts sécurité

Les permissions séparent écriture raw, publication dérivée et lecture des
packs. Les logs et manifests ne contiennent pas de secrets. Les suppressions
de données sont auditées et ne sont pas exécutées par une compaction ordinaire.

## Impacts performance

Le partitionnement favorise les scans temporels, les audits par fenêtre et les
lectures de cohortes. Les benchmarks mesureront débit d'écriture, compression,
lecture sélective, coût DuckDB, compaction, occupation disque et transfert de
packs. L'optimisation doit conserver les contrats de provenance et ne pas
multiplier les petites partitions.

## Compatibilité et migration

Un changement de chemin, format, partitionnement ou politique de rétention
produit une nouvelle version de stockage et un manifest de migration. Les
anciens manifests restent lisibles. Une migration ne supprime l'ancienne
publication qu'après validation des hashes, comptages, dépendances et capacité
de restauration.

Un changement de schéma suit la compatibilité définie par RFC-003. Les lecteurs
doivent déclarer les versions supportées et ne pas déduire une colonne absente.

## Observabilité

Les métriques minimales sont : espace total et libre, croissance par couche,
fichiers et bytes par partition, petits fichiers, fichiers orphelins, échecs de
hash, manifests manquants, distributions `coverage_status` et états de publication, durée et débit de compaction,
âge du dernier fichier, âge du dernier manifest, erreurs DuckDB, état et âge de réplication R2, échecs/retries, retards d'indexation Neon et temps de transfert des packs.

Chaque alerte cite couche, partition, version, run et manifest concernés.

## Tests

L'implémentation devra démontrer :

1. publication atomique après interruption ;
2. lecture d'une partition complète et d'un sous-ensemble temporel ;
3. cohérence hashes/comptages entre manifest et fichiers ;
4. rejeu d'une compaction sans modification du raw ;
5. absence de divergence entre vue DuckDB et Parquet source ;
6. restauration d'un pack sur un chemin différent ;
7. détection d'un fichier orphelin, manquant ou modifié ;
8. application contrôlée d'une politique de rétention ;
9. compatibilité de lecture entre versions autorisées ;
10. comportement documenté lorsque l'espace libre est insuffisant.
11. publication locale valide et reprise de réplication après indisponibilité R2 ou Neon.
12. restauration R2 vers des chemins relatifs reconstruits sous `ATLAS_DATA_DIR`.

## Critères d'acceptation

La RFC pourra être ACCEPTED lorsque les couches, formats, chemins, partitions,
publications atomiques, manifests, compaction, rétention et rôle de DuckDB
seront cohérents avec RFC-002, RFC-003 et RFC-004 ; lorsque les chemins seront
configurables ; et lorsque l'intégrité, le transfert et la restauration seront
testables.

Aucune création de base ou migration volumineuse ne démarre avant ACCEPTED.

## Questions ouvertes

- Quel volume quotidien réel et quelle capacité de réserve doivent guider les
  seuils de partition et de compaction ?
- Quelle durée chiffrée retenir par couche après mesure du stockage ?
- Quelle stratégie de verrouillage et de reprise utiliser pour les compactions ?
- Quelle taille cible de fichier Parquet maximise lecture et transfert ?
- Quand matérialiser une vue DuckDB plutôt que la recalculer ?
- Quel format spécialisé retenir pour les graphes et séquences dans les packs ?

## Décision finale

En attente de revue. Cette RFC reste DRAFT et n'autorise aucune création de
base, migration volumineuse ou politique destructive de rétention.

## Historique des modifications

| Date | Version | Modification | Auteur |
| --- | --- | --- | --- |
| 2026-07-17 | 0.1 | Création du brouillon | Équipe AtlasPump |
| 2026-07-17 | 0.2 | Ajout des publications locales, indexation Neon, archivage R2 et workflow de réplication vérifiée. | Équipe AtlasPump |
| 2026-07-17 | 0.3 | Politique C v1 non destructive : échantillonnage déterministe, fenêtre raw/normalized, états de quarantaine et manifestes de planification. | Équipe AtlasPump |
