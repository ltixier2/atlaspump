# RFC-004 — Collecte live et replay

| Champ | Valeur |
| --- | --- |
| Numéro | RFC-004 |
| Titre | Collecte live et replay |
| Statut | DRAFT |
| Auteur | Équipe AtlasPump |
| Date | 2026-07-17 |
| Version | 0.1 |

## Résumé

Cette RFC définit le contrat de collecte historique et live d'AtlasPump. Elle
sépare l'adaptateur de source, la capture brute immuable, les checkpoints, les
manifestes et les couches de normalisation ultérieures. Le replay et le live
partagent les mêmes contrats ; le live ajoute la fraîcheur, le retard et la
reprise.

La RFC ne choisit ni framework, ni service distribué, ni déploiement
particulier. Elle n'implémente aucun collecteur et ne transforme pas les
payloads en événements canoniques.

## Contexte

PumpApi est la source principale retenue à ce stade pour les archives de replay
et le flux live. PumpPortal et les RPC sont des sources secondaires pour la
validation, la redondance et les enrichissements ciblés. Les expérimentations
ont validé le téléchargement et la lecture streaming d'archives horaires
JSONL.ZST sur Cerebro. Ces résultats sont une preuve de faisabilité, pas une
garantie de comportement futur du fournisseur.

## Problème

Une collecte qui écrit directement des données normalisées ou qui reprend au
dernier fichier connu peut perdre des événements, modifier l'historique ou
produire des doublons impossibles à auditer. Le système doit savoir quelle
source et quelle fenêtre ont été demandées, quels payloads ont été reçus,
quand ils l'ont été, quelles portions sont manquantes et où reprendre sans
altérer le raw.

## Objectifs

1. Capturer les payloads originaux avec leur provenance.
2. Conserver un raw immuable, append-only et rejouable.
3. Fournir checkpoint et reprise après erreur ou arrêt.
4. Rendre les runs historiques idempotents à entrée égale.
5. Partager les contrats essentiels entre replay et live.
6. Détecter et mesurer trous, doublons, retards et erreurs.
7. Publier chaque fenêtre finalisée avec manifeste et hashes.
8. Isoler les fournisseurs derrière des adaptateurs remplaçables.
9. Permettre de relire le raw sans rappeler le fournisseur.
10. Préserver une approche micro-batch avant toute distribution complexe.

## Non-objectifs

- définir le schéma complet de CanonicalEvent ;
- normaliser ou classifier les événements collectés ;
- garantir une couverture supérieure à celle du fournisseur ;
- reconstruire toute la blockchain Solana ;
- imposer Kafka, Redis, une base centrale ou un orchestrateur ;
- exécuter des ordres ou prendre une décision de trading ;
- fusionner silencieusement des sources contradictoires ;
- supprimer du raw un payload invalide, inconnu ou dupliqué ;
- définir la rétention et le partitionnement détaillés de RFC-005.

## Proposition

### Architecture logique

La chaîne logique est :

    source replay/live
        → source adapter
        → capture envelope
        → raw append-only
        → manifeste de collecte
        → normalisation ultérieure

L'adaptateur est responsable du protocole fournisseur et ne publie jamais
directement une ligne canonique. Il émet une enveloppe contenant le payload
original et ses métadonnées. Le Raw Ingestion Layer écrit ces enveloppes,
finalise les fichiers atomiquement et publie le manifeste après contrôle.

### Modes de collecte

| Mode | Entrée | Unité de reprise | Fin de fenêtre |
| --- | --- | --- | --- |
| Replay | archive ou fenêtre historique | fichier, partition, offset | explicite |
| Live | flux ou polling fournisseur | curseur, offset ou borne temporelle | run ouvert ou micro-batch |
| Rattrapage | fenêtre déjà tentée | unité défaillante | manifeste révisé ou nouvel essai |

Le replay est prioritaire pour valider les contrats. Le live peut être une
boucle checkpointée ou un micro-batch ; il ne doit pas créer un contrat
différent.

### Enveloppe de capture

Chaque unité capturée porte au minimum :

| Champ | Sémantique |
| --- | --- |
| source_event_id | Identité fournisseur ou identité locale versionnée. |
| provider | Fournisseur effectivement utilisé. |
| mode | REPLAY, LIVE ou BACKFILL. |
| capture_run_id | Identifiant unique du run. |
| source_partition | Archive, canal, fenêtre ou partition fournisseur. |
| source_cursor | Offset, curseur ou position si disponible. |
| payload | Message original, sans correction sémantique. |
| payload_hash | Hash du payload selon une représentation définie. |
| received_at | Instant UTC de réception par AtlasPump. |
| archive_timestamp | Temps associé à l'archive, nullable. |
| source_timestamp | Temps fourni par le flux, nullable. |
| adapter_version | Version de l'adaptateur. |
| status | RECEIVED, DUPLICATE, INVALID, ERROR ou équivalent versionné. |
| raw_reference | Fichier, ligne, offset ou bloc raw. |

Le payload est la donnée brute ; aucun champ dérivé ne doit le remplacer. Les
métadonnées de capture restent distinctes de CanonicalEvent.

### Raw et finalisation

Le raw est écrit dans des fichiers temporaires, puis finalisé atomiquement.
Un fichier finalisé ne doit plus être modifié. Une reprise produit un nouveau
run et de nouvelles métadonnées ; elle ne réécrit pas silencieusement une
publication existante.

Chaque fichier finalisé est associé à son hash, sa taille, sa fenêtre logique,
sa partition source, ses comptages, son run, sa version d'adaptateur et son
manifeste. Le manifeste porte l'état PROVISIONAL, COMPLETE, PARTIAL ou FAILED.

Les payloads invalides ou inconnus restent traçables dans le raw ou une zone
de rejets liée au même run. Leur exclusion d'une couche dérivée ne supprime
pas la source.

### Checkpoints et reprise

Un checkpoint décrit la dernière position confirmée et durablement associée au
run. Il ne progresse qu'après écriture et contrôle de l'unité correspondante.
Il contient au minimum le run, le fournisseur, le mode, la fenêtre, le curseur
ou la partition, la dernière position, la référence du dernier fichier,
les compteurs, l'état, l'instant de mise à jour, la version d'adaptateur et la
configuration non secrète.

Après interruption, le collecteur reprend au dernier checkpoint sûr avec une
marge de recouvrement lorsque le fournisseur peut rejouer des éléments. Les
doublons de recouvrement sont conservés et identifiés ; leur déduplication
analytique suit RFC-003 et ne modifie pas le raw.

Une erreur non récupérable bloque la publication complète de la fenêtre. Un
état PARTIAL n'est possible que si les trous, erreurs et bornes sont manifestés.

### Idempotence

À fenêtre, fournisseur, version d'adaptateur et configuration égales, un replay
doit produire les mêmes identités de capture et hashes de payload. Les runs
peuvent avoir des identifiants différents, mais l'identité du contenu ne dépend
pas de l'heure de reprise.

L'identité repose sur l'identifiant fournisseur lorsqu'il existe ; sinon sur
fournisseur, partition, offset et hash du payload, avec id_strategy_version.
Une collision entre payloads divergents est une anomalie, pas une
déduplication silencieuse.

### Replay, live et temps

Le temps de réception ne remplace jamais le temps blockchain. La collecte
conserve séparément les temps source, archive et réception. Un événement livré
en retard ou hors ordre est conservé tel quel et signalé ; le tri canonique et
la reconstruction interviennent ensuite.

Le live expose au minimum le retard calculable, la dernière réception, l'âge
du dernier checkpoint, les fenêtres ou curseurs manquants, les reconnexions,
les erreurs et le volume reçu par unité de temps.

### Manifeste de collecte

Le manifeste utilise le contrat Manifest de RFC-003 et peut ajouter :

    manifest_type = COLLECTION
    provider
    mode
    requested_window
    covered_window
    source_partitions
    source_cursors
    capture_run_id
    adapter_version
    input_request_hash
    source_files
    source_hashes
    output_files
    output_hashes
    received_count
    invalid_count
    duplicate_count
    missing_partitions
    error_count
    status

COMPLETE signifie que le contrat de couverture déclaré est satisfait ; cela ne
prouve pas que le fournisseur n'a jamais perdu un événement. PARTIAL et FAILED
doivent expliciter la raison, la fenêtre et les unités à reprendre.

### Sources secondaires

PumpPortal et les RPC sont utilisés via des adaptateurs distincts. Une donnée
secondaire conserve son fournisseur et sa provenance. Elle peut être comparée
ou jointe dans une couche ultérieure, mais ne corrige pas silencieusement la
source principale.

## Modèle de données concerné

Cette RFC consomme SourceEvent, CanonicalEvent, Manifest et les identifiants
versionnés de RFC-003. Elle produit des captures et métadonnées pouvant
alimenter source_events, mais ne publie pas encore CanonicalEvent.

Le lien minimal est :

    capture envelope → raw_reference/source_event_id → CanonicalEvent.raw_reference

Les temps blockchain, archive et réception restent distincts. UNKNOWN, INVALID
et les absences sont conservés avec provenance et ne deviennent pas des
valeurs sentinelles.

## Impacts opérationnels

- Cerebro est le nœud d'exécution prioritaire, avec ATLAS_DATA_DIR configurable.
- Collecte et normalisation sont rejouables indépendamment.
- Les jobs sont supervisables par fenêtre, run, partition et manifeste.
- Les fichiers partiels ne sont jamais présentés comme complets.
- Le stockage et la compaction détaillés relèvent de RFC-005.
- Le live peut démarrer en micro-batch sans bus distribué.

## Impacts sécurité

Les secrets fournisseur restent hors Git et hors manifeste portable. Les droits
d'écriture sur le raw sont limités au compte de collecte. Les logs et manifests
ne contiennent jamais de token d'accès. Aucun chemin de collecte ne déclenche
d'ordre ou d'action financière.

## Impacts performance

Le replay est streaming afin de limiter la mémoire et traiter les archives
réelles. Les performances sont mesurées séparément pour téléchargement,
décompression, écriture et reprise. Le live privilégie débit, fraîcheur et
reprise ; une optimisation ne doit pas sacrifier le raw.

Les limites fournisseur, bande passante, disque, temporaires et petits fichiers
seront mesurées avant les choix de compaction de RFC-005.

## Compatibilité et migration

Un changement de fournisseur, schéma ou adaptateur crée une nouvelle
adapter_version et conserve les captures anciennes. Un changement d'identité
crée un nouvel id_strategy_version et, si nécessaire, une correspondance. Les
archives restent lisibles même si le fournisseur disparaît. Toute évolution
majeure publie un manifeste de migration.

## Observabilité

Les métriques minimales sont les requêtes et réponses, événements reçus,
invalides et dupliqués, octets téléchargés et écrits, débit, latence, retard,
partitions manquantes, erreurs, retries, reconnexions, interruptions,
checkpoints, fichiers temporaires et finalisés, états de manifestes, espace
disque et durée du run.

Les logs sont corrélables par capture_run_id, fournisseur, fenêtre, partition
et manifest_id. Le monitoring ne remplace pas les manifestes de provenance.

## Tests

L'implémentation devra démontrer :

1. replay d'une archive sans perte de payload ;
2. reprise avant et après finalisation ;
3. idempotence sur deux replays identiques ;
4. détection d'un payload divergent sous une identité ;
5. conservation des payloads invalides et inconnus ;
6. finalisation atomique sans fichier partiel publié ;
7. manifestes corrects pour COMPLETE, PARTIAL et FAILED ;
8. tolérance aux doublons de recouvrement ;
9. séparation des temps source, archive et réception ;
10. simulation d'un retard, d'un trou et d'une reconnexion live ;
11. absence de secrets dans logs et manifests ;
12. relecture du raw par une autre version de normalisation.

## Critères d'acceptation

La RFC pourra être ACCEPTED lorsque les responsabilités adaptateur, capture,
raw, checkpoint et normalisation seront sans ambiguïté ; lorsque le contrat
replay/live sera cohérent avec RFC-002 et RFC-003 ; lorsque immutabilité,
provenance, idempotence, reprise, couverture et états de manifeste seront
vérifiables ; et lorsque les frontières avec RFC-005 seront explicites.

Aucune implémentation structurante ne démarre avant ACCEPTED.

## Questions ouvertes

- PumpApi fournit-il un curseur stable et une rétention suffisante ?
- Quelle marge de recouvrement appliquer à chaque source ?
- Quelle durée de silence déclenche une alerte live ?
- Quand une fenêtre PARTIAL doit-elle être rejouée ?
- Quel niveau de duplication conserver avant compaction ?
- Faut-il une seconde capture indépendante pour mesurer la couverture ?
- Quelle stratégie de backoff respecter par fournisseur ?
- Quelle cadence de micro-batch convient à Cerebro ?

## Décision finale

En attente de revue. Cette RFC reste DRAFT et n'autorise aucune implémentation
de collecteur ou de service live structurant.

## Historique des modifications

| Date | Version | Modification | Auteur |
| --- | --- | --- | --- |
| 2026-07-17 | 0.1 | Création du brouillon | Équipe AtlasPump |
