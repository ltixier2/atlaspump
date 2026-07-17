# Pipeline journalier local

La collecte `collect-day` copie des archives déjà présentes dans `--input-root` vers `raw/pumpapi/replay/date=YYYY-MM-DD`, en streaming pour les compteurs et les checkpoints. Elle publie atomiquement un manifeste `COLLECTION` contenant les partitions, hashes, tailles, statuts de couverture, contrat et utilisabilité.

`normalize-day` accepte uniquement ce manifeste raw. Il relit les archives sans accès PumpApi, conserve les rejets et publie `normalized/date=YYYY-MM-DD/canonical_observations.parquet` ainsi qu’un manifeste `NORMALIZATION`.

Le champ legacy `event_id` est absent de ce contrat, sauf colonne `event_id_legacy` nullable réservée à un adaptateur de lecture ultérieur. Une sortie historique contenant `block` et `event_id` est lisible uniquement avec le lecteur legacy ; elle ne doit pas être fusionnée avec une sortie RFC-003. Les fichiers Jour 1 ne sont ni migrés ni modifiés par ce pipeline.

La publication locale est non destructive : une publication `LOCAL_COMPLETE` existante n’est jamais écrasée. `--resume` réutilise uniquement un manifeste déjà publié. Les fichiers auxiliaires et faux Parquet restent exclus par `atlaspump.file_discovery`.
