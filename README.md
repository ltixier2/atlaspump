# AtlasPump

AtlasPump est une plateforme de recherche consacrée à l'écosystème Pump.fun et
PumpSwap. Elle vise à produire des données fiables et reproductibles pour la
reconstruction de cycles de vie de tokens, l'analyse et l'apprentissage
automatique.

## Vision

Les données sont l'actif principal du projet. Chaque fait observé doit rester
traçable vers sa source, et chaque donnée dérivée doit pouvoir être rejouée et
versionnée.

## Pipeline cible

```text
Événements Pump.fun / PumpSwap
  -> collecte et replay historique
  -> normalisation Parquet
  -> reconstruction des lifecycles
  -> contrôle qualité
  -> Feature Lab
  -> Dataset Factory versionnée
  -> modèles et évaluation
  -> backtests réalistes
  -> inférence, paper trading, puis trading éventuel
```

## Statut

Le projet est en **phase de conception**. Aucune fonctionnalité métier, aucun
collecteur, aucun pipeline exécutable ni modèle ne sont encore implémentés.

## RFC

Les décisions structurantes sont définies et revues dans les
[RFC](docs/rfc/README.md). Une fonctionnalité ne peut être implémentée qu'après
l'acceptation de sa RFC structurante.

## Données

Les données volumineuses, modèles, checkpoints et bases locales ne doivent
jamais être stockés dans Git. Les chemins physiques sont configurables avec
`ATLAS_DATA_DIR` ; Cerebro est le stockage opérationnel, Neon le catalogue et
Cloudflare R2 la sauvegarde distante restaurable ; voir [.env.example](.env.example).

## Pipeline journalier local

Le replay journalier est séparé en deux publications : collecte raw puis normalisation. Les commandes ne téléchargent rien lorsque `--input-root` contient des archives locales au chemin `YYYY/MM/DD/HH.jsonl.zst`.

```bash
python -m atlaspump.cli collect-day --date 2026-04-19 --hours 0-23 \
  --input-root /chemin/vers/archives --output-root /mnt/atlaspump --resume
python -m atlaspump.cli normalize-day --date 2026-04-19 \
  --raw-manifest /mnt/atlaspump/manifests/collection/date=2026-04-19/collection_manifest.json \
  --output-root /mnt/atlaspump
```

Les manifestes utilisent des chemins relatifs à `--output-root`. La normalisation écrit `canonical_observations.parquet`, avec `source_event_id`, `canonical_observation_id`, `logical_event_id`, `slot`, `block_height` et `provider_block`. Les commandes historiques fondées sur `event_id` et `block` restent legacy ; elles ne constituent pas une publication RFC-003.
