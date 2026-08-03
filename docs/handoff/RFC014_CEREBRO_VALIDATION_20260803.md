# RFC-014 Cerebro — validation des corrections de persistance et de supervision

Document de synthèse, 2026-08-03. Trace de code/documentation pour les
corrections de persistance et de supervision réalisées sur le pipeline
shadow RFC-014 (Cerebro). **Ne prétend pas que la validation économique est
terminée** — voir §Hors périmètre.

## Contexte

Le run 24h original du pipeline shadow (snapshot v8.1.1) s'est terminé
avec un backlog transport de 86 302 enregistrements non consommés, causé
par un coût de persistance croissant avec la taille de l'état accumulé.
Ce travail corrige cette cause racine en deux temps, puis une régression
de supervision découverte lors de la revalidation à pleine échelle.

## Corrections WAL/checkpoints

**Dédoublonnage (`state.seen`)** — `persist_locked()` réécrivait
l'intégralité de l'ensemble de déduplication (`sorted()` + JSON complet) à
chaque événement, un coût O(n) par appel appliqué n fois sur la durée d'un
run. Remplacé par :
- `seen_event_ids.wal` — append-only, O(1) par nouvel événement
  (`StreamState.record_seen`) ;
- `seen_event_ids.json` — checkpoint complet périodique
  (`StreamState.checkpoint`), WAL tronqué seulement après écriture
  confirmée du checkpoint qui le supersède.

**État des fenêtres (`window_state`)** — même anti-pattern découvert lors
de la revalidation à grande échelle : chaque `TokenWindow` conservait sa
liste d'événements bruts sans jamais la vider après finalisation, alors
que rien ne la relit une fois la fenêtre fermée
(`WindowBook._due_locked()`/`censor_open_windows()` ne touchent que les
fenêtres `OPEN`). Corrigé par un découpage à trois niveaux :
- `open_windows_state.json` — réécrit à chaque événement, mais bon marché
  car borné par le nombre de fenêtres concurremment ouvertes, pas par le
  nombre total de tokens ;
- `window_transitions.wal` — append-only pour chaque finalisation, censure,
  écriture de prédiction/outcome ;
- `window_outcome_state.json` — checkpoint périodique des fenêtres
  fermées, **sans** leurs événements bruts.

Reprise après crash : fusion checkpoint + rejeu WAL au chargement, rejeu
idempotent par construction, testé pour l'interruption avant et après
checkpoint, et pour un rejeu WAL répété deux fois.

## Gain de performance observé

| Mesure | Avant | Après |
|---|---:|---:|
| Coût dédup par événement (n=213 760) | 284,31 ms | 2,07 ms (×138) |
| Coût checkpoint fenêtres (2351 tokens × 46 événements) | 462,5 ms | 19,5 ms (×23,7) |
| Débit consumer (replay offline comparable) | ~15,2 événements/s (n'a pas terminé dans le budget alloué) | ~33,6 événements/s (a terminé) |

## Validation replay complet et smoke live

- **Replay offline complet** : intégralité de l'archive capturée du run
  24h original (308 648 enregistrements, 292 146 événements valides, 9868
  tokens) rejouée avec le code corrigé — `COMPLETED_EOF` (terminaison
  naturelle, une première), zéro gap/doublon/dead-letter/erreur
  modèle/orphelin. Extraction accélérée via cache + parallélisation
  (lecture seule, hors code de production) : 22-23 s au lieu de ~40 min.
- **Smoke live** (30 min max / 150 tokens max, données PumpAPI réelles) :
  `COMPLETED_EOF`, backlog transport final à 0, T+10 médiane 5 ms / p95
  13,1 ms.
- **Run 24h live complet** (snapshot v8.1.3, correctifs dédup + fenêtres) :
  backlog transport resté à 1-7 enregistrements pendant 24h continues (vs
  86 302 sur le run original) — confirme les deux correctifs à pleine
  échelle et en conditions réelles.

## v8.1.4 — correction de supervision, testée sur fermeture courte

Le run 24h v8.1.3 a révélé une régression distincte : le consumer partageait
le même budget de temps que le collecteur, pouvant s'arrêter quelques
secondes avant la fin réelle de celui-ci et manquer les tout derniers
enregistrements (dont `COLLECTOR_SHUTDOWN`/`SESSION_END`) — écart observé
de 7 enregistrements sur 309 507. Corrigé exclusivement au niveau de la
configuration de supervision (nouveau script `commands/launch_24h.sh`,
bundlé uniquement dans le snapshot candidat v8.1.4 — **aucun changement de
`shadow_stream.py` ni `run_shadow_stream.py` pour cette correction**) :
budget de temps du consumer découplé de celui du collecteur, arrêt piloté
exclusivement par le superviseur (drainage puis EOF naturel).

Testé par un **run de fermeture court** (collecteur 120 s, capture PumpAPI
réelle) : `COMPLETED_EOF`, source et cursor finaux strictement alignés
(2152/2152), backlog final à 0, `COLLECTOR_SHUTDOWN`/`SESSION_END`
consommés, aucun `BrokenPipeError`, aucun processus résiduel.

## Réserve explicite — v8.1.4 non revalidé sur un second run 24h complet

**Aucun second run 24h complet n'a été exécuté avec le correctif de
supervision de v8.1.4.** Seul un test court (2 minutes) valide le
mécanisme corrigé. Le comportement de débit/backlog sur 24h reste hérité
de la validation v8.1.3 (code pipeline inchangé), mais n'a pas été
re-mesuré avec ce script de supervision précis sur la durée complète. Un
nouveau run 24h de validation reste nécessaire avant toute décision de
promotion, avec un seuil T+10 explicite à confirmer au préalable (proposé :
p50 ≤ 10 s, p95 ≤ 90 s — non validé formellement).

## Hors périmètre — aucune validation économique ni trading réel

Ce travail porte uniquement sur la fiabilité opérationnelle du pipeline
shadow (persistance, débit, fermeture propre). **Il ne constitue aucune
validation économique** : aucun outcome de gain/perte, aucun modèle
CatBoost connecté à une décision, aucune exécution de trade réel ou
simulé. Le pipeline reste `shadow_only` de bout en bout sur toutes les
validations décrites ici.

## Snapshots concernés (non inclus dans ce dépôt)

`v8.1.1` → `v8.1.4` vivent sous `/mnt/atlaspump/deployments/` (hors dépôt
git, hors périmètre de cette PR). Ce commit documente et trace le code
source ayant produit ces snapshots, pas les snapshots eux-mêmes.
