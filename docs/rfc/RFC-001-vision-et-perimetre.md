# RFC-001 — Vision et périmètre d'AtlasPump

| Champ | Valeur |
| --- | --- |
| Numéro | RFC-001 |
| Titre | Vision et périmètre d'AtlasPump |
| Statut | DRAFT |
| Auteur | Équipe AtlasPump |
| Date | 2026-07-16 |
| Version | 0.1 |

## Résumé

AtlasPump est une plateforme de recherche sur Pump.fun et PumpSwap. Son actif
principal est un historique et un flux d'événements structurés, traçables et
rejouables. La plateforme doit permettre l'étude des cycles de vie de tokens,
des créateurs, des wallets et de leurs relations, puis l'évaluation rigoureuse
de modèles et de stratégies.

Le projet n'est pas un simple bot de trading. L'inférence, le paper trading et
l'exécution réelle éventuelle sont des consommateurs aval de données et de
modèles validés ; ils ne définissent pas la finalité initiale du projet.

Cette RFC fixe les objectifs, limites, utilisateurs, principes, critères de
succès, hypothèses, risques et arbitrages qui orienteront les RFC ultérieures.
Elle ne fixe ni format de stockage précis, ni fournisseur exclusif, ni choix de
modèle, ni implémentation.

## Contexte

Pump.fun et PumpSwap génèrent des événements rapides, hétérogènes et fortement
temporels. Les analyser utilement demande de conserver les faits bruts, de les
normaliser, de reconstruire l'état des tokens et de produire des jeux de données
dont la disponibilité temporelle est connue.

Des expérimentations réalisées hors de ce dépôt ont démontré la faisabilité de
télécharger des archives horaires PumpApi en `.jsonl.zst`, de les normaliser en
flux vers Parquet, de classifier les événements `BUY`, `SELL`, `TRANSFER`,
`CREATE_TOKEN`, `CREATE_POOL`, `MIGRATE`, `ADD_LIQUIDITY` et
`REMOVE_LIQUIDITY`, de fusionner plusieurs heures, de dédupliquer par
identifiant déterministe, de reconstruire des lifecycles et de traiter des
millions d'événements avec manifestes et rapports. Ces résultats sont des
preuves de faisabilité, non des engagements définitifs de conception.

L'infrastructure cible est distribuée : Cerebro assure prioritairement la
chaîne data et les traitements CPU ; le MacBook Pro M4 Pro sert au développement
et aux modèles tabulaires ; la machine Windows équipée d'une RTX 3080 vise les
entraînements deep learning. Les packs de données doivent être portables entre
ces machines. Les chemins physiques, y compris `/mnt/atlaspump` sur Cerebro,
restent configurables via `ATLAS_DATA_DIR` et ne doivent jamais être codés en
dur.

## Problème

Sans corpus canonique, versionné et temporellement fiable, il est impossible de
répondre de manière reproductible aux questions sur les trajectoires de tokens,
l'activité des créateurs et des wallets, ou l'intérêt réel d'un modèle. Les
données incomplètes, les événements tardifs, le biais de survivant et la fuite
temporelle peuvent produire des conclusions artificiellement optimistes.

AtlasPump doit donc transformer des observations de fournisseurs externes en
datasets traçables, puis distinguer clairement :

- les **faits observés** provenant des sources ;
- les **données dérivées** produites par des transformations rejouables ;
- les **inférences** et décisions produites par des modèles ou des règles.

## Objectifs

1. Constituer un historique propre, exploitable et extensible d'événements
   Pump.fun et PumpSwap, avec collecte live et replay historique.
2. Préserver les données brutes et normaliser les événements vers des formats
   analytiques, notamment Parquet, sans perdre leur traçabilité.
3. Reconstruire le lifecycle de chaque token, y compris les états incertains,
   les données censurées et les observations incomplètes.
4. Mesurer la qualité, la couverture et la complétude des données avant leur
   emploi dans une analyse ou un modèle.
5. Produire des caractéristiques tabulaires, temporelles et relationnelles,
   puis des datasets versionnés et transférables entre machines.
6. Établir des baselines tabulaires et développer, lorsqu'elles apportent une
   valeur démontrée, des modèles graphiques et temporels.
7. Produire un moteur de backtest réaliste et auditable, incluant frais,
   latence, liquidité et slippage.
8. Préparer une inférence en temps réel et, seulement à un stade ultérieur, le
   paper trading puis une exécution réelle soumise à des RFC dédiées.
9. Assurer reproductibilité, observabilité et traçabilité de la source brute
   au résultat de recherche, modèle ou simulation.

## Non-objectifs

AtlasPump ne vise pas à :

- promettre un rendement financier ou garantir la détection de tous les pumps ;
- exécuter immédiatement des ordres réels ni remplacer les décisions de gestion
  du risque ;
- reconstruire l'intégralité de Solana ou stocker toute la blockchain ;
- dépendre d'un modèle unique, d'un fournisseur unique ou d'une machine unique ;
- utiliser un LLM comme moteur direct de prédiction de marché ;
- privilégier la sophistication d'un modèle au détriment de la robustesse, de
  l'interprétabilité et des baselines ;
- traiter une observation censurée, absente ou incomplète comme un échec du
  token ;
- versionner dans Git des données volumineuses, modèles, checkpoints ou bases
  locales.

## Utilisateurs et usages visés

| Profil | Usage principal |
| --- | --- |
| Chercheur ou expérimentateur | Explorer des hypothèses, définir labels et mesurer des résultats reproductibles. |
| Ingénieur data | Assurer collecte, normalisation, contrats de données, qualité et lineage. |
| Ingénieur ML | Construire, comparer et expliquer des baselines et modèles tabulaires. |
| Ingénieur DL | Évaluer GNN, Transformers temporels et architectures hybrides sur des packs versionnés. |
| Opérateur du pipeline | Superviser les replays, la collecte live, le stockage, les reprises et les coûts. |
| Développeur du moteur de backtest | Simuler des stratégies sans anticipation et avec contraintes de marché explicites. |
| Futur consommateur de scores en temps réel | Consommer des scores, probabilités et métadonnées de modèle avec des contrats stables. |

Le grand public n'est pas une cible initiale. Les résultats sont d'abord des
outils de recherche et d'exploitation interne.

## Principes directeurs

1. **Les données constituent l'actif principal.** Leur qualité, lineage et
   disponibilité priment sur la rapidité d'adoption d'un modèle.
2. **Les modèles sont remplaçables.** Les datasets et contrats d'inférence ne
   doivent pas dépendre d'une architecture particulière.
3. **Les données brutes sont immuables.** Une correction crée une nouvelle
   représentation dérivée, jamais une mutation silencieuse du raw.
4. **Toute transformation est rejouable.** Code, paramètres, entrées et sortie
   versionnée doivent permettre de reconstituer le résultat.
5. **Toute donnée dérivée est versionnée.** Elle est publiée avec un manifeste,
   son schéma et son lineage.
6. **Les faits sont séparés des inférences.** Les probabilités, labels et états
   reconstruits ne sont jamais présentés comme des faits observés.
7. **La censure est une information.** Une censure gauche ou droite, ou une
   absence de couverture, ne signifie pas échec.
8. **Toute complexité est comparée à une baseline simple.** Une architecture
   plus complexe doit démontrer une valeur additionnelle robuste.
9. **L'évaluation mesure une valeur réelle.** Les métriques ML sont nécessaires
   mais insuffisantes sans robustesse temporelle et, lorsque pertinent,
   simulation après frictions.
10. **Les backtests modélisent les frictions.** Frais, latence, liquidité et
    slippage sont des paramètres explicites, pas des détails facultatifs.
11. **Le projet évite l'enfermement fournisseur.** Les sources externes sont
    isolées par contrats et les données conservées sous formats portables.
12. **Les données volumineuses restent hors Git.** Git contient code,
    documentation, manifests et petits fixtures seulement.
13. **Les environnements sont configurables.** Aucun chemin propre à une
    machine n'est inclus dans la logique ou les manifests portables.
14. **Les décisions importantes sont documentées.** Les RFC servent de registre
    de décision et de contrat de revue.
15. **Aucune brique structurante ne précède sa RFC.** Aucun agent ne doit
    l'implémenter avant le statut `ACCEPTED` de la RFC correspondante.

## Questions de recherche

AtlasPump doit permettre de tester, sans présumer de la réponse :

- Peut-on détecter assez tôt les tokens ayant une forte probabilité de migration ?
- Peut-on estimer la probabilité d'atteindre des multiples de rendement ou des
  seuils de market cap explicitement définis ?
- Peut-on prédire un retournement rapide et améliorer le timing de sortie face
  à des règles simples ?
- Peut-on distinguer une activité organique d'une activité coordonnée ?
- Les relations entre wallets apportent-elles une information additionnelle aux
  seules séries temporelles ?
- Quels signaux de créateurs sont prédictifs hors période d'entraînement ?
- Quelle fenêtre d'observation équilibre précocité, complétude et qualité de
  label ?
- Un système hiérarchique combinant modèle tabulaire, GNN et Transformer
  produit-il un gain net, robuste et justifié par son coût ?
- Un gain prédictif subsiste-t-il après frais, slippage, liquidité et latence ?

## Proposition

AtlasPump adopte une progression centrée sur les données :

1. sources et captures brutes immuables ;
2. normalisation canonique et contrôles de qualité ;
3. lifecycles, caractéristiques et datasets versionnés ;
4. baselines tabulaires et évaluation temporelle ;
5. modèles relationnels ou temporels si leur valeur additionnelle est établie ;
6. backtests réalistes ;
7. inférence, paper trading et éventuel trading, soumis à des décisions
   distinctes.

Cerebro est la cible privilégiée pour collecte, replay, normalisation, DuckDB,
lifecycles et préparation des packs. Le MacBook Pro M4 Pro et la machine
Windows RTX 3080 servent à l'expérimentation selon leurs capacités. Cette
répartition est une orientation opérationnelle, pas une contrainte de format ou
de couplage : les packs doivent être portables et leurs chemins configurables.

## Alternatives étudiées

| Alternative | Motif de non-adoption à ce stade |
| --- | --- |
| Construire directement un bot de trading | Inverse l'ordre de dépendance : les données, la validation et le risque ne sont pas encore fondés. |
| Commencer par GNN ou Transformer | Empêche de mesurer la valeur marginale par rapport à une baseline tabulaire fiable. |
| Conserver uniquement des agrégats | Fait perdre la possibilité de rejouer, de corriger et de définir de nouveaux labels. |
| Fonder le système sur un seul fournisseur | Expose la recherche à une indisponibilité ou à une dérive de schéma. |
| Centraliser tous les calculs sur une machine | Réduit la flexibilité alors que l'infrastructure est volontairement hétérogène. |

## Modèle de données concerné

Cette RFC ne définit pas le modèle canonique ; RFC-003 le fera. Elle impose
toutefois les propriétés suivantes :

- les captures brutes, événements normalisés, données dérivées et inférences
  sont des couches distinctes ;
- chaque couche dérivée référence ses entrées, sa version de transformation et
  ses paramètres par manifeste ;
- les temps d'événement, de réception et de disponibilité sont distinguables ;
- les états de censure, couverture et qualité sont représentables ;
- les jeux de données sont exportables sans chemin absolu et sans dépendance à
  une machine donnée.

## Impacts opérationnels

La plateforme devra supporter le replay et la reprise après incident, détecter
les pertes silencieuses, surveiller capacité de stockage et coûts, et produire
des temps de traitement compatibles avec l'usage visé. Les détails de
partitionnement, orchestration, rétention et déploiement appartiennent aux
RFC-004, RFC-005 et RFC-013.

## Impacts sécurité

Les accès aux fournisseurs et, plus tard, aux systèmes d'exécution doivent être
séparés des données et du code. Les secrets ne sont jamais versionnés. Le
trading réel, s'il est envisagé, devra faire l'objet d'une analyse de risque,
de contrôles d'autorisation et de mécanismes d'arrêt dédiés.

## Impacts performance

Les décisions doivent tenir compte du débit d'archives, de la taille des raw,
des transferts de packs, du coût des graphes et des contraintes de latence. La
performance n'est pas un objectif isolé : elle s'arbitre avec l'exhaustivité,
la reproductibilité et le coût.

## Compatibilité et migration

Le dépôt est en phase de conception : aucune migration d'implémentation n'est
requise. Les RFC futures devront définir une évolution explicite des schémas,
des datasets et des contrats afin que les résultats historiques restent
interprétables.

## Observabilité

Les futurs composants devront exposer au minimum leur couverture de source,
volume traité, erreurs, retards, taux d'événements non classés, résultats de
contrôle qualité, identifiants de manifeste et coûts/temps de traitement. Ces
signaux ne sont pas encore définis au niveau de leur instrumentation.

## Tests

Cette RFC est documentaire. Son acceptation exige une revue de cohérence avec
les RFC ultérieures. Les implémentations devront, selon leur domaine, tester
l'idempotence, la rejouabilité, l'absence de fuite temporelle, la compatibilité
de schéma, les contrôles qualité et les hypothèses de backtest.

## Critères d'acceptation

La RFC pourra passer à `ACCEPTED` si :

- son périmètre de recherche, ses utilisateurs et ses non-objectifs sont validés ;
- les principes directeurs sont jugés contraignants pour les RFC ultérieures ;
- les critères de réussite sont mesurables sans promettre une performance non
  observée ;
- les risques et arbitrages structurants sont reconnus et tracés ;
- les questions ouvertes sont attribuées aux RFC suivantes ou explicitement
  différées.

## Mesure du succès

| Niveau | Critères mesurables à définir et suivre |
| --- | --- |
| Données | Couverture des archives, taux d'événements normalisés, taux `UNKNOWN`, taux de lifecycles exploitables, présence de manifests et reproductibilité des datasets. |
| Modèles | Dépassement de baselines naïves, calibration, PR-AUC pour classes rares, stabilité hors période, robustesse temporelle et comparaison aux arbres ; valeur marginale du graphe et du temporel. |
| Trading simulé | Résultats après frais, drawdown, profit factor, stabilité sur plusieurs fenêtres et sensibilité à la latence et au slippage. |
| Opérations | Reprise après incident, absence de perte silencieuse, surveillance du stockage, idempotence, coût maîtrisé et délai de traitement compatible avec l'usage. |

Les seuils numériques restent ouverts : ils dépendront des labels, de la
couverture, de la période étudiée et des contraintes définies dans les RFC
spécialisées. Aucun seuil ne doit être choisi a posteriori pour justifier un
modèle.

## Risques

| Risque | Conséquence | Direction de mitigation |
| --- | --- | --- |
| Biais de survivant | Surestimation de trajectoires positives | Conserver la couverture et les absences, définir les populations étudiées. |
| Fuite de données ou temporelle | Mesures artificiellement optimistes | Séparer disponibilité, événements et splits temporels. |
| Changement de régime ou de protocole | Dégradation hors période | Versionner les périodes, surveiller les dérives et réévaluer. |
| Déséquilibre des classes | Métriques trompeuses | Utiliser métriques adaptées, calibration et baselines. |
| Données manquantes ou censurées | Labels erronés | Représenter explicitement qualité et censure. |
| Dépendance au schéma PumpApi | Rupture de normalisation | Isoler les contrats fournisseur et versionner les schémas. |
| Instabilité de fournisseurs | Trous de données | Prévoir reprise, contrôle de couverture et alternatives. |
| Corrélations trompeuses entre wallets | Faux signal relationnel | Comparer au tabulaire, effectuer validations temporelles et analyses de robustesse. |
| Surapprentissage | Absence de généralisation | Splits temporels, baselines, fenêtres multiples et simplicité par défaut. |
| Métriques mal choisies | Décisions de modèle non pertinentes | Relier les métriques aux usages et aux simulations. |
| Coûts d'infrastructure | Projet non soutenable | Suivre stockage, calcul, transfert et rétention. |
| Faux sentiment de sécurité du backtest | Risque de pertes réelles | Modéliser frictions, tester sensibilité et séparer simulation/exécution. |
| Trading réel | Risque financier et opérationnel | Reporter à des RFC dédiées avec garde-fous et paper trading préalable. |

## Arbitrages

| Arbitrage | Position à instruire dans les RFC suivantes |
| --- | --- |
| Exhaustivité vs coût | Mesurer la valeur d'une couverture plus large avant de la rendre obligatoire. |
| Raw complet vs stockage | Préserver le raw utile au rejeu tout en définissant rétention et compression. |
| Streaming complet vs filtrage | Ne filtrer qu'après avoir évalué la perte d'information et la rejouabilité. |
| Modèle simple vs complexe | Baseline par défaut ; complexité seulement si gain robuste et utile. |
| Précision vs latence | Adapter le compromis au consommateur, sans mélanger recherche batch et inférence live. |
| Temps réel vs batch | Favoriser d'abord la qualité batch ; introduire le live selon un besoin démontré. |
| Robustesse vs vitesse d'itération | Protéger les contrats et manifests, tout en isolant les espaces d'expérimentation. |
| Centralisation vs séparation par machine | Standardiser les packs portables plutôt que les chemins et l'environnement. |
| Richesse du graphe vs coût de calcul | Ajouter relations et profondeur si leur valeur marginale est établie. |
| Fenêtre courte vs qualité du label | Évaluer explicitement précocité, censure et stabilité des labels. |

## Questions ouvertes

- Quelle durée de conservation du raw faut-il retenir ?
- Quelle profondeur historique viser initialement ?
- Quelle définition opérationnelle d'un token à succès doit être adoptée ?
- Quel horizon d'observation minimal garantit un label utile ?
- Quelle politique de censure appliquer selon les usages ?
- Quel niveau de dépendance envers PumpApi est acceptable ?
- Quand passer du batch au live ?
- Quand introduire le graphe dans la stratégie de modèles ?
- Quel niveau de performance, de stabilité et de valeur simulée justifierait un
  modèle complexe ?
- À partir de quel niveau de maturité des données, modèles et backtests
  envisager le paper trading ?

## Décision finale

En attente de revue. Cette RFC reste `DRAFT` et n'autorise aucune
implémentation de brique structurante.

## Historique des modifications

| Date | Version | Modification | Auteur |
| --- | --- | --- | --- |
| 2026-07-16 | 0.1 | Création du brouillon | Équipe AtlasPump |
