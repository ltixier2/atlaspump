# RFC-008 — Feature Lab

## Problème

AtlasPump doit séparer la préparation de variables temporellement sûres de la
Dataset Factory. Sans registre d'instant de disponibilité, un agrégat final de
lifecycle peut devenir par erreur une feature de prédiction précoce.

## Objectifs

- Features versionnées, reproductibles et traçables jusqu'au `token_summary`.
- Instant de disponibilité et fenêtre source explicites.
- Compatibilité replay et faisabilité live documentées.
- Calcul incrémental par jour et mémoire bornée.
- Schémas explicites, publication atomique et absence de fuite du futur.

## Non-objectifs

Ce RFC ne crée ni modèle, ni split train/validation/test, ni stratégie, ni
backtest, ni rentabilité, ni métrique absente sans source validée.

## Périmètres

- **Common :** 18–26 avril 2026, uniquement variables réellement comparables.
- **RFC enrichi :** 19–26 avril 2026, fenêtres et attributs RFC détaillés.

## Politique temporelle

Une feature n'est sélectionnable à l'horizon `h` que si elle est enregistrée,
`ONLINE_SAFE` ou `HORIZON_SAFE`, et disponible à `t <= h`. Les labels et
variables finales (`migration`, timestamp de migration, durée finale, compteurs
de lifecycle final) sont exclus des whitelists d'horizon. Les nulls sont
préservés : zéro, inconnu et non calculé ne sont jamais confondus.

## Limites

Le Jour 18 reste legacy; ses métriques RFC détaillées sont `NOT_COMPUTED`.
`wallet_dominant` et `wallet_dominant_share` restent absents. La censure est
hétérogène et Kaplan-Meier neuf jours n'est pas défini. Deux dimanches sont
insuffisants pour inférer un effet hebdomadaire. Aucun résultat ne prouve une
causalité ou une rentabilité.
