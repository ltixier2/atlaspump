# Glossaire AtlasPump

| Terme | Définition |
| --- | --- |
| Raw | Payload fournisseur immuable, conservé avec provenance. |
| SourceEvent | Message brut reçu d'une source. |
| CanonicalEvent | Événement normalisé dans le contrat commun. |
| Événement logique | Occurrence métier dédupliquée, identifiée par `event_id`. |
| Curated | Entité reconstruite/validée portant qualité et provenance. |
| Feature | Variable dérivée à un instant `observation_cutoff`. |
| Label | Cible d'apprentissage versionnée et potentiellement censurée. |
| Prediction | Sortie d'un modèle versionné ; ce n'est pas un fait. |
| Manifest | Description versionnée des entrées, sorties, hashes et producteur. |
| Censure gauche/droite | Début/fin de l'observation inconnus ou incomplets. |
| `UNKNOWN` | Événement reçu mais non classifié ; distinct de zéro et d'absence. |
| `as_of` / observation cutoff | Instant maximal de connaissance autorisé pour un calcul. |
| Lifecycle | Reconstruction versionnée de faits concernant un mint dans une fenêtre d'observation. |
| Censure | Limite connue de la fenêtre d'observation ; elle n'est pas un résultat économique. |
| Migration observée | Migration soutenue par un événement canonique qualifié. |
| Migration inférée | Migration déduite par une règle versionnée, avec confiance et preuves. |
| Période de grâce | Délai documenté pour absorber retard de source ou finalisation avant conclure sur la couverture. |
