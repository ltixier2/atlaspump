# Contribuer à AtlasPump

AtlasPump est actuellement en phase de conception. Les décisions structurantes
sont prises par RFC avant toute implémentation.

## Processus de conception

1. Créer ou mettre à jour une RFC à partir du [modèle](docs/rfc/RFC-000-template.md).
2. Publier la RFC avec le statut `DRAFT`.
3. Soumettre la RFC à revue.
4. Résoudre ou tracer explicitement les questions ouvertes.
5. Passer la RFC à `ACCEPTED` lorsque la décision est validée.
6. Une RFC `DRAFT` peut exceptionnellement avoir une implémentation
   expérimentale, explicitement `NON_PROMOTABLE` et isolée du live.
7. `ACCEPTED` signifie que le contrat est stabilisé ; une implémentation
   expérimentale ne vaut pas validation de production.
8. Toute promotion exige une revue, des tests, un audit et une décision
   explicite.
9. Après validation, passer la RFC à `IMPLEMENTED`.

> Aucun agent ne doit promouvoir une fonctionnalité dont la RFC structurante
> n'est pas `ACCEPTED`. Les implémentations DRAFT sont admises uniquement pour
> des expérimentations offline ou shadow clairement non promotionnables.

Les données volumineuses ne sont jamais ajoutées à Git. Les emplacements de
données doivent être configurés avec `ATLAS_DATA_DIR`, jamais codés en dur.
