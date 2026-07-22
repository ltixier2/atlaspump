# Registre des features RFC-008

Source machine-readable : `src/atlaspump/features/registry.py`. Le checksum du
registre est publié dans chaque manifeste Feature Lab. Chaque entrée contient
nom, version, famille, colonnes sources, instant et fenêtre de disponibilité,
type, nullabilité, politique de données manquantes/censure, périmètre,
faisabilité replay/live, risque de fuite, statut et avertissements.

Les familles sont : calendrier/common, activité précoce, transactionnelle,
wallets, durée, qualité lifecycle et disponibilité. Les fenêtres 10 s à 60 min
sont `HORIZON_SAFE`. Les compteurs transactionnels, wallets uniques et durée
complète sont actuellement `POST_OUTCOME` : ils peuvent être audités mais ne
peuvent pas être sélectionnés pour une prédiction précoce. Les métriques wallet
dominantes sont `NOT_COMPUTED`.
