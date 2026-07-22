# RFC-009 — Dataset Factory

RFC-009 transforme les features RFC-008 en cohortes temporelles reproductibles,
avec labels séparés, exclusions explicites et splits walk-forward. Elle ne
comprend ni modèle, ni backtest, ni imputation apprise. Le périmètre détaillé
est 19–26 avril; 25–26 sont réservés aux tests principaux. Les labels ne
produisent un négatif que lorsque le suivi minimal est confirmé; toute censure
incompatible reste exclue. Les partitions sont publiées date par date et
feature-horizon par feature-horizon afin de borner la mémoire.
