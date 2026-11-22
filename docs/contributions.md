# Contributions réelles du binôme

La fiche enseignante prévoit quatre étudiants. Les sources et l'historique Git fournis ici documentent **deux contributeurs identifiés** ; ce tableau n'attribue aucun travail à deux membres non identifiés.

| Membre identifié | Travaux vérifiables | Livrables / revues |
| --- | --- | --- |
| Mohammed Reda Zelmati (`@MohammedRedaZelmati`) | Contrats événement et features, validation des fichiers FD001, schéma et requêtes PostgreSQL, préparation Silver, qualité et KPI Spark SQL, entraînement et évaluation Spark ML, dashboard SQL, tests, revue des PR d'Ayman | PR #1 à #5 ; `contracts/`, `database/`, `batch/`, `dashboard/`, `docs/data-card.md`, `tests/test_event_contract.py`, `tests/test_rul_targets.py`, `tests/test_ml_splits.py` ; revues #6 à #8 |
| Ayman El Baida (`@Aymannel04`) | Docker et versions du cluster, Kafka, HDFS, bootstrap, producteur et replay, archive Bronze, inférence Structured Streaming, sink PostgreSQL, tests de streaming et essais distribués sur son PC, revue des PR de Reda | PR #6 à #8 ; `infra/`, `ingestion/`, `streaming/`, scripts de replay et bootstrap, `docs/runbook.md`, `docs/runtime-lock.md`, `tests/test_replay_producer.py`, `tests/test_streaming_sink.py` ; revues #1 à #5 |
| Travail commun | Contrats discutés en revue, branche d'intégration, vérification de compatibilité des 25 features, de la clé SQL et des checkpoints ; cette documentation R8 est rédigée par Reda pour revue d'Ayman | PR d'intégration à faire relire par les deux avant fusion |

Les métriques du cluster citées dans le rapport proviennent des essais rapportés par Ayman dans `docs/runtime-lock.md` et ses revues GitHub. Reda a vérifié localement les fichiers FD001, les tests légers, la syntaxe Compose et les fusions, sans revendiquer une seconde exécution distribuée.
