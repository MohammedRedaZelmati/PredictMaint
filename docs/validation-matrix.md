# Matrice de validation et provenance des preuves

Les nombres ci-dessous ne mélangent pas les contrôles locaux de Reda et les essais rapportés par Ayman. Les résultats distribués sont documentés dans `docs/runtime-lock.md`, les revues GitHub du 21-22 novembre 2026 et les exports et journaux de [`docs/evidence/`](evidence/README.md), archivés depuis le cluster d'Ayman.

| Contrôle | Machine / source | Résultat constaté | Limite |
| --- | --- | --- | --- |
| Fichiers FD001 téléchargés, hashes, largeur, valeurs finies et cycles | PC de Reda, `docs/data-card.md` | Train 20 631 / 100 moteurs ; test 13 096 / 100 ; RUL officiel 100 | Source miroir PHM Society, licence de redistribution explicite non trouvée |
| Tests Python après fusion #8 | PC de Reda, `python -m pytest -q --basetemp=data/raw/pytest` | 82 réussis, 30 ignorés | PySpark/Java absents localement ; aucun entraînement local revendiqué |
| Compose | PC de Reda, `docker compose config --quiet` | Configuration valide | Aucun conteneur du projet démarré ici ; 4,72 Gio libres au contrôle R8 |
| Cluster Spark et HDFS | PC d'Ayman, `docs/runtime-lock.md` | 2 workers Spark, 2 DataNodes, réplication HDFS 2 ; 200 tâches par worker lors du contrôle distribué | Mesure issue du cluster d'Ayman, non répétée sur le PC de Reda |
| Tests Spark dans l'image | PC d'Ayman, `docker compose run --rm --no-deps spark-client python3 -m pytest -q tests` | 112 réussis, 0 ignoré, Spark inclus | Résultat rapporté dans `docs/runtime-lock.md`, non répété sur le PC de Reda |
| Kafka et Bronze | PC d'Ayman, runtime-lock et PR #7 | Train 20 631 lignes logiques, test 13 096 ; 0 rejet sur les replays complets | Les replays de vérification ajoutent 412 doublons physiques test, dédupliqués en Silver |
| Contrat Kafka renforcé | PC d'Ayman, commentaire PR #7 | 4 événements invalides injectés ; archive et inférence : 4 reçus / 4 rejetés ; comptes Bronze/SQL inchangés | Le test sur le cluster est rapporté par Ayman ; pas répété ici |
| Silver et KPI finaux | PC d'Ayman, revues PR #2 | Silver 20 631 ; 164 KPI ; bandes RUL 2 100 / 3 000 / 15 531 ; doublons Bronze train 0 / test 412 | `missing_value_pct` vaut 0 par contrat sur Bronze accepté ; rejets comptés par l'archive |
| Entraînement et tuning | PC d'Ayman, [`comparison.csv`](evidence/model_v001/comparison.csv) et [`metrics.json`](evidence/model_v001/metrics.json) | B0 + LR + RF + 8 GBT ; GBT profondeur 5, 60 itérations, pas 0,05 ; validation RMSE 22,37 / MAE 17,14 sur 60 snapshots | LR a divergé ; les 11 candidats et leurs scores sont archivés |
| Test officiel | PC d'Ayman, [`metrics.json`](evidence/model_v001/metrics.json) et [`predictions_test.csv`](evidence/model_v001/predictions_test.csv) | 100 cycles terminaux, RMSE 17,23 / MAE 12,77 ; `model_registry` mis à jour | Un seul passage officiel après gel du modèle |
| Inférence et SQL | PC d'Ayman, runtime-lock et PR #8 | 13 096 événements test scorés, UPSERT idempotent sur replay ; checkpoint repris | Conflits détectés dans un même micro-lot ; un conflit dans un micro-lot ultérieur peut écraser la même clé SQL, mais Bronze/Silver le détectent |
| Latence | PC d'Ayman, runtime-lock | p50 18,1 s ; p95 31,6 s ; max 31,9 s | Replay en rafale, pas une mesure de régime permanent |
| Dashboard | PC d'Ayman, [capture du run complet](evidence/dashboard_full_test_run.jpg) et revue PR #4 | 164 KPI, 412 doublons, 100 moteurs sur le run complet ; arrêt puis reprise PostgreSQL sans trace d'erreur ni rechargement manuel | Le sélecteur ouvre le run le plus récent, parfois un replay partiel |
| Reprise | PC d'Ayman, runtime-lock et revues #7/#8 | Archive et inférence redémarrées sur leurs checkpoints ; reprise sans perte rapportée | Ne jamais effacer les checkpoints ni les volumes |

**Encore à joindre pour la remise finale :** preuve de validation du sujet/dataset par l'enseignante et lien Google Drive de la vidéo enregistrée (~10 min). Les journaux, captures et exports de comparaison déjà disponibles sont inventoriés dans [`docs/evidence/README.md`](evidence/README.md).
