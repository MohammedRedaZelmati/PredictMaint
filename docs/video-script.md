# Conducteur de démonstration (~10 minutes)

Ce fichier est un **script de tournage**, pas une vidéo enregistrée. La fiche enseignante demande de déposer la vidéo sur Google Drive. Le cluster en fonctionnement et les captures doivent être enregistrés sur la machine qui exécute réellement la démo ; les chiffres affichés doivent être ceux du run filmé.

| Temps | Écran / action | Message à dire |
| --- | --- | --- |
| 0:00-0:50 | Titre, cas d'usage et fiche FD001 | 100 moteurs train, 100 moteurs test ; RUL en cycles plafonnés à 125, une seule tâche de régression. |
| 0:50-2:00 | Schéma du README puis `docker compose ps` | Kafka, deux voies Spark, HDFS répliqué, PostgreSQL, Streamlit ; montrer 2 workers et 2 DataNodes. |
| 2:00-3:00 | Topic Kafka et replay train, logs de l'archive | L'événement contient 24 mesures, pas de vrai RUL ; la voie froide écrit le Bronze Parquet et garde `archive_v1`. |
| 3:00-4:00 | Silver, KPI et PostgreSQL | Montrer 20 631 cycles train, les trois bandes RUL 2 100 / 3 000 / 15 531, capteurs variables et 412 doublons de replay test identifiés comme doublons. |
| 4:00-5:30 | `docs/evidence/model_v001/comparison.csv`, manifeste v001 et métriques | Expliquer 80/20 par moteur, trois snapshots causaux, B0/LR/RF/8 GBT, sélection GBT sur RMSE validation, test officiel séparé ; ne pas masquer la divergence de LR. |
| 5:30-7:30 | Démarrer l'inférence, replay test, dashboard | Montrer le hash des 25 features, les lignes SQL et une trajectoire moteur ; choisir explicitement le run complet de 100 moteurs si le dernier run est un smoke test. |
| 7:30-8:30 | Checkpoint et reprise | Arrêter/reprendre un job sans supprimer le checkpoint ; montrer les comptes avant/après, sans relancer un entraînement ou l'évaluation officielle. |
| 8:30-9:30 | Résultats et limites | RMSE test 17,23 / MAE 12,77 rapportés sur le run officiel ; latence p50 18,1 s / p95 31,6 s en rafale, non steady-state. |
| 9:30-10:00 | Contributions et dépôt | Reda : données, SQL, ML, dashboard ; Ayman : cluster, Kafka, HDFS, streaming ; revues croisées et limites. |

Avant l'enregistrement : vérifier RAM, espace disque, URL du dashboard, état des deux workers et DataNodes, source des fichiers FD001 et absence de secrets à l'écran. Filmer les résultats du run actif ; ne pas présenter une maquette ou une capture ancienne comme une exécution en direct.
