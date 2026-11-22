# SmartPredict — Contrats communs immuables (V1.0)

**Destinataires : Ayman / Claude Code et Reda / Codex.**  
**Statut : spécification de travail, non implémentation testée.**  
**Sources de référence :** `Fiche_Projet_Pipeline_Big_Data_Version 2026.pdf` (fiche enseignante) et `SmartPredict_Architecture_Binome.pdf` / `.tex` (cahier d'architecture interne).  
**Principe :** les éléments ci-dessous reprennent les décisions figées dans le cahier ; un problème technique est consigné et soumis à une PR conjointe, jamais corrigé par une substitution silencieuse.

## 0. Hiérarchie et décisions communes

1. **Fiche de la professeure :** exigences académiques obligatoires. Le binôme à deux est considéré comme accepté selon le retour de l'équipe.
2. **Cahier d'architecture SmartPredict V1.0 :** choix techniques et frontières entre contributions.
3. **Le présent contrat :** résumé exploitable et vérifiable, subordonné au cahier si une différence est relevée.
4. **`contracts/` versionné dans Git :** implémentation contrôlable des schémas convenus ; modification uniquement après accord des deux.
5. **Code source :** ne fait jamais autorité sur les contrats par simple fait d'exister.

**Non négociable :** Spark master + deux workers distincts, HDFS NameNode + deux DataNodes et réplication 2, Kafka comme *source* de Structured Streaming, archivage Parquet HDFS, analyses Spark SQL, trois algorithmes **Spark ML** comparés + tuning, modèle/pipeline sauvegardé et réutilisé en streaming, base PostgreSQL, reprise avec checkpoints, dépôt reproductible, vidéo ~10 minutes, rapport final <=20 pages. Le dashboard Streamlit est le bonus retenu dans le cahier.

**Sujet :** maintenance prédictive de moteurs NASA C-MAPSS **FD001**, **une seule tâche de régression** : RUL (*remaining useful life*) en **cycles**, plafonné à 125. Horodatages Kafka = événements **rejoués/simulés**, jamais des timestamps réels NASA. La validation préalable du sujet et du jeu de données par l'enseignante doit être traçable.

## 1. Versions, services et réseau

| Élément | Contrat V1 | Responsable |
|---|---|---|
| Python des jobs | Python embarqué dans les images Spark, version réelle documentée | Ayman vérifie |
| Spark / PySpark | 3.5.3, Scala 2.12, JDK 17 | Ayman |
| Spark Docker | `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` (disponibilité à vérifier) | Ayman |
| Hadoop / HDFS | 3.3.6 ; `apache/hadoop:3.3.6` (roles/entrée à vérifier) | Ayman |
| Kafka | 3.8.1 en KRaft, `apache/kafka:3.8.1` | Ayman |
| Kafka Spark connector | `spark-sql-kafka-0-10_2.12:3.5.3` | Ayman |
| PostgreSQL | 16.x, version mineure / digest à verrouiller après test | Ayman compose / Reda SQL |
| Streamlit | 1.39.x ou autre micro-version validée/consignée | Reda |
| Spark master | `spark://spark-master:7077` ; UI hôte `localhost:8080` | Ayman |
| Spark workers | `spark-worker-1` et `spark-worker-2`, chacun conteneur propre | Ayman |
| HDFS | `hdfs://namenode:8020` ; UI hôte `localhost:9870` | Ayman |
| DataNodes | `datanode-1` et `datanode-2`, volumes distincts | Ayman |
| Kafka interne / hôte | `kafka:9092` / `localhost:29092` | Ayman |
| PostgreSQL interne / hôte | `postgres:5432` / `localhost:5432` si libre | Les deux |
| Dashboard interne / hôte | `dashboard:8501` / `localhost:8501` | Reda |
| Réseau Docker | `smartpredict_net` | Ayman |

Ne pas promettre que les tags fonctionnent avant `docker compose config`, `docker compose pull`, contrôle des services et journal de versions dans `docs/runtime-lock.md`. **Aucun passage vers Spark local, scikit-learn, XGBoost, InfluxDB, Grafana, MinIO ou un seul worker/DataNode.**

## 2. Topic, clé, payload V1

- Topic : **`engine.measurements.v1`** ; **3 partitions**, **réplication Kafka 1** (broker unique). À ne pas confondre avec réplication **HDFS 2**.
- Clé Kafka : `fd001:<source_split>:<engine_id>`.
- `event_id` déterministe : `fd001:<source_split>:<engine_id>:<cycle>` **sans** `run_id`.
- `run_id` : UUID neuf par session de replay, stable *pendant* cette session ; doit pouvoir être explicitement réutilisé lors d'une reprise voulue.
- Schéma `schema_version: 1`; `dataset: "NASA-CMAPSS-FD001"`.
- `source_split`: `train` ou `test`; ID des moteurs séparés entre ces espaces.
- `engine_id`, `cycle`: entiers >=1.
- `event_ts`: ISO 8601 UTC **simulé** (origine UTC fixe pour la session + cycle × 60 s si cette convention est choisie) ; `published_at`: ISO UTC réel de publication.
- Champs numériques **obligatoires** : `op_setting_1`, `op_setting_2`, `op_setting_3` et `sensor_01` à `sensor_21` (21 capteurs), tous finis.
- **AUCUN** `target_rul`, RUL vrai ou information de panne dans les événements Kafka.
- Un événement représente **une ligne, un moteur, un cycle**, et le producteur conserve l'ordre **par moteur** en utilisant la même clé.
- Les erreurs de schéma sont comptées/loggées ; les messages invalides ne contaminent ni Silver ni l'inférence.

Exemple de **payload COMPLET synthétique**, destiné exclusivement aux tests de schéma (ces chiffres ne sont **pas** des lectures NASA) :

```json
{
  "schema_version": 1,
  "dataset": "NASA-CMAPSS-FD001",
  "source_split": "test",
  "run_id": "10000000-0000-4000-8000-000000000001",
  "event_id": "fd001:test:1:100",
  "engine_id": 1,
  "cycle": 100,
  "event_ts": "2026-11-21T10:00:00Z",
  "published_at": "2026-11-21T17:00:00Z",
  "op_setting_1": 0.0,
  "op_setting_2": 0.0,
  "op_setting_3": 100.0,
  "sensor_01": 1.0,
  "sensor_02": 2.0,
  "sensor_03": 3.0,
  "sensor_04": 4.0,
  "sensor_05": 5.0,
  "sensor_06": 6.0,
  "sensor_07": 7.0,
  "sensor_08": 8.0,
  "sensor_09": 9.0,
  "sensor_10": 10.0,
  "sensor_11": 11.0,
  "sensor_12": 12.0,
  "sensor_13": 13.0,
  "sensor_14": 14.0,
  "sensor_15": 15.0,
  "sensor_16": 16.0,
  "sensor_17": 17.0,
  "sensor_18": 18.0,
  "sensor_19": 19.0,
  "sensor_20": 20.0,
  "sensor_21": 21.0
}
```

Le producteur réel **remplace toutes ces valeurs fictives par les mesures NASA** et n'envoie jamais de données étiquetées comme réelles lorsqu'elles sont synthétiques.

## 3. HDFS : lieux exacts

```text
hdfs://namenode:8020/smartpredict/
  bronze/engine_events/          # Parquet, append depuis Kafka
  silver/train_labeled/          # 100 moteurs train, RUL train calculé
  gold/batch_kpis/               # KPI historiques Spark SQL
  reference/test_rul/            # 100 cibles test officielles
  models/rul_pipeline/v001/      # PipelineModel Spark ML
  models/metadata/v001/          # manifest.json, metrics.json
  checkpoints/archive_v1/       # Checkpoint exclusif voie froide
  checkpoints/inference_v1/     # Checkpoint exclusif voie chaude
```

`fs.defaultFS=hdfs://namenode:8020`, `dfs.replication=2`; volumes nommés : `hdfs_nn_data`, `hdfs_dn1_data`, `hdfs_dn2_data`. Bronze = *append* des événements valides (replays possibles), Silver = **une ligne logique par** `(source_split, engine_id, cycle)` après vérification des conflits de valeur et déduplication ; l'entraînement ne doit jamais multiplier les observations parce qu'on a rejoué Kafka. Les checkpoints et les données ne doivent pas être supprimés pour « réparer » un bug.

## 4. Data science : caractéristiques et méthode

FD001 : `train_FD001.txt` = **20 631 mesures de 100 moteurs**, `test_FD001.txt` = **13 096 mesures de 100 autres moteurs**, `RUL_FD001.txt` = **100 cibles terminales** ; nombres à **vérifier sur la source utilisée**. Chaque ligne train/test contient **26 colonnes** (id, cycle, 3 settings, 21 capteurs).

- Train : `target_rul = min(125, max_train_cycle(engine_id) - cycle)`.
- Test terminal : `target_rul = min(125, official_RUL_for_engine)` ; **NE JAMAIS** mettre zéro au dernier cycle test.
- Test historique si nécessaire : `min(125, official_RUL + last_observed_test_cycle - current_cycle)`.
- Variables ML (ordre **strict** et fixe, 25 doubles) : `cycle`, `op_setting_1`, `op_setting_2`, `op_setting_3`, `sensor_01`...`sensor_21`.
- Exclus du vecteur : `engine_id`, `target_rul`, `run_id`, `event_id`, `source_split`, timestamps, maximum futur de cycle. **Pas de rolling features en V1.**
- Préparation sauvegardée : `pyspark.ml.Pipeline` : **`Imputer` → `VectorAssembler` → régression** ; `PipelineModel` inférable directement depuis HDFS.
- Baseline B0 : médiane RUL train ; candidats M1 : `LinearRegression`, M2 : `RandomForestRegressor`, M3 : `GBTRegressor` **Spark ML uniquement**.
- Train/validation par moteur : 80 moteurs fit et 20 validation déterministes avec seed 42, jamais découpage aléatoire de lignes.
- Snapshots validation causaux : cycles `floor(0.60 T)`, `floor(0.75 T)`, `floor(0.90 T)` pour chaque moteur validation. Critère : **RMSE sur snapshots validation**, MAE aussi. Test NASA officiel **une seule fois** après sélection et refit.
- GBT tuning **8 configurations** : `maxDepth` 3 ou 5 × `maxIter` 30 ou 60 × `stepSize` 0.05 ou 0.10. **Exécution séquentielle si RAM limitée** ; ne pas enlever des configurations sans le déclarer comme test non terminé.
- Final : refit du candidat sélectionné sur 100 moteurs train ; chemin HDFS version `v001`, sortie colonne `prediction` double ; sauvegarde **du pipeline entier**.
- Artefacts : `manifest.json`, `metrics.json`, `comparison.csv`, `predictions_test.csv` (100 lignes terminales), plus documentation des limites et qualité.

Manifeste `hdfs://namenode:8020/smartpredict/models/metadata/v001/manifest.json` :

```json
{
  "schema_version": 1,
  "model_version": "v001",
  "model_hdfs_uri": "hdfs://namenode:8020/smartpredict/models/rul_pipeline/v001/",
  "algorithm": "<real-selected-Spark-ML-regressor>",
  "target_name": "target_rul",
  "target_cap_cycles": 125,
  "feature_columns_uri": "contracts/feature_columns.json",
  "feature_schema_hash": "<real-canonical-features-sha256>",
  "split_seed": 42,
  "validation_metric": "rmse",
  "trained_at_utc": "<real-ISO-UTC>",
  "train_dataset_sha256": "<real-sha256>"
}
```

`contracts/feature_columns.json`: objet `{"schema_version":1,"features":[...25 éléments...]}`. Choisir explicitement la sérialisation canonique de **la liste** pour son hash : UTF-8, `json.dumps(features, ensure_ascii=False, separators=(',', ':'))`, même code de calcul côté Ayman et Reda ; les tests garantissent une égalité exacte.

## 5. SQL PostgreSQL et santé

**Schéma `database/schema.sql` créé par Reda ; Ayman l'utilise sans le réécrire.**

```sql
CREATE TABLE IF NOT EXISTS predictions (
  run_id UUID NOT NULL,
  event_id TEXT NOT NULL,
  model_version TEXT NOT NULL,
  source_split TEXT NOT NULL CHECK (source_split IN ('train','test')),
  engine_id INTEGER NOT NULL CHECK (engine_id > 0),
  cycle INTEGER NOT NULL CHECK (cycle > 0),
  event_ts TIMESTAMPTZ NOT NULL,
  published_at TIMESTAMPTZ NOT NULL,
  predicted_at TIMESTAMPTZ NOT NULL,
  predicted_rul_cycles DOUBLE PRECISION NOT NULL,
  health_status TEXT NOT NULL CHECK (health_status IN ('HEALTHY','WARNING','CRITICAL')),
  PRIMARY KEY (run_id, event_id, model_version)
);
CREATE INDEX IF NOT EXISTS idx_prediction_latest
  ON predictions(run_id, engine_id, cycle DESC);
CREATE INDEX IF NOT EXISTS idx_prediction_time
  ON predictions(predicted_at DESC);

CREATE TABLE IF NOT EXISTS batch_kpis (
  snapshot_id TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  dimension_name TEXT NOT NULL DEFAULT 'all',
  dimension_value TEXT NOT NULL DEFAULT 'all',
  metric_value DOUBLE PRECISION NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (snapshot_id, metric_name, dimension_name, dimension_value)
);
CREATE TABLE IF NOT EXISTS model_registry (
  model_version TEXT PRIMARY KEY,
  model_hdfs_uri TEXT NOT NULL,
  feature_schema_hash TEXT NOT NULL,
  algorithm TEXT NOT NULL,
  validation_rmse DOUBLE PRECISION,
  test_rmse DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`predicted_rul_cycles = max(0, min(125, prediction))` ; `CRITICAL` si <=20, `WARNING` si >20 et <=50, `HEALTHY` si >50. Writer de prédictions **UPSERT** sur `(run_id, event_id, model_version)` ; KPI UPSERT par clé primaire. Dashboard : par défaut **dernier `run_id` test**, sélecteur de run, rafraîchissement **5 s**, courbe RUL/cycle, état courant/moteur, KPI, version, horodatages, latence indicative `predicted_at - published_at` (secondes).

## 6. Commandes / modules immuables

```text
python -m ingestion.producer --split train|test --speed <float> [--run-id <uuid>] [--engine-ids 1,2,3]
spark-submit streaming/archive.py
spark-submit batch/train_models.py --version v001
spark-submit streaming/inference.py --model-version v001
```

`streaming/archive.py` : Kafka → Parquet HDFS + checkpoint `archive_v1`. `batch/prepare.py` : Bronze (train) → Silver + RUL sans fuite. `batch/kpis.py` + `batch/publish_kpis.py`: HDFS → Gold + SQL. `batch/train_models.py` et `batch/evaluate.py` : modèle HDFS et test officiel. `streaming/inference.py` : `PipelineModel.load` au démarrage, Kafka → prédictions distribuées → PostgreSQL + checkpoint `inference_v1`. `dashboard/app.py` : lecture SQL seulement.

Kafka offsets : cold `earliest` à la première exécution sans checkpoint ; hot `latest` à la première exécution sans checkpoint **et avant** replay de test ; un checkpoint existant prévaut sur `startingOffsets`. Ne jamais faire fonctionner deux instances avec un même checkpoint, ni arrêter une requête en effaçant son état. La reprise doit être démontrée, jamais seulement présumée.

## 7. Fichiers sous propriété exclusive

**Ayman :** `.env.example`, `.gitignore`, `docker-compose.yml`, `infra/`, `ingestion/`, `streaming/`, `scripts/bootstrap.sh`, `scripts/run_train_replay.sh`, `scripts/run_test_replay.sh`, `tests/test_replay_producer.py`, `tests/test_streaming_sink.py`, `docs/runtime-lock.md`, `docs/runbook.md`.

**Reda :** `requirements.txt`, `contracts/`, `batch/`, `database/`, `dashboard/`, `scripts/run_batch.sh`, `scripts/run_evaluate.sh`, `tests/test_event_contract.py`, `tests/test_rul_targets.py`, `tests/test_ml_splits.py`, `docs/decisions.md`, `docs/data-card.md`.

**Commun et relu par les deux :** `README.md`, `docs/architecture_smartpredict.tex`, `docs/validation-matrix.md`, `docs/contributions.md`, `tests/test_integration_e2e.py`, `scripts/smoke_test.sh`, `docs/report/`, vidéo. `infra/Dockerfile.dashboard` **appartient à Ayman** avec les exigences de dépendances transmises par Reda.

Branches : `feature/reda-contracts-data`, `feature/ayman-cluster`, `feature/ayman-ingestion-archive`, `feature/reda-batch-kpis`, `feature/reda-sparkml`, `feature/ayman-online-inference`, `feature/reda-dashboard`, puis `integration/e2e-final`. Chacun pousse ses **vrais commits** depuis son **propre compte** ; PR relue par l'autre ; éviter squash unique qui masque les auteurs ; pas de force-push/main direct/commits factices.

**Ordre de dépendances :** PR Reda contrats+SQL → fusion → producer et writer Ayman alignés. PR Ayman infra+archive → Bronze réel → Reda Silver/ML. PR Reda modèle `v001` → fusion → Ayman inférence réelle. UI Reda peut utiliser une fixture **explicitement fictive** en attendant le writer, pas pour la preuve finale.

## 8. Sécurité mémoire commune — ordinateurs Windows de 16 Go

**Ayman :** Claude Code fonctionne dans **Claude Desktop**, qui doit **rester ouvert**. RAM observée chez Ayman : environ **13,4 Go / 15,2 Go**, avec **1,8 Go disponibles** au moment de la mesure ; **ne pas lancer le cluster complet dans cet état**. Reda dispose également de 16 Go, mais sa disponibilité réelle doit être vérifiée.

### Avant *chaque* commande lourde : contrôles mesurés

```powershell
# Mémoire physique disponible (GiB approximatifs), dans PowerShell
Get-CimInstance Win32_OperatingSystem | Select-Object @{Name='AvailableGiB';Expression={[math]::Round($_.FreePhysicalMemory/1MB,2)}},@{Name='TotalGiB';Expression={[math]::Round($_.TotalVisibleMemorySize/1MB,2)}}
# Analyse des gros processus (sans en arrêter aucun)
Get-Process | Sort-Object WorkingSet64 -Descending | Select-Object -First 15 ProcessName,Id,@{Name='WorkingSetGiB';Expression={[math]::Round($_.WorkingSet64/1GB,2)}}
# Services Docker / empreintes du projet
wsl --list --verbose
docker compose ps
docker stats --no-stream
```

Les chiffres de `Get-Process` par processus ne s'additionnent pas nécessairement à la RAM physique totale (mémoire partagée, cache). Ne pas utiliser un unique « free » pour conclure que la machine est hors-jeu : mesurer la mémoire **disponible**, l'état WSL et les conteneurs.

### Règles de décision opérationnelles (heuristiques conservatrices, non exigences de la professeure)

- **< 3,5 Go disponibles** : **interdiction de démarrer un nouveau job Spark, entraînement GBT, replay massif ou cluster complet** ; faire seulement édition/tests Python légers et préparer un plan de libération RAM.
- **3,5 à < 5 Go** : tests ciblés sans nouvelle charge lourde (config Compose, schémas, unitaires), arrêter les services Docker non nécessaires **avec vérification**.
- **5 à < 7 Go** : autoriser **un seul** smoke test limité et instrumenté, jamais tuning complet et hot+cold+replay en parallèle sans nouvelle vérification.
- **>= 7 Go** : les tests d'intégration deviennent envisageables, **pas garantis** ; vérifier aussi le budget WSL/Docker, disques, CPU, outils ouverts et faire un premier essai contrôlé.
- **Si mémoire disponible < 1,5 Go pendant le test, swap intense, OOM ou freeze** : arrêter **proprement** le producer/job en cours, conserver logs et checkpoints, éviter toute action destructive, puis revoir le budget. Ne pas poursuivre coûte que coûte.

**Toujours garder :** Claude Desktop pour Ayman, l'éditeur nécessaire, Git et Docker/services indispensables au job lancé. **Ne jamais terminer brutalement** `svchost`, Defender, Explorer, processus système, WSL, PostgreSQL en cours d'écriture, NameNode/DataNodes ou processus `java` sans identifier sa fonction. **Ne jamais fermer une application de l'utilisateur sans demande/accord explicite**, ni perdre des onglets ou du travail non enregistré.

Avant gros lancement, l'assistant doit proposer à l'utilisateur de **sauvegarder puis fermer volontairement** (si non nécessaires) ChatGPT Desktop, Spotify, Edge, onglets Brave/WhatsApp, applications Electron et serveurs locaux SQL **non utilisés**. Claude Desktop **n'est pas** à fermer chez Ayman. Le simple fait de suggérer la fermeture n'autorise pas l'assistant à l'exécuter. Pour des services SQL, vérifier le service exact et les dépendances avant toute proposition d'arrêt.

**Mode allégé sans suppression d'exigence :** développer les modules séparément ; démarrer `Kafka/HDFS/Spark` seulement pour les essais correspondants, 2 workers/2 DataNodes lors des preuves réglementaires, Spark worker de départ **1 cœur / ~1 Go** sous réserve de JVM overhead et mesures, **un job ML/tuning à la fois**, test sur 1–3 moteurs avant replay complet. Exemple de configuration facultative WSL2 **à proposer, jamais imposer automatiquement** : `%USERPROFILE%\.wslconfig` avec `[wsl2] memory=7GB, processors=4, swap=4GB`, nécessitant arrêt **contrôlé** de WSL pour prise d'effet. Une limite RAM trop basse peut causer des OOM ; ne pas changer si Docker utilise déjà WSL pour un service actif. Réexaminer après mesure réelle.

Les gros tests **peuvent être centralisés** sur le PC des deux qui possède le plus de mémoire disponible ; l'autre contribue par ses véritables développements, commits, tests légers et revue. Aucun faux log/score ni affirmation « testé » sans exécution réelle.

## 9. Porte de sortie et preuve finale

Un lot n'est « terminé » qu'avec : fichiers effectifs, test exécuté (ou **blocker déclaré**), commande de reproduction, capture/log ciblé sans secrets, branche/PR et auteur Git réel, mise à jour des documents correspondants. Les tests d'intégration finaux comprennent : 2 workers, 2 DataNodes, réplication HDFS 2, Kafka payload valide, 20 631 lignes train **logiques** en Bronze/Silver si dataset complet, KPI Gold/SQL, 3 modèles + 8 tuning + modèle v001, 100 labels test corrects, streaming chaud → PostgreSQL, dashboard 5 secondes, reprise archive+inférence, clonage et reproduction sur l'autre poste. Rapporter clairement tout test non réalisé et ses limites.
