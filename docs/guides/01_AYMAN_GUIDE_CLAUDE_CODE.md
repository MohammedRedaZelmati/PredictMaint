# Guide d'exécution — Ayman + Claude Code (dans Claude Desktop)

**Projet : SmartPredict — architecture Big Data NASA C-MAPSS FD001**  
**Machine : Windows, 16 Go RAM ; mémoire observée ~13,4/15,2 Go, soit ~1,8 Go disponibles.**  
**Périmètre exclusif : Docker/Kafka/HDFS/Spark, producteur, archive Structured Streaming, inférence, writer SQL, reprise.**  
**Hors périmètre : créer le modèle ML, modifier le schéma SQL, définir de nouvelles features ou refaire le dashboard.**

> **IMPORTANT :** ce guide est un plan exécutable, pas une preuve que le cluster ou les images fonctionnent déjà. Claude Code doit inspecter l'environnement, exécuter de vrais tests et rapporter honnêtement les blocages. L'objectif n'est pas de faire travailler Claude à la place de Reda, ni de générer artificiellement des contributions GitHub.

## A. Préparation humaine préalable : une seule fois

1. Récupérer **le même dépôt GitHub** que Reda ; inviter Reda en collaborateur. Copier le cahier d'architecture dans `docs/architecture_smartpredict.tex` (branche intégration commune) et les fichiers `00_CONTRATS_COMMUNS.md`, `01_AYMAN_GUIDE_CLAUDE_CODE.md`, `02_REDA_GUIDE_CODEX.md` dans `docs/guides/`, si vous choisissez de les suivre dans Git. Garder la fiche prof à portée des deux (ne pas publier une source interdite de diffusion).
2. Ayman configure `git config user.name` et `git config user.email` avec ses **propres** identité et email GitHub ; Reda fait de même sur son ordinateur. Vérifier `git remote -v`, `git status` et `git branch --show-current`.
3. Vérifier Windows + Docker Desktop (WSL2) + Git + place disque (viser au moins ~20 Go de marge) **sans lancer tout le cluster**.
4. Attendre la PR `feature/reda-contracts-data` **fusionnée** avant de figer les champs Kafka, features et writer ; entre-temps, utiliser uniquement des fixtures marquées « tests ».
5. Faire confirmer ou documenter la validation du sujet/dataset par l'enseignante.

**Commande Git au début de chaque lot :**

```bash
git switch main
git pull --ff-only origin main
git switch -c feature/ayman-cluster
# Remplacer le nom pour les lots suivants ; jamais de commits sur main.
```

## B. Protocole RAM obligatoire (à appliquer AVANT chaque lancement)

**Règle prioritaire : ne PAS fermer Claude Desktop.** Ayman y exécute Claude Code ; le fermer interrompt son travail. Ne pas fermer automatiquement le terminal actif, l'éditeur, Docker ou un service en cours d'écriture. Ne jamais terminer un processus Windows essentiel ou une session WSL par `taskkill` aveugle. Ne jamais déclencher `docker compose down -v`, `docker volume prune`, suppression des checkpoints ou `wsl --shutdown` sans consentement explicite, sauvegarde et vérification qu'aucune tâche active n'en dépend.

### Check RAM (PowerShell hôte, et Docker si actif)

```powershell
Get-CimInstance Win32_OperatingSystem | Select-Object @{N='AvailableGiB';E={[math]::Round($_.FreePhysicalMemory/1MB,2)}},@{N='TotalGiB';E={[math]::Round($_.TotalVisibleMemorySize/1MB,2)}}
Get-Process | Sort-Object WorkingSet64 -Descending | Select-Object -First 15 ProcessName,Id,@{N='GiB';E={[math]::Round($_.WorkingSet64/1GB,2)}}
wsl --list --verbose
docker compose ps
docker stats --no-stream
```

**Lecture :** disponibles <3,5 Go = pas de démarrage Spark/ML/cluster ; 3,5–5 Go = seulement tests légers ; 5–7 Go = un seul smoke test limité ; >=7 Go = essayer l'intégration de manière surveillée (aucune garantie). Pendant test, si <1,5 Go, swap massif, OOM ou PC non réactif : **arrêt propre** du replay/jobs, relevé des logs, puis réduction de charge. Ne jamais modifier algorithmes, features, nombre de workers ou réplication HDFS pour masquer un OOM.

### Plan de libération de RAM, **soumis à l'accord d'Ayman**

1. Demander **« as-tu sauvegardé tes documents et onglets ? »** puis proposer fermeture volontaire de ChatGPT Desktop (~1,2 Go observés), Spotify (~0,8 Go), onglets Brave inutiles (Brave total ~3,1 Go), Edge et autres Electron. Les gains sont incertains, vérifier après fermeture.
2. **Garder Claude Desktop**, outil principal, même s'il consomme ~2,2 Go. Un seul assistant IA interactif si possible.
3. Si SQL Server de Windows est inutile pour ce test, proposer son arrêt **uniquement** après vérification du nom du service, absence de session de travail et accord humain. **Ne pas arrêter PostgreSQL Docker** pendant le sink ou la démo.
4. Préférer une phase de test à la fois : Compose config → Kafka → HDFS → Spark → producer → archive → modèle disponible → inference ; stopper **uniquement** les conteneurs non requis et uniquement après vérification des dépendances.
5. Définir dans Compose, à tester et documenter, une mémoire modeste et des options JVM adaptées : Spark workers **1 core / 1 Go** de mémoire applicative chacun au départ ; autres conteneurs limités avec marge heap + overhead, sans hard limits irréalistes. Ne pas supprimer le worker 2 ou DataNode 2.
6. Facultatif : proposer `.wslconfig` sous Windows (`memory=7GB`, `processors=4`, `swap=4GB`) comme **hypothèse expérimentale**, seulement avec autorisation de modifier WSL, aucun conteneur actif, sauvegarde et essai mesuré ; `wsl --shutdown` est potentiellement destructeur pour des sessions actives et ne doit jamais être automatique.
7. Préparer les commandes **légères** et leurs fixtures avant les tests lourds. Tester 1–3 moteurs et vitesse de replay très faible ; faire le replay complet uniquement après succès des tests.

**Rapport RAM dans chaque PR lourde :** date, RAM hôte disponible avant/après, empreinte Docker, nombre de services, durée, OOM éventuel, nombre de moteurs/événements réellement traités. Ces seuils sont une **politique prudente proposée**, pas une valeur garantie ni un critère officiel de la fiche.

## C. Déroulement exact par lots

### A0 — Audit préalable (aucune modification de code)

- Lire la fiche PDF, le cahier d'architecture `.tex`/PDF, le contrat commun et les contrats Git déjà fusionnés.
- Diagnostiquer : RAM hôte, Docker/Compose, WSL, CPU, disque, versions, ports 8080/9870/29092/5432/8501, espace disque et disponibilité des images.
- Produire un tableau **besoin / présent / manquant / risque RAM / commande de test** ; noter les conflits de ports plutôt que modifier unilatéralement les interfaces.
- Relire `git status` et signaler tout changement non commité. Ne pas toucher aux dossiers de Reda.
- **Sortie :** plan opérationnel court avant génération, aucune affirmation de compatibilité non testée.

### A1 — Socle Docker / configuration (`feature/ayman-cluster`)

**Écrire uniquement :** `.env.example`, `.gitignore`, `docker-compose.yml`, `infra/Dockerfile.spark`, `infra/Dockerfile.hadoop` si nécessaire, `infra/Dockerfile.dashboard` (en attendant la liste de dépendances de Reda), `infra/hadoop/core-site.xml`, `infra/hadoop/hdfs-site.xml`, `infra/spark/spark-defaults.conf`, `infra/kafka/`, `infra/healthchecks/`, `docs/runtime-lock.md`.

**Doit fournir :**
- Kafka 3.8.1 **KRaft**, broker unique, listeners Kafka hôte/interne corrects, topic 3 partitions, réplication broker 1.
- HDFS Hadoop 3.3.6 **1 NameNode + 2 DataNodes distincts**, volumes **`hdfs_nn_data`, `hdfs_dn1_data`, `hdfs_dn2_data`**, `dfs.replication=2`, UI sur 9870, RPC sur 8020.
- Spark 3.5.3 **1 master + 2 workers distincts** (`spark-worker-1`, `spark-worker-2`) ; une image Spark commune aux jobs qui contient Python/JVM/clients Hadoop et les bibliothèques nécessaires.
- PostgreSQL 16 et Streamlit prévus dans Compose ; **Reda est propriétaire des schémas SQL, dépendances app et source du dashboard**.
- Réseau `smartpredict_net`, noms d'hôtes figés, healthchecks, persistance, variables via `.env`, aucun secret en clair commité.
- Un conteneur `spark-client` pour soumettre les jobs, et des services profilés `spark-archive` et `spark-inference` pour traitements longs (sans lancer automatiquement les deux lors du setup).
- Configuration Spark forcée pour distribuer sur `spark://spark-master:7077`; connecteur Kafka `spark-sql-kafka-0-10_2.12:3.5.3`; accès HDFS depuis drivers/workers ; PostgreSQL client disponible sur drivers/executors si writer l'utilise.

**Tests successifs à RAM contrôlée :** `docker compose config`, pull images, démarrer Kafka seul, HDFS (NN+2 DN), Spark (master+2 workers) dans la mesure possible, inspecter UIs/logs, valider noms réseau et client Spark distribué **réel**. Faire `hdfs dfs -fs hdfs://namenode:8020 -ls /` puis `hdfs fsck` sur fichier de test pour vérifier réplication 2. Si Docker image ne permet pas le service prévu, écrire un blocage précis dans `docs/runtime-lock.md`; **ne pas changer de versions sans PR**.

**Terminée si :** Compose config valide, deux DataNodes enregistrés, deux workers enregistrés, une vraie tâche Spark distribuée, heap/ressources documentés, README/commandes de lancement proposées.

### A2 — Bootstrap / chemins / commandes (`feature/ayman-cluster` ou branche dédiée)

**Écrire :** `scripts/bootstrap.sh`, `docs/runbook.md`.

- Initialiser Kafka topic `engine.measurements.v1`; créer HDFS `bronze/`, `silver/`, `gold/`, `reference/`, `models/`, `checkpoints/` **sans supprimer l'existant** ; créer et vérifier database `smartpredict` et appliquer **le `database/schema.sql` de Reda**, jamais un DDL parallèle.
- `bootstrap.sh` doit être **idempotent** : deuxième passage ne supprime pas le topic, volumes, snapshots, modèles ni checkpoints ; documenter son comportement si le service est déjà initialisé.
- Exposer des commandes `docker compose run --rm spark-client bash scripts/bootstrap.sh` exécutables depuis la racine ; ne pas oublier chemins volume/bind mounts.

**Terminée si :** bootstrap réexécutable sans perte d'état, tests HDFS/PostgreSQL de base reproductibles.

### A3 — Producteur Kafka (`feature/ayman-ingestion-archive`)

**Écrire :** `ingestion/__init__.py`, `ingestion/dataset_reader.py`, `ingestion/producer.py`, `tests/test_replay_producer.py`, `scripts/run_train_replay.sh`, `scripts/run_test_replay.sh`.

**Interface commandée :**

```bash
python -m ingestion.producer --split train --speed 1.0
python -m ingestion.producer --split test --speed 1.0 --engine-ids 1,2,3
python -m ingestion.producer --split test --speed 10 --run-id 10000000-0000-4000-8000-000000000001
```

- Parser **strictement 26 colonnes** ; les fichiers train/test sont FD001 réels, `RUL_FD001.txt` n'est **pas** transmis à Kafka ; source factice possible uniquement en tests explicitement nommés comme tels.
- Émettre `schema_version=1`, `dataset=NASA-CMAPSS-FD001`, `source_split=train|test`, `run_id`, `event_id`, `engine_id`, `cycle`, `event_ts` simulé, `published_at` réel, 3 settings et 21 capteurs.
- `event_id=fd001:<split>:<engine_id>:<cycle>` ; **clé Kafka = `fd001:<split>:<engine_id>`**. Maintenir ordre par clé, un `run_id` stable pour toute session, identifiant différent aux sessions suivantes sauf option explicite.
- Tous les nombres finis ; cycle >0, engine >0 ; messages invalides rejetés avant publication. Paramètre `--speed` : cadence documentée et contrôlée sans casser l'ordre ; compteur envoyé/rejeté et temps écoulé en fin de run.
- Ne jamais ajouter `target_rul`, maximum de cycle ni vérité de test dans le JSON.

**Tests :** JSON Schema Reda, comptes du dataset source réel, 100 train/100 test, 20 631/13 096 lignes si fichiers complets, clé Kafka identique pour un moteur, event_id stable entre replays, filtre moteurs et reprise voulue avec même UUID. **Attendre fusion des contrats Reda avant validation réelle.**

### A4 — Voie froide Kafka → HDFS Parquet (`feature/ayman-ingestion-archive`)

**Écrire :** `streaming/__init__.py`, `streaming/archive.py`, tests streaming/cold ciblés.

- Session Spark **cluster** (`spark://spark-master:7077`), lecture `readStream.format('kafka')`, topic V1, parsing JSON depuis `contracts/event.schema.json` V1 ou schéma Spark strict synchronisé avec lui.
- Valider et conserver les mesures et l'enveloppe ; tracer Kafka offset/partition/timestamp d'ingestion ; **aucun RUL** calculé dans le job cold.
- `.writeStream.format('parquet')` vers `hdfs://namenode:8020/smartpredict/bronze/engine_events/`, partitions de données `source_split`, checkpoint **exclusif** `hdfs://namenode:8020/smartpredict/checkpoints/archive_v1/`.
- `startingOffsets=earliest` **seulement au premier démarrage sans checkpoint**. Bronze append ; duplication inter-replay autorisée, Reda nettoiera au niveau Silver.
- Mettre en place logs d'erreurs sans fuite de secrets, prise en charge signal stop et tests de reprise avec checkpoint.

**Barrière B1 :** ne déclarer A4 réussi que si un vrai replay Kafka a produit du Parquet lisible via Spark SQL depuis HDFS. Transmettre à Reda le schéma exact, 5 événements anonymisés/factuels et une commande de comptage logique pour son lot R2.

### A5 — Inférence à chaud (`feature/ayman-online-inference`)

**Attendre** PR Reda modèle `v001` fusionnée et `database/schema.sql` disponible. Sans cela, coder et tester les interfaces sur fixture étiquetée *test*, mais **pas de démonstration ML finale**.

**Écrire :** `streaming/inference.py`, `streaming/postgres_writer.py`, `tests/test_streaming_sink.py`.

- Au **démarrage**, vérifier `schema_version=1`, `model_version=v001`, `feature_schema_hash` strict et URI ; `PipelineModel.load('hdfs://namenode:8020/smartpredict/models/rul_pipeline/v001/')` ; erreur claire si absent/incompatible.
- Kafka `latest` **sans checkpoint existant**, démarrer avant replay test, puis checkpoint `hdfs://namenode:8020/smartpredict/checkpoints/inference_v1/`; reprendre depuis le checkpoint ensuite.
- Dans `foreachBatch`, valider l'événement puis `model.transform(batch_df)` avec les **25 features exactes**, Spark distribué ; jamais de `toPandas()` non borné sur le driver, jamais d'heuristique substitutive ni de `fit()` dans le flux chaud.
- Borner `prediction` à `[0,125]` ; statut : <=20 CRITICAL, <=50 WARNING, >50 HEALTHY ; écrire `predicted_at` au moment du scoring.
- Écrire **exactement** le schéma PostgreSQL de Reda en UPSERT `(run_id, event_id, model_version)` ; `foreachPartition` / transactions et gestion de retry idempotente, credential depuis environnement ; exception DB doit faire échouer le micro-batch pour retry.
- Signaler erreur Kafka indisponible / SQL indisponible / mauvais modèle / événement invalide de manière testable. Ne jamais *masquer* l'absence de prédiction par une valeur factice.

**Barrière B2 :** modèle réellement chargé depuis HDFS dans le client/worker, requête Kafka active avant replay, traces de `predictions` en base, version modèle correcte, 2 exécutions du même événement avec même `(run_id,event_id,model_version)` → **une seule** ligne.

### A6 — Robustesse et reprise (Ayman avec vérification par Reda)

- Tester arrêts/reprises `spark-archive` puis `spark-inference` **avec mêmes checkpoints**, Kafka rétention suffisante, N événements avant / après, absence de doublons SQL pour la même session.
- Tester publication avec erreur de schéma ; modèle manquant/incompatible ; indisponibilité temporaire SQL ; Kafka indisponible. Montrer les logs/erreurs utiles, pas seulement les exceptions brutes.
- Tester `spark.sparkContext.master` et tâches sur **deux workers**, pas `local[*]`; 2 DataNodes + réplication 2 ; `fsck` des données archivées.
- Mesurer latence pour un `run_id` de démo, p50/p95/max sur données réelles stockées ; documenter contexte et éventuels outliers.
- Reproduire sur PC Reda **après clone** avec aide d'Ayman. Ne pas écraser volumes/DB si Reda possède des données existantes.

### A7 — Documentation, revue et Git

- Mettre à jour `docs/runbook.md`, `docs/runtime-lock.md`, section infra/streaming du README **uniquement en branche intégration commune**, `docs/contributions.md` en revue croisée.
- Pousser des commits substantifs : `feat(infra): add 2-worker Spark and 2-datanode HDFS compose`, `feat(ingestion): replay FD001 rows to Kafka`, `feat(streaming): archive Kafka events to HDFS Parquet`, `feat(inference): score Spark ML model into Postgres`.
- Ouvrir PR, attendre revue Reda pour interfaces communes. **Ne pas attribuer les commits de Reda à Ayman ; ne pas falsifier dates, tests, captures, résultats ou scores.**

## D. Tableau des critères d'arrêt et de livraison

| Porte | Condition minimale | En cas d'échec |
|---|---|---|
| RAM | Mesure préalable et mémoire suffisante | Reporter test lourd, proposer libération volontaire |
| Contrats | `event.schema.json`, `feature_columns.json` et `schema.sql` de Reda | Attendre PR ou tests isolés, ne pas modifier |
| Infra | 1 Spark master, 2 workers, 1 NN, 2 DN, réplication HDFS2 | Diagnostic sans supprimer noeuds |
| Producer | JSON/clé/IDs conformes, test réel Kafka | Corriger seulement fichiers Ayman |
| Cold | Parquet HDFS + checkpoint lisibles | Journaliser Kafka/connecteur/HDFS, reprendre sans effacement |
| Modèle | PipelineModel v001 + manifeste cohérents | Bloquer hot ; pas de prédiction factice |
| Hot | Job Kafka→Spark ML→SQL réel, UPSERT idempotent | Échec explicite du micro-batch/retry |
| Reprise | Offsets checkpoints préservés et SQL non doublé | Capturer bug, ne jamais `down -v` |
| Collaboration | Branches Ayman, PRs, revue Reda | Ne pas pusher directement main |

## E. Mise en service et ordre d'intégration

1. Revue des contrats avec Reda et fusion de `feature/reda-contracts-data`.
2. `feature/ayman-cluster` : Docker/cluster/Kafka/HDFS et `bootstrap.sh` prouvés.
3. `feature/ayman-ingestion-archive` : producer et archive ; envoyer preuve Bronze à Reda.
4. Attendre `feature/reda-batch-kpis` puis `feature/reda-sparkml` (v001).
5. `feature/ayman-online-inference` : inference et writer SQL prouvés.
6. Intégration finale **avec** Reda : dashboard, reprises, README, rapport <=20 pages et vidéo 10 minutes.

**Commandes de démonstration à implémenter, sans prétendre qu'elles existent déjà :**

```bash
docker compose config
docker compose up -d namenode datanode-1 datanode-2 spark-master spark-worker-1 spark-worker-2 kafka postgres
docker compose run --rm spark-client bash scripts/bootstrap.sh
docker compose --profile jobs up -d spark-archive
docker compose run --rm spark-client bash scripts/run_train_replay.sh
docker compose run --rm spark-client bash scripts/run_batch.sh
docker compose --profile jobs up -d spark-inference
docker compose run --rm spark-client bash scripts/run_test_replay.sh
```

Ne jamais démarrer le replay de test **avant** la requête d'inférence sur un topic avec offset initial `latest`. Après une interruption, reprendre *avec* les checkpoints. Si la machine n'a pas de marge RAM, fractionner les opérations par phase, mais le pipeline complet doit être démontré au moins une fois sur le PC le plus stable.

## F. Rapport obligatoire de Claude Code après chaque lot

Claude Code doit terminer sa réponse de chaque phase avec les **10 rubriques suivantes** :

1. Lot / branche / commit(s) effectifs (ne pas annoncer commit si non fait).
2. Fichiers créés/modifiés (tous dans le domaine Ayman).
3. Contrats lus et versions utilisées.
4. Commandes **réellement** exécutées, avec résultat ou échec.
5. Preuves : extraits logs, captures, comptages, état cluster.
6. RAM disponible avant/après, conteneurs actifs, dépassements observés.
7. Comportement reprise/rollback ; aucune perte de données.
8. Blocages liés à Reda et interface attendue.
9. Proposition de message de commit/PR et révision requise.
10. Prochaine étape autorisée, avec justification de la barrière franchie.

**Ne jamais continuer silencieusement vers le lot suivant si la dépendance externe ou le test essentiel du lot actuel est bloqué.** Les défauts sont consignés, pas dissimulés.

## G. Prompt prêt à coller

Le prompt exécutable, à coller dans **Claude Code au sein de Claude Desktop**, est aussi fourni en fichier autonome : `PROMPT_AYMAN_CLAUDE_CODE.txt`. Il demande à l'assistant de lire ce guide et le contrat partagé depuis la racine réelle du dépôt, d'effectuer A0 puis A1 et de poursuivre uniquement après vérification des critères et barrières.
