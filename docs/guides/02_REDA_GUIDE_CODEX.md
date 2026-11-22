# Guide d'exécution — Reda + Codex

**Projet : SmartPredict — pipeline Big Data NASA C-MAPSS FD001**  
**Machine : Windows, 16 Go de RAM (disponibilité réelle à mesurer).**  
**Périmètre exclusif : contrats, données FD001, Silver/Gold Spark SQL, trois Spark ML + tuning, évaluation, PostgreSQL (DDL) et Streamlit.**  
**Hors périmètre : réécrire Docker/Kafka/HDFS/Spark d'Ayman, producteur, archivage et inférence en streaming.**

> **IMPORTANT :** Codex doit d'abord lire la fiche et le cahier d'architecture, puis développer sur sa propre branche du même dépôt GitHub que celui d'Ayman. Aucun test, modèle, résultat, publication ou commit ne peut être déclaré réussi sans preuve. La fiche exige **3 techniques ML** + tuning et un **modèle Spark ML** ; un notebook scikit-learn/XGBoost ne remplit pas cette obligation.

## A. Préparation humaine, Git et ordre des dépendances

1. Cloner le **même** repo que Ayman ; configurer `user.name="Reda"` et `user.email` lié au compte GitHub de Reda. Ne jamais coder dans une copie privée puis transférer une archive en prétendant à une collaboration Git.
2. Obtenir à la racine `docs/architecture_smartpredict.tex`, `docs/guides/00_CONTRATS_COMMUNS.md`, ce guide et le PDF/texte de la fiche enseignante. La source prof domine en cas de contradiction et le cahier fixe les conventions V1.
3. Si les fichiers de référence ne sont pas disponibles, **arrêter la génération d'interface** jusqu'à mise à disposition ; ne pas réinventer `event.schema.json`, variables ou SQL.
4. La **première PR de Reda** est `feature/reda-contracts-data` avec `contracts/`, `database/schema.sql`, data-card et tests contractuels. Ayman s'appuie ensuite sur cette PR pour Kafka/SQL.
5. Les travaux Silver/ML requièrent un **Bronze HDFS réel** archivé par Ayman, sauf tests unitaires utilisant des fixtures clairement marquées. Il est interdit de présenter un modèle entraîné sur un CSV local comme preuve finale de « Spark cluster + HDFS ».

```bash
git switch main
git pull --ff-only origin main
git switch -c feature/reda-contracts-data
git config user.name "Reda"
git config user.email "<email-GitHub-personnel>"
git status
```

**Principe Git :** Reda pousse ses développements et ouvre ses PR ; Ayman fait de même pour les siens. Revue mutuelle des contrats et des PR, pas de push direct sur `main`, pas de force-push, pas de squash unique par défaut, pas d'usurpation de commits ni de faux historiques.

## B. Gestion mémoire Codex (Windows 16 Go)

Reda peut être la machine d'intégration principale **uniquement si sa RAM effectivement disponible le permet**. Ne pas supposer que 16 Go installés = 16 Go disponibles. Codex doit mesurer *avant* de lancer Spark, GBT et Docker, et ne doit jamais fermer une application ni arrêter un service non nécessaire sans demander à Reda, l'avertir des données en cours et obtenir son accord.

```powershell
Get-CimInstance Win32_OperatingSystem | Select-Object @{N='AvailableGiB';E={[math]::Round($_.FreePhysicalMemory/1MB,2)}},@{N='TotalGiB';E={[math]::Round($_.TotalVisibleMemorySize/1MB,2)}}
Get-Process | Sort-Object WorkingSet64 -Descending | Select-Object -First 15 ProcessName,Id,@{N='GiB';E={[math]::Round($_.WorkingSet64/1GB,2)}}
wsl --list --verbose
docker compose ps
docker stats --no-stream
```

### Politique des lancements

- <3,5 Go disponibles : seulement texte, schémas, tests unitaires légers, ne pas lancer de Spark/GBT/cluster.
- 3,5–5 Go : préparer les scripts, tests courts et contrôler Docker existant ; pas de nouveau job lourd.
- 5–7 Go : **un** essai Spark/SQL très limité, borné et mesuré (1–3 moteurs).
- >=7 Go : essai complet possible sous surveillance, si mémoire WSL/Docker suffisante. Pas de garantie : arrêter proprement si OOM, swaps sévères ou disponible <1,5 Go.
- Un **seul** entraînement ou configuration GBT à la fois. Le grid tuning de **8 essais** se fait **séquentiellement**, l'un après l'autre, avec libération des caches/intermédiaires Spark si cela est justifié et mesuré.
- Préférer une preuve distribuée légère en cluster à de grandes collectes Pandas ; **pas de `toPandas()` massif** ; limiter partitions et tâches selon le dataset sans tricher sur le protocole ML.
- Avant entraînement lourd : **proposer**, sans le faire automatiquement, de fermer ChatGPT Desktop, Spotify, navigateur/onglets et logiciels non nécessaires, après sauvegarde. Sur le PC Reda, conserver **Codex et son environnement d'exécution**. Ne pas tuer de processus système, `java` inconnu, PostgreSQL actif, DataNodes, Spark jobs en écriture, ou WSL.
- Mettre en pause, avec vérification, `spark-inference` et replays non nécessaires **pendant entraînement batch** ; garder Kafka/HDFS/Spark nécessaires à la lecture Bronze, aux calculs et à l'export modèle. Reprendre les services ensuite dans l'ordre contractuel.

**Rapport mémoire** dans les PR de training : RAM hôte disponible avant/après, Docker stats, Workers visibles, temps des 8 configurations, éventuels OOM, taille des entrées, caches conservés/libérés. Ne jamais inventer un résultat ou enlever silencieusement un hyperparamètre.

## C. Lots de développement Reda

### R0 — Audit et définition des contrats (aucun code lourd)

- Lire et comparer la fiche, le cahier et `00_CONTRATS_COMMUNS.md`.
- Vérifier état Git, machine, Python, Docker/WSL, mémoire et disponibilité de `train_FD001.txt`, `test_FD001.txt`, `RUL_FD001.txt` ; documenter absence/licence/provenance/source, **ne jamais générer silencieusement le vrai dataset**.
- Fixer les tests de contrat et l'ordre des features, confirmer avec Ayman les noms des endpoints et des dossiers.
- **Sortie R0 :** liste explicite des interfaces à créer, dépendances sur Ayman et plan de tests ; pas de changement d'architecture.

### R1 — Contrats, data-card et SQL (`feature/reda-contracts-data`)

**Écrire uniquement :** `contracts/event.schema.json`, `contracts/feature_columns.json`, `contracts/README.md`, `database/schema.sql`, `database/queries.sql`, `docs/data-card.md`, `docs/decisions.md`, `tests/test_event_contract.py`, `requirements.txt`.

- Schéma JSON Draft au choix **mais explicitement fixé dans le fichier** ; exactement 1 événement = moteur/cycle, `schema_version:1`, `dataset:"NASA-CMAPSS-FD001"`, `source_split:train|test`, `run_id` UUID, `event_id` déterministe, timestamp simulé vs réel, trois settings, 21 capteurs, tous obligatoires. **Pas de vérité terrain dans Kafka.**
- `contracts/feature_columns.json` objet `{"schema_version":1,"features":["cycle","op_setting_1","op_setting_2","op_setting_3","sensor_01",...,"sensor_21"]}` exactement **25 noms**, dans cet ordre, aucune feature rolling, ni engine_id, ni timestamp, ni RUL.
- Définir/hash commun via représentation canonique UTF-8 JSON de la **liste** des features, séparateurs `(',', ':')` sans espaces ; publier une valeur reproductible et un petit test pour Ayman.
- PostgreSQL `predictions`, `batch_kpis`, `model_registry` exactement comme dans le cahier ; index/PK/contraintes, SQL idempotent ; **ne pas créer une table concurrente** `sensor_data` ou `rul_predictions`.
- La `data-card` décrit FD001 : 26 colonnes, 100 moteurs train/100 test, 20 631/13 096 lignes, 100 RUL terminaux **à vérifier sur fichiers réels**, unité cycles, limite 125, licence/URL/sha256 de la source téléchargée, horodatages fictifs, risques de fuite.
- Tests : exemples valide/invalide, absence de champ, types/infini/NaN, exclusion de RUL, deux splits qui partagent un engine_id, hash features.
- Dépendances Python `requirements.txt` : compatible avec **Spark 3.5.3** et image choisie par Ayman ; ne pas imposer une autre version de PySpark/JDK. Documenter toute incompatibilité pour revue Ayman.

**Barrière C1 :** ouvrir PR, obtenir **revue d'Ayman**, fusionner avant que son producer et writer deviennent définitifs. Diffuser exemples de JSON valides **complets** (21 capteurs) et SQL créé. Ne pas attendre la fin du ML pour publier les contrats.

### R2 — Bronze → qualité → Silver RUL (`feature/reda-batch-kpis`)

**Écrire :** `batch/__init__.py`, `batch/data_quality.py`, `batch/prepare.py`, `tests/test_rul_targets.py`, `tests/test_ml_splits.py`, et scripts de test si nécessaires dans `batch/`.

- **Entrée :** `hdfs://namenode:8020/smartpredict/bronze/engine_events/` remplie par le job Ayman. Lecture depuis un job Spark **cluster**, pas `local[*]` pour une preuve finale.
- Vérifier `source_split`, 26 mesures, IDs/cycles >0, nature réelle du dataset, valeurs finies, absence de doublons contradictoires. Nettoyer les replays sans multiplier la trajectoire : unicité logique `(source_split,engine_id,cycle)` ; si deux valeurs différentes portent cette identité, **conflit explicite**, pas de `dropDuplicates` arbitraire.
- Entrée train : calcul `max_cycle` **uniquement pour produire la cible d'entraînement**, `target_rul=min(125,max_cycle-cycle)`, puis retirer tout max-cycle des features. Dernier cycle train RUL=0.
- Entrée test : ne jamais inférer une panne au dernier cycle ; charger plus tard `RUL_FD001.txt` dans `hdfs://namenode:8020/smartpredict/reference/test_rul/` avec traçabilité, afin que `batch/evaluate.py` utilise les **vrais RUL officiels**.
- **Sortie :** `hdfs://namenode:8020/smartpredict/silver/train_labeled/` Parquet, snapshot reconstruit proprement sans détruire les checkpoints ; nombres et rapports de qualité reproductibles.
- Tests de données sur fixtures courtes puis sur FD001 réel : 100 moteurs train, 20 631 lignes logiques si complet ; labels toujours `[0,125]`; RUL terminal train=0 ; validation et test sans label futur dans `X`.

**Barrière C2 :** ne pas déclarer Silver final sans l'archive réelle d'Ayman et sans compter les lignes dans **HDFS via Spark**. Si Ayman n'a pas fini, produire tests isolés et marquer « en attente de Bronze ».

### R3 — Analyses Spark SQL / Gold / base (`feature/reda-batch-kpis`)

**Écrire :** `batch/kpis.py`, `batch/publish_kpis.py`, `scripts/run_batch.sh`, `database/queries.sql`.

**KPI obligatoires du cahier (sur vue dédupliquée) :**

1. `KPI-01` nombre de mesures par split.
2. `KPI-02` nombre de moteurs distincts.
3. `KPI-03` longueur min/moyenne/médiane/max de trajectoires train.
4. `KPI-04` moyennes et écarts-types d'au moins cinq capteurs par tranche de cycle.
5. `KPI-05` pourcentage de valeurs manquantes et erreurs de qualité.
6. `KPI-06` effectif de moteurs train par bande de RUL (définition et regroupement explicites).

- Requêtes **Spark SQL** via vues temporaires / tables Spark ; mesures reproductibles sur HDFS snapshot.
- Écrire `hdfs://namenode:8020/smartpredict/gold/batch_kpis/` en Parquet **et** `batch_kpis` PostgreSQL ; clé `snapshot_id, metric_name, dimension_name, dimension_value` idempotente, `snapshot_id` lié à une provenance/hachage explicite.
- Ne jamais modifier `streaming/archive.py` pour « arranger » la forme d'un KPI ; demander un changement contrat si blocage.
- Expliquer chaque KPI : signification, formule, unité, qualité, limitation. Les KPI train/test ne doivent pas être interprétés comme validation d'un modèle.

### R4 — Préparer l'évaluation honnête (`feature/reda-sparkml`)

**Écrire :** début de `batch/train_models.py`, `batch/evaluate.py`, tests ML.

- Split **par moteur**, `seed=42`, exactement **80 moteurs pour fit + 20 moteurs validation** de l'ensemble train, sans recouvrir les IDs. Garder les **100 moteurs de test NASA complètement hors** sélection/tuning.
- Variables : exactement 25 doubles `cycle`, settings 1..3, sensors 1..21 ; `engine_id` groupe, n'entre pas dans `features` ; pas de colonnes d'identité/session/label/timestamp/max cycle.
- Pipeline sauvegardé : `Imputer` → `VectorAssembler` → estimateur Spark ML. Les paramètres d'imputation ne doivent être appris que sur **fit** au moment de la comparaison ; validation reçoit `model.transform`.
- Snapshots validation, pour chaque moteur de validation à `floor(0.60 T)`, `floor(0.75 T)`, `floor(0.90 T)`, correspondants aux cycles réellement présents ; cible dérivée du T pour la **mesure uniquement**. Priorité au RMSE sur ces snapshots, MAE aussi ; scores sur tous les cycles validation en diagnostic secondaire.
- Ne jamais se contenter d'un `randomSplit` par ligne ni de `TrainValidationSplit` Spark avec mélange de cycles de même moteur.

**Barrière C3 :** tests prouvent disjonction 80/20, non-fuite, features exactes, métrique snapshots bien calculée.

### R5 — Trois techniques Spark ML + référence + tuning GBT (`feature/reda-sparkml`)

**Écrire :** implémentation finalisée `batch/train_models.py`, résultats ML réels et sauvegarde versionnée.

- Référence `B0` = médiane train (baseline, score RMSE/MAE validation) ; `M1` = `LinearRegression` Spark ML ; `M2` = `RandomForestRegressor` Spark ML ; `M3` = `GBTRegressor` Spark ML. Ne pas remplacer ces trois par XGBoost ou sklearn.
- Même split moteur, même jeu de validation et protocole métrique, `seed=42` là où disponible ; temps d'entraînement et nombre d'observations.
- `GBTRegressor` : **exactement huit expériences** `maxDepth∈{3,5}`, `maxIter∈{30,60}`, `stepSize∈{0.05,0.10}` ; *séquentielles*, une seule configuration active. Enregistrer score et paramètres de chacune ; ne pas remplir les résultats manquants par extrapolation.
- Choisir finalement le meilleur **candidat** parmi `M1`, `M2` et `M3` tuné selon RMSE de validation ; conserver B0 pour comparaison. Le gagnant **n'est pas automatiquement GBT**.
- Puis **refit** du modèle choisi sur 100 moteurs train et sauvegarde Spark `PipelineModel` complet vers `hdfs://namenode:8020/smartpredict/models/rul_pipeline/v001/`.
- Manifeste vers `hdfs://namenode:8020/smartpredict/models/metadata/v001/manifest.json` (voir contrat commun) avec SHA256 features, hash dataset réel, paramètres, algo réellement choisi, URI et horodatage. `metrics.json`, `comparison.csv`; en cas de reprise, éviter d'écraser une version existante sans décision documentée.
- Test de `PipelineModel.load` dans une **nouvelle session Spark** avec résultat colonne `prediction` double ; communiquer à Ayman le modèle **et la preuve de compatibilité** avant le lot d'inférence.

**Barrière C4 :** trois modèles réellement entraînés + 8 tuning réellement exécutés + modèle v001 reloadable. En cas de RAM insuffisante : arrêter proprement et consigner quelles configurations **restent** à faire ; **ne pas annoncer C4 atteint**.

### R6 — Évaluation officielle NASA test (après gel)

**Écrire :** `batch/evaluate.py`, `scripts/run_evaluate.sh`, `tests` liés.

- Source test = Parquet Bronze `source_split=test` dédupliqué ; cibles = **fichier NASA officiel** `RUL_FD001.txt` mis en HDFS `reference/test_rul/`, correspondance par ordre des IDs moteurs test vérifié.
- Sélectionner **le dernier cycle observé** de chacun des **100 moteurs test**, prédire **avec** le pipeline gelé, joindre le RUL officiel terminal `min(125, official_value)` ; ne jamais faire `max_test_cycle - cycle` !
- Mesurer **RMSE (principale)** + MAE sur **100 points terminaux** ; optionnellement R² avec interprétation, sans changer les paramètres après lecture de test.
- Écrire `predictions_test.csv` de **100 lignes**, `metrics.json` avec protocole, date, tailles, source, plages RUL, diagnostics d'erreur. Les métriques doivent provenir de l'exécution ; aucune approximation rapportée comme résultat.

**Barrière C5 :** 100 prédictions test terminales et cible officielle vérifiée. Si source manquante, bloque et signaler plutôt que synthétiser des étiquettes.

### R7 — Base PostgreSQL, dashboard Streamlit (`feature/reda-dashboard`)

**Écrire :** `dashboard/app.py`, `dashboard/data_access.py` et tests UI/SQL, ajuster `database/queries.sql`; **ne pas éditer `infra/Dockerfile.dashboard`**, transmettre à Ayman les dépendances/commande de lancement à intégrer.

- PostgreSQL source unique de restitution : `predictions`, `batch_kpis`, `model_registry` ; **Reda définit DDL**, Ayman writer respecte.
- Afficher : (1) nombre de moteurs du **dernier run test**, (2) état courant par moteur, (3) courbe `predicted_rul_cycles` vs cycle, (4) CRITICAL et WARNING, (5) KPI batch, (6) modèle/version et heure des prédictions, (7) latence indicative `predicted_at-published_at`.
- Rafraîchissement **5 secondes** configurable ; sélecteur `run_id` et filtrage strict, aucune addition des historiques de replays entre sessions.
- Si aucun modèle/donnée, montrer **« en attente »**, et non des chiffres random affichés comme réels. Pour développer l'UI avant hot writer, une fixture explicitement marquée `DEMO SYNTHETIQUE` peut être isolée des tests finaux.
- Dashboard **ne calcule aucune prédiction** et ne demande jamais `RUL_FD001.txt` pour afficher la durée restante prédite.
- Démontrer dans l'UI au moins un replay test en direct réel depuis Ayman ; captures/vidéo datées et logs sans secrets.

### R8 — Intégration finale et contributions

- Faire tourner sur le **PC le plus disponible en RAM** le `docker-compose.yml` d'Ayman sans le modifier unilatéralement.
- Reda charge HDFS Bronze du replay Ayman, génère Silver et Gold, entraîne pipeline v001, vérifie son chargement dans hot job Ayman, inspecte prédictions SQL puis dashboard.
- Fournir vrais tableaux de comparaison des trois modèles et huit configs, RMSE/MAE officiel test, sources/limites, latence et comportement SQL/refresh.
- Réviser avec Ayman le **rapport final <=20 pages** (document distinct du cahier d'architecture long), sections contribution personnelle et vidéo **10 minutes** ; ne pas attribuer à Reda les réalisations d'Ayman.
- Ouvrir PR individuelles, revues croisées ; conserver paternité réelle des commits. Utiliser `integration/e2e-final` pour modifications communes.

## D. Tableau d'interfaces à ne pas inventer

| Source | Contrat attendu | Action Reda |
|---|---|---|
| `contracts/event.schema.json` | topic `engine.measurements.v1`, V1, 3 settings + 21 capteurs | **Créer** et faire approuver |
| `contracts/feature_columns.json` | 25 features ordonnées, hash canonique | **Créer** et fournir tests |
| `database/schema.sql` | tables et types exacts, PK predictions run/event/model | **Créer** ; Ayman consomme |
| Bronze HDFS | `bronze/engine_events/` Parquet, `source_split` | Consommer après Ayman |
| Silver HDFS | `silver/train_labeled/` Parquet, `target_rul` | Produire par `prepare.py` |
| Gold HDFS | `gold/batch_kpis/` Parquet | Produire + SQL |
| Modèle HDFS | `models/rul_pipeline/v001/` Spark PipelineModel | Produire + tester reload |
| Manifeste HDFS | `models/metadata/v001/manifest.json` | Produire version réelle |
| SQL `predictions` | `predicted_rul_cycles`, `health_status`, version, run_id | Lire, ne pas écrire depuis dashboard |

## E. Critères exacts de « fini » pour Reda

| Lot | Vérification attendue | Si non conforme |
|---|---|---|
| Contrats R1 | PR relue par Ayman et tests schémas | Ne pas publier interface concurrente |
| Silver R2 | Train FD001 HDFS dédupliqué : 20 631 lignes logiques | Attendre archive réelle ou corriger Reda |
| KPI R3 | Requêtes Spark SQL et ligne SQL concordantes | Bloquer publication des métriques |
| Split R4 | 80 moteurs fit / 20 validation disjoints | Bloquer ML et corriger split |
| Modèles R5 | B0 + 3 Spark ML + 8 tuning, scores tracés | Compléter expérimentations, pas chiffres fictifs |
| Export R5 | `PipelineModel.load` sur session distincte | Ne pas demander hot test à Ayman |
| Test R6 | 100 moteurs test terminaux + RUL officiel | Bloquer métrique annoncée |
| UI R7 | Refresh 5s, filtre run, aucun mélange données | Corriger UI sans modifier writer Ayman |
| Git | PR Reda, vrais commits, revue Ayman | Ne pas fabriquer contribution |

## F. Rapport obligatoire de Codex après chaque lot

1. Lot, branche, auteur Git effectif.
2. Fichiers Reda créés/modifiés.
3. Contrats/versions lus et hypothèses vérifiées.
4. Commandes réellement exécutées et résultats/failures.
5. Mémoire libre avant/après, `docker stats`, jobs concurrents.
6. Comptages dataset, preuves anti-fuite et métriques **réelles** si applicables.
7. Dépendance bloquante sur Ayman, avec interface précise.
8. Screenshots/logs/preuves sans données sensibles.
9. Proposition commit/PR et demande de revue.
10. Prochaine étape autorisée ; si barrière non franchie, expliquer pourquoi.

**Le prompt à coller dans Codex est fourni en texte autonome :** `PROMPT_REDA_CODEX.txt`. Il impose de lire le cahier et ce guide dans le dépôt avant de coder.
