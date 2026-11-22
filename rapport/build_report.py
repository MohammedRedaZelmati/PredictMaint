"""Build the reviewable SmartPredict report from verified, attributed facts.

Run from the repository root: python rapport/build_report.py
The numerical results reported from the distributed cluster belong to Ayman's
2026-11-21/22 live run; this script does not benchmark or train anything.
"""

from pathlib import Path

from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate,
    Spacer, Table, TableStyle,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "rapport" / "SmartPredict_Rapport.pdf"
NAVY = colors.HexColor("#13243A")
BLUE = colors.HexColor("#2563A6")
TEAL = colors.HexColor("#0D766E")
PALE = colors.HexColor("#EAF1F7")
MUTED = colors.HexColor("#526476")
LIGHT = colors.HexColor("#F5F8FB")

styles = getSampleStyleSheet()
styles.add(ParagraphStyle(
    name="CoverTitle", fontName="Helvetica-Bold", fontSize=26, leading=31,
    textColor=NAVY, alignment=TA_CENTER, spaceAfter=16,
))
styles.add(ParagraphStyle(
    name="CoverSub", fontName="Helvetica", fontSize=13, leading=19,
    textColor=MUTED, alignment=TA_CENTER, spaceAfter=11,
))
styles.add(ParagraphStyle(
    name="SectionX", fontName="Helvetica-Bold", fontSize=16, leading=20,
    textColor=NAVY, spaceBefore=0, spaceAfter=12,
))
styles.add(ParagraphStyle(
    name="SubX", fontName="Helvetica-Bold", fontSize=10.5, leading=14,
    textColor=BLUE, spaceBefore=10, spaceAfter=5,
))
styles.add(ParagraphStyle(
    name="BodyX", fontName="Helvetica", fontSize=9.2, leading=13.3,
    textColor=NAVY, spaceAfter=7,
))
styles.add(ParagraphStyle(
    name="SmallX", fontName="Helvetica", fontSize=8, leading=11.2,
    textColor=NAVY, spaceAfter=5,
))
styles.add(ParagraphStyle(
    name="MutedX", fontName="Helvetica", fontSize=8, leading=11,
    textColor=MUTED, spaceAfter=5,
))
styles.add(ParagraphStyle(
    name="TableHeadX", fontName="Helvetica-Bold", fontSize=8, leading=10.4,
    textColor=colors.white,
))
styles.add(ParagraphStyle(
    name="TableCellX", fontName="Helvetica", fontSize=7.7, leading=10.2,
    textColor=NAVY,
))


def para(text, style="BodyX"):
    return Paragraph(text, styles[style])


def section(title):
    return para(title, "SectionX")


def sub(title):
    return para(title, "SubX")


def bullet(text):
    return para("- " + text)


def table(headers, rows, widths, *, header=True):
    data = []
    if header:
        data.append([para(str(value), "TableHeadX") for value in headers])
    data += [[para(str(value), "TableCellX") for value in row] for row in rows]
    result = Table(data, colWidths=widths, hAlign="LEFT", repeatRows=1 if header else 0)
    commands = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LINEBELOW", (0, -1), (-1, -1), 0.4, colors.HexColor("#CAD7E4")),
    ]
    if header:
        commands += [("BACKGROUND", (0, 0), (-1, 0), NAVY)]
        first_data_row = 1
    else:
        first_data_row = 0
    for row_index in range(first_data_row, len(data)):
        if (row_index - first_data_row) % 2:
            commands.append(("BACKGROUND", (0, row_index), (-1, row_index), LIGHT))
    result.setStyle(TableStyle(commands))
    return result


def arrow(drawing, x1, y1, x2, y2):
    drawing.add(Line(x1, y1, x2, y2, strokeColor=BLUE, strokeWidth=1.4))
    if abs(x2 - x1) >= abs(y2 - y1):
        sign = 1 if x2 > x1 else -1
        points = [x2, y2, x2 - sign * 5, y2 + 3, x2 - sign * 5, y2 - 3]
    else:
        sign = 1 if y2 > y1 else -1
        points = [x2, y2, x2 - 3, y2 - sign * 5, x2 + 3, y2 - sign * 5]
    drawing.add(Polygon(points=points, fillColor=BLUE, strokeColor=BLUE))


def box(drawing, x, y, width, height, label, shade=PALE):
    drawing.add(Rect(x, y, width, height, rx=5, ry=5,
                     fillColor=shade, strokeColor=BLUE, strokeWidth=0.7))
    drawing.add(String(x + width / 2, y + height / 2 - 2.5, label,
                       fontName="Helvetica-Bold", fontSize=8, fillColor=NAVY,
                       textAnchor="middle"))


def architecture_diagram():
    d = Drawing(500, 195)
    box(d, 0, 144, 75, 34, "FD001 local")
    box(d, 98, 144, 66, 34, "Kafka")
    box(d, 190, 144, 82, 34, "Archive Spark")
    box(d, 299, 144, 74, 34, "HDFS Bronze")
    box(d, 399, 144, 95, 34, "Silver / Gold")
    arrow(d, 75, 161, 98, 161)
    arrow(d, 164, 161, 190, 161)
    arrow(d, 272, 161, 299, 161)
    arrow(d, 373, 161, 399, 161)
    box(d, 399, 76, 95, 34, "Pipeline ML v001")
    arrow(d, 446, 144, 446, 110)
    box(d, 98, 14, 104, 34, "Inférence Spark")
    box(d, 257, 14, 105, 34, "PostgreSQL")
    box(d, 407, 14, 87, 34, "Streamlit")
    arrow(d, 131, 144, 131, 48)
    arrow(d, 399, 93, 202, 31)
    arrow(d, 202, 31, 257, 31)
    arrow(d, 362, 31, 407, 31)
    d.add(String(4, 117, "Voie froide : historique, KPI, entraînement", fontName="Helvetica",
                 fontSize=8, fillColor=MUTED))
    d.add(String(4, 65, "Voie chaude : scoring des nouveaux événements", fontName="Helvetica",
                 fontSize=8, fillColor=MUTED))
    return d


def bands_chart():
    d = Drawing(500, 120)
    values = [("0-20", 2100), ("21-50", 3000), ("51-125", 15531)]
    maximum = max(value for _, value in values)
    for index, (label, value) in enumerate(values):
        y = 91 - 36 * index
        d.add(String(0, y + 4, label, fontName="Helvetica-Bold", fontSize=9,
                     fillColor=NAVY))
        d.add(Rect(69, y, value / maximum * 350, 15,
                   fillColor=TEAL if index == 2 else BLUE, strokeColor=None))
        d.add(String(430, y + 4, f"{value:,}".replace(",", " "),
                     fontName="Helvetica-Bold", fontSize=9, fillColor=NAVY))
    return d


def page_decoration(canvas, doc):
    canvas.saveState()
    width, height = A4
    if doc.page > 1:
        canvas.setStrokeColor(colors.HexColor("#D5E0EA"))
        canvas.line(48, height - 39, width - 48, height - 39)
        canvas.setFont("Helvetica-Bold", 7.5)
        canvas.setFillColor(MUTED)
        canvas.drawString(48, height - 31, "SMARTPREDICT  |  RAPPORT D'INTÉGRATION")
    canvas.setStrokeColor(colors.HexColor("#D5E0EA"))
    canvas.line(48, 39, width - 48, 39)
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(48, 28, "NASA C-MAPSS FD001  |  22 novembre 2026")
    canvas.drawRightString(width - 48, 28, f"Page {doc.page}")
    canvas.restoreState()


story = []
story += [Spacer(1, 65), para("SMARTPREDICT", "CoverTitle"),
          para("Pipeline Big Data de maintenance prédictive", "CoverSub"),
          para("Rapport d'intégration - NASA C-MAPSS FD001", "CoverSub"),
          Spacer(1, 28), HRFlowable(width="65%", color=TEAL, thickness=2, hAlign="CENTER"),
          Spacer(1, 25), para("Binôme : Mohammed Reda Zelmati et Ayman El Baida", "CoverSub"),
          para("22 novembre 2026  |  Version pour revue croisée", "CoverSub"),
          Spacer(1, 45),
          para("<b>Traçabilité.</b> Les mesures du cluster et du modèle présentées ici proviennent des essais "
               "rapportés par Ayman sur son PC. Reda a vérifié localement les fichiers FD001, le code, les "
               "tests légers et la configuration Compose. Les limites de preuve sont indiquées explicitement.",
               "BodyX"), PageBreak()]

story += [section("1. Synthèse et périmètre"),
          para("L'objectif est de prédire le <b>remaining useful life</b> (RUL) d'un turbofan simulé, en "
               "cycles restants, à partir de mesures disponibles au cycle observé. Le modèle répond à une "
               "seule tâche : la régression, avec une cible plafonnée à 125 cycles. Le pipeline relie Kafka, "
               "Spark Structured Streaming, HDFS, Spark SQL, Spark ML, PostgreSQL et Streamlit sur un cluster "
               "Docker distribué simulé."),
          para("La fiche enseignante demande un dépôt reproductible, deux workers Spark, deux DataNodes HDFS, "
               "une réplication à 2, trois techniques ML avec tuning, une évaluation indépendante, une "
               "reprise streaming, un rapport de 20 pages maximum et une vidéo d'environ 10 minutes. Le code "
               "des lots R1 à R7 est fusionné dans `main`. Le présent document couvre R8 ; la vidéo "
               "enregistrée et la preuve de validation pédagogique du sujet restent à joindre."),
          sub("Résultat principal rapporté"),
          table(["Élément", "Constat"], [
              ("Modèle", "GBTRegressor, profondeur 5, 60 itérations, pas 0,05 ; PipelineModel v001 sauvegardé dans HDFS"),
              ("Validation", "RMSE 22,37 et MAE 17,14 sur 60 snapshots de 20 moteurs train tenus à l'écart"),
              ("Test officiel", "RMSE 17,23 et MAE 12,77 sur les 100 derniers cycles observés des moteurs test"),
              ("Streaming", "13 096 événements test scorés et écrits dans PostgreSQL, avec UPSERT idempotent"),
          ], [115, 383]),
          Spacer(1, 13),
          para("<b>Interprétation.</b> Une erreur quadratique moyenne d'environ 17 cycles sur les observations "
               "terminales est compatible avec une démonstration de maintenance prédictive, mais ne donne pas "
               "une garantie opérationnelle. Le jeu FD001 est simulé, la métrique est calculée sur un seul "
               "sous-ensemble et la latence publiée concerne un replay en rafale."), PageBreak()]

story += [section("2. Cas métier, données et qualité"),
          para("FD001 vient du jeu simulé NASA Prognostics Center of Excellence, obtenu via le miroir PHM "
               "Society le 21 novembre 2026. La page du miroir demande l'attribution, sans licence explicite "
               "de redistribution trouvée. Les fichiers restent dans `data/raw/`, hors Git. Chaque ligne "
               "train/test contient 26 champs : identifiant moteur, cycle, 3 réglages et 21 capteurs."),
          table(["Fichier", "Volume vérifié", "Usage"], [
              ("train_FD001.txt", "20 631 cycles, 100 moteurs", "Apprentissage et validation par groupes de moteurs"),
              ("test_FD001.txt", "13 096 cycles, 100 autres moteurs", "Replay et test officiel seulement"),
              ("RUL_FD001.txt", "100 valeurs", "Vérité du dernier cycle observé de chaque moteur test"),
          ], [115, 145, 238]),
          Spacer(1, 9),
          para("Les cycles commencent à 1 et sont consécutifs pour chaque moteur. Les valeurs sont numériques "
               "finies ; les trois fichiers ont été contrôlés par SHA-256 (hashes complets dans "
               "`docs/data-card.md`). Onze RUL officiels test dépassent 125 et sont plafonnés pour l'évaluation."),
          sub("Contrat et absence de fuite"),
          bullet("L'événement Kafka V1 a 33 champs, dont 24 mesures ; il ne contient jamais de vrai RUL, de cycle de panne ou de maximum futur."),
          bullet("La clé Kafka est `fd001:<split>:<moteur>` ; l'identifiant d'événement inclut aussi le cycle. `event_ts` est simulé en UTC, `published_at` est l'heure réelle de publication."),
          bullet("Le RUL train vaut `min(125, dernier_cycle_train - cycle)` ; le RUL test terminal vient uniquement de `RUL_FD001.txt`, jamais de la longueur observée du test."),
          bullet("Les 25 features sont `cycle`, 3 réglages et 21 capteurs dans un ordre fixé par `contracts/feature_columns.json` ; aucun identifiant, timestamp, label ou statistique future n'entre dans le modèle."),
          sub("Qualité Bronze et Silver"),
          para("Bronze conserve les événements valides et peut contenir plusieurs occurrences physiques d'un "
               "même cycle après replay. Silver refuse les mesures contradictoires et déduplique les répétitions "
               "identiques. Dans l'essai rapporté, le test Bronze contenait 13 508 lignes physiques pour "
               "13 096 lignes logiques, soit 412 doublons de replay, sans les qualifier d'erreurs. Les valeurs "
               "manquantes publiées sont à 0 % <b>par contrat</b> : les événements invalides sont rejetés en "
               "amont et leur nombre est journalisé par l'archive."), PageBreak()]

story += [section("3. Architecture et reproduction"), architecture_diagram(),
          para("Kafka 3.8.1 KRaft alimente deux jobs Structured Streaming. L'archive écrit le Bronze Parquet "
               "dans HDFS 3.3.6 avec un checkpoint exclusif `archive_v1`. L'inférence charge au démarrage "
               "le PipelineModel sauvegardé, vérifie le hash du schéma de features, puis écrit les prédictions "
               "dans PostgreSQL 16 avec un checkpoint `inference_v1`. Spark 3.5.3 / PySpark 3.5.3, Scala "
               "2.12 et Java 17 s'exécutent sur un master et deux workers ; HDFS a un NameNode, deux "
               "DataNodes, des volumes distincts et une réplication de 2."),
          sub("Séquence de lancement"),
          table(["Étape", "Commande ou condition vérifiable"], [
              ("1. Préparer", "Placer les trois fichiers FD001 dans `data/raw/`, copier `.env.example` vers `.env`, mesurer la RAM, puis `docker compose config --quiet`."),
              ("2. Cluster", "Démarrer Kafka, HDFS, Spark et Postgres par phases ; lancer `scripts/bootstrap.sh --strict` dans `spark-client`."),
              ("3. Historique", "Démarrer `spark-archive` avant le replay train ; vérifier 20 631 cycles logiques ; arrêter l'archive pour les jobs lourds si la RAM manque."),
              ("4. Batch / ML", "Lancer `scripts/run_batch.sh`, entraîner v001 une fois et vérifier le rechargement du PipelineModel."),
              ("5. Flux test", "Démarrer l'archive et l'inférence avant le replay test ; attendre 13 096 événements, puis lancer l'évaluation officielle une seule fois."),
              ("6. Visualiser", "Ouvrir Streamlit sur le port 8501 ; sélectionner le run complet pour voir les 100 moteurs."),
          ], [100, 398]),
          Spacer(1, 9),
          para("Le README contient les commandes copiables. Les checkpoints, volumes HDFS et données SQL sont "
               "persistants ; il ne faut pas lancer `docker compose down -v`. Sur le PC de Reda, 4,72 Gio "
               "étaient disponibles au contrôle R8, donc le cluster n'a pas été démarré ici. Les essais "
               "distribués ont été rapportés depuis le PC d'Ayman."), PageBreak()]

story += [section("4. Analyse batch et indicateurs"),
          para("Le job `batch/prepare.py` lit le Bronze réel, contrôle la complétude du train "
               "(20 631 cycles / 100 moteurs), les trajectoires consécutives et le RUL terminal nul. "
               "Il écrit Silver train étiqueté dans HDFS. `batch/kpis.py` utilise Spark SQL pour publier "
               "un snapshot Gold et `batch/publish_kpis.py` l'insère de façon idempotente dans PostgreSQL."),
          sub("Bandes de RUL train - comptes rapportés"), bands_chart(),
          para("Les trois bandes totalisent 20 631 cycles. Les 100 moteurs passent par les bandes proches "
               "de la panne ; compter les <i>cycles</i> par bande est plus informatif que compter les moteurs, "
               "qui donnerait 100 dans chaque bande."),
          table(["Famille KPI", "Résultat / lecture"], [
              ("Mesures et moteurs", "20 631 train et 13 096 test ; 100 moteurs distincts dans chaque split."),
              ("Trajectoires train", "Min, moyenne, médiane et max des longueurs calculés sur Silver/Bronze logique."),
              ("Capteurs", "Moyenne et écart type de 02, 04, 07, 11, 12 par split et bande de cycle triable. Exemple rapporté : sensor_04 de 1403,0 à 1428,6 ; sensor_12 de 521,9 à 519,7 sur les bandes train."),
              ("Qualité", "0 doublon Bronze train, 412 test dus à des replays ; 0 % de mesures manquantes dans les lignes acceptées."),
          ], [130, 368]),
          Spacer(1, 8),
          para("Ayman a exécuté le dernier code KPI sur son cluster et a rapporté 164 lignes pour un snapshot. "
               "Le détail et les commandes sont reliés à la revue de la PR #2 dans la matrice de validation."),
          PageBreak()]

story += [section("5. Modélisation et évaluation"),
          para("Le split est fixé par graine 42 sur les identifiants moteur : 80 moteurs train pour le fit, "
               "20 autres pour la validation. L'imputation est ajustée sur le fit seulement. Chaque moteur "
               "de validation fournit trois snapshots causaux à 60 %, 75 % et 90 % de sa trajectoire, soit "
               "60 points pour le choix du modèle. Les 100 moteurs test officiels n'entrent ni dans ce choix "
               "ni dans le tuning."),
          sub("Candidats Spark ML"),
          table(["Candidat", "Rôle et constat disponible"], [
              ("B0 - médiane", "Référence simple ajustée sur les moteurs fit ; score numérique non archivé dans Git."),
              ("M1 - LinearRegression", "Technique testée, mais l'optimisation LBFGS a divergé sur les features non standardisées (RMSE de validation autour de 1,0e7 dans le journal d'Ayman). Ce n'est pas une référence exploitable."),
              ("M2 - RandomForestRegressor", "Technique testée ; score exact présent dans le `comparison.csv` HDFS, non archivé dans Git."),
              ("M3 - GBTRegressor", "Huit réglages séquentiels : profondeur 3/5, itérations 30/60, pas 0,05/0,10. Sélection rapportée : profondeur 5, 60 itérations, pas 0,05."),
          ], [140, 358]),
          Spacer(1, 10),
          table(["Évaluation", "RMSE", "MAE", "Échantillon"], [
              ("Validation groupée", "22,37", "17,14", "60 snapshots / 20 moteurs"),
              ("Test officiel terminal", "17,23", "12,77", "100 moteurs test"),
          ], [140, 82, 82, 194]),
          Spacer(1, 9),
          para("Après sélection, le pipeline complet <b>Imputer - VectorAssembler - GBT</b> est refitté "
               "sur les 100 moteurs train, sauvegardé dans HDFS sous v001, puis rechargé pour vérifier "
               "qu'il produit une prédiction. Le manifeste contient le hash canonique des 25 features, "
               "l'algorithme, les paramètres, le plafond, la graine et le hash du train. L'évaluation "
               "officielle compare les 100 derniers cycles observés aux labels `RUL_FD001.txt` plafonnés ; "
               "`predictions_test.csv` et `metrics.json` sont dans HDFS."),
          para("<b>Limite de reproductibilité.</b> Les scores détaillés B0/RF et des huit GBT ne sont pas "
               "dans ce dépôt ; le rapport ne les invente pas. Ils doivent être exportés depuis "
               "`/smartpredict/models/metadata/v001/comparison.csv` pour une table complète de comparaison. "
               "La divergence de LR mérite un essai futur avec StandardScaler, sans réécrire le modèle "
               "v001 déjà gelé."), PageBreak()]

story += [section("6. Streaming, SQL et restitution"),
          para("L'inférence lit Kafka avec Structured Streaming et vérifie les 33 champs du contrat, "
               "l'absence de champ additionnel et les timestamps UTC avant transformation. Le modèle "
               "HDFS est chargé au démarrage après vérification du manifeste. Chaque micro-lot rejette "
               "les mesures contradictoires pour un même `(run_id, event_id)` ; les replays identiques "
               "sont dédupliqués. Une prédiction nulle, NaN ou infinie fait échouer le lot plutôt que "
               "d'inventer un état de santé."),
          para("Les prédictions sont plafonnées à [0, 125], puis classées CRITICAL si RUL <= 20, "
               "WARNING si 20 < RUL <= 50 et HEALTHY au-delà. `streaming/postgres_writer.py` écrit "
               "les 11 colonnes convenues dans `predictions` et réessaie une transaction en échec. "
               "L'UPSERT sur `(run_id, event_id, model_version)` rend un replay idempotent. Le dashboard "
               "lit seulement PostgreSQL, rafraîchit toutes les 5 secondes et n'exécute pas de modèle."),
          sub("Essais de la voie chaude rapportés par Ayman"),
          table(["Essai", "Constat"], [
              ("Replay complet", "13 096 événements test scorés, 100 moteurs, 0 rejet sur le run normal ; 12 moteurs critiques et 16 à surveiller dans la vue terminale rapportée."),
              ("Idempotence", "Replay de deux moteurs avec le même run_id : 80 événements rescorrés, compte SQL inchangé à 13 096."),
              ("Reprise", "Inférence arrêtée, 126 événements publiés, puis reprise depuis `inference_v1` sans perte rapportée. Archive redémarrée sans relire les anciens offsets."),
              ("Contrat négatif", "Quatre événements hors contrat injectés : 4 reçus / 4 rejetés par archive et inférence ; Bronze et SQL inchangés."),
              ("PostgreSQL indisponible", "Dashboard b8c1460 : message d'indisponibilité sans trace d'erreur, retour automatique dans un rafraîchissement après redémarrage de la base."),
          ], [115, 383]),
          Spacer(1, 8),
          para("La latence `predicted_at - published_at` rapportée pour un replay en rafale est p50 18,1 s, "
               "p95 31,6 s et maximum 31,9 s. Ce n'est pas une mesure en régime permanent. Un conflit "
               "arrivant dans un micro-lot ultérieur peut encore réécrire la même clé SQL, car la table "
               "ne stocke pas les mesures ; le contrôle Bronze/Silver le détecte sur l'historique complet."),
          PageBreak()]

story += [section("7. Vérification, limites et reprise"),
          table(["Niveau de preuve", "Vérification"], [
              ("Reda, local", "Hashes et structure FD001 vérifiés ; après fusion #8 : 82 tests réussis, 30 tests Spark ignorés faute d'environnement local ; `docker compose config --quiet` valide."),
              ("Ayman, cluster", "D'après `docs/runtime-lock.md` et ses revues : deux workers, deux DataNodes, réplication 2 ; 110 tests réussis / 2 ignorés dans l'image Spark lors de la correction #8 ; essais froid, chaud, reprise et interface."),
              ("Preuves non jointes", "Journaux bruts de jobs et captures, table complète `comparison.csv`, preuve de validation du sujet par l'enseignante et vidéo Google Drive."),
          ], [130, 368]),
          Spacer(1, 12),
          sub("Difficultés et corrections constatées"),
          bullet("Le healthcheck Spark master échouait sous `/bin/sh` et a été corrigé avec Bash et le hostname du master."),
          bullet("Les deux streaming jobs se disputaient initialement les workers ; la configuration finale prévoit deux cores et 1 Gio par worker, avec exécuteurs à 450 Mio."),
          bullet("L'écriture des métadonnées HDFS a nécessité des octets compatibles Py4J ; les sorties partielles d'un essai échoué ont été conservées hors du chemin v001."),
          bullet("Le parseur Spark a été renforcé après revue : clés JSON exactes, labels même nuls refusés, fuseau UTC contrôlé avant conversion."),
          bullet("Le sink refuse maintenant les conflits de mesures dans un micro-lot et les prédictions infinies ; les checkpoints restent la source de reprise."),
          sub("Risques résiduels"),
          para("Le test officiel porte sur FD001 simulé et 100 observations terminales ; on ne peut pas "
               "en déduire une performance en exploitation réelle. La régression linéaire non normalisée "
               "a divergé. Le contrôle des conflits dans l'inférence est limité au micro-lot ; les "
               "contradictions entre lots sont détectées dans Bronze/Silver. Les statistiques de latence "
               "proviennent d'un replay rapide. La RAM libre est descendue à 0,27 Gio durant un premier "
               "job sur le PC d'Ayman, sans OOM rapporté, ce qui montre une marge faible. Aucune charge "
               "lourde n'a été relancée sur le PC de Reda sous son seuil de RAM."),
          PageBreak()]

story += [section("8. Contributions et livrables"),
          para("La fiche enseignante prévoit quatre étudiants ; l'historique Git fourni identifie deux "
               "personnes. Le rapport documente leurs travaux sans inventer deux autres contributions. "
               "Les revues croisées montrent la collaboration et les limites de chaque preuve."),
          table(["Membre", "Développements, vérifications et documentation"], [
              ("Mohammed Reda Zelmati (@MohammedRedaZelmati)", "Contrats de données et features ; vérification locale des fichiers FD001 ; schéma SQL ; Silver, KPI et publication ; entraînement/évaluation Spark ML ; dashboard ; tests de contrats ; revues des PR #6 à #8 ; documentation d'intégration et ce rapport. PR #1 à #5."),
              ("Ayman El Baida (@Aymannel04)", "Cluster Docker, versions et bootstrap ; Kafka, producteur, archive HDFS ; inférence Structured Streaming et sink PostgreSQL ; tests streaming ; exécution distribuée et rapport des métriques ; runbook/runtime-lock ; revues des PR #1 à #5. PR #6 à #8."),
          ], [150, 348]),
          Spacer(1, 12),
          sub("État de remise"),
          bullet("Code et contrats fusionnés dans `main` ; README reproductible, matrice de validation et conducteur vidéo dans la branche d'intégration, en attente de revue commune."),
          bullet("Le PDF est limité à moins de 20 pages ; son générateur `rapport/build_report.py` reste modifiable et les chiffres sont liés aux preuves citées."),
          bullet("La vidéo enregistrée d'environ 10 minutes et son lien Google Drive restent à produire sur la machine qui exécute la démonstration réelle."),
          sub("Références et traçabilité"),
          para("[1] Fiche enseignante 'Pipeline Big Data de bout en bout', version 2026, fournie dans "
               "l'archive locale du projet. [2] `docs/architecture_smartpredict.tex` et "
               "`docs/guides/00_CONTRATS_COMMUNS.md`. [3] `docs/data-card.md` pour la provenance et les "
               "hashes FD001. [4] `docs/runtime-lock.md` et `docs/validation-matrix.md` pour les essais "
               "attribués. [5] Revues GitHub du dépôt PredictMaint, PR #1 à #8, 21-22 novembre 2026. "
               "[6] Miroir PHM Society de la collection NASA : https://data.phmsociety.org/nasa/."),
          Spacer(1, 12),
          para("<b>Conclusion.</b> Le pipeline démontre la continuité Kafka - HDFS - Spark SQL/ML - "
               "PostgreSQL - Streamlit, avec reprise par checkpoint et contrat de features vérifié. "
               "La remise finale dépend encore de la conservation des preuves brutes, de la revue "
               "commune du présent lot et de l'enregistrement de la vidéo demandée.")]


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(OUTPUT), pagesize=A4, leftMargin=48, rightMargin=48,
        topMargin=53, bottomMargin=52, title="SmartPredict - Rapport d'intégration FD001",
        author="Reda et Ayman (contributions attribuées)",
    )
    doc.build(story, onFirstPage=page_decoration, onLaterPages=page_decoration)
    print(OUTPUT)


if __name__ == "__main__":
    main()
