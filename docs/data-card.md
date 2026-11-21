# NASA C-MAPSS FD001 — fiche de données

## Source et état de vérification

Le projet vise le sous-ensemble FD001 du jeu simulé C-MAPSS du NASA Prognostics Center of Excellence. La [page NASA Open Data](https://data.nasa.gov/dataset/c-mapss-aircraft-engine-simulator-data) consultée le 21 novembre 2026 annonce que C-MAPSS est actuellement indisponible au téléchargement. Aucun fichier source FD001 n'a été trouvé dans ce dépôt lors de R0. L'URL effective de l'archive utilisée, ses conditions de réutilisation et les SHA-256 de chaque fichier restent à enregistrer après obtention d'une copie dont la provenance est vérifiée. Aucune donnée de remplacement n'est présentée comme NASA.

## Structure attendue, à contrôler sur les fichiers réels

| Fichier | Moteurs attendus | Lignes attendues | Rôle |
| --- | ---: | ---: | --- |
| `train_FD001.txt` | 100 | 20 631 | Trajectoires jusqu'à la panne simulée |
| `test_FD001.txt` | 100 | 13 096 | Trajectoires tronquées |
| `RUL_FD001.txt` | 100 valeurs | 100 | RUL au dernier cycle observé de chaque moteur test |

Chaque ligne train/test a 26 champs : identifiant moteur, cycle, trois réglages et 21 capteurs. Le RUL est mesuré en cycles et plafonné à 125. Le RUL train est `min(125, dernier_cycle_train - cycle)`. Le RUL test terminal provient exclusivement de `RUL_FD001.txt`, plafonné à 125. Les horodatages des événements Kafka sont simulés ; ils ne représentent pas des dates de mesure NASA.

## Provenance à compléter lors de l'ingestion vérifiée

| Élément | Valeur vérifiée |
| --- | --- |
| URL de téléchargement effectif | En attente |
| Conditions de réutilisation de cette copie | En attente |
| SHA-256 `train_FD001.txt` | En attente |
| SHA-256 `test_FD001.txt` | En attente |
| SHA-256 `RUL_FD001.txt` | En attente |
| Nombre de champs et valeurs finies | En attente |

Le split validation se fait par moteurs train disjoints du fit. Les 100 moteurs test officiels ne participent ni à la sélection du modèle ni au réglage de ses paramètres. Les features excluent identifiants, timestamps, RUL et maximum futur de cycle.
