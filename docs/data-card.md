# NASA C-MAPSS FD001 — fiche de données

## Source et état de vérification

Le projet utilise le sous-ensemble FD001 du jeu simulé du NASA Prognostics Center of Excellence, téléchargé le 21 novembre 2026 depuis le [miroir PHM Society du dépôt NASA](https://data.phmsociety.org/nasa/), entrée « Turbofan Engine Degradation Simulation ». URL de l'archive téléchargée : `https://phm-datasets.s3.amazonaws.com/NASA/6.+Turbofan+Engine+Degradation+Simulation+Data+Set.zip`. L'archive externe contient `CMAPSSData.zip`, qui contient les trois fichiers FD001. SHA-256 de l'archive externe : `c9c5dec12a945a82e8bb4446589d7fb3cc057b5e5d81fa1a12e25ee9912ad3b2`. Les fichiers restent locaux dans `data/raw/`, ignoré par Git.

La page du miroir demande de créditer le dépôt et les donateurs dans les publications et précise que l'utilisation se fait aux risques de l'utilisateur. Elle n'affiche pas de licence de redistribution explicite. La page NASA Open Data relative au **simulateur** C-MAPSS annonce une indisponibilité du téléchargement et n'est pas l'URL de cette archive FD001.

## Structure vérifiée sur les fichiers téléchargés

| Fichier | Moteurs attendus | Lignes attendues | Rôle |
| --- | ---: | ---: | --- |
| `train_FD001.txt` | 100 | 20 631 | Trajectoires jusqu'à la panne simulée |
| `test_FD001.txt` | 100 | 13 096 | Trajectoires tronquées |
| `RUL_FD001.txt` | 100 valeurs | 100 | RUL au dernier cycle observé de chaque moteur test |

Chaque ligne train/test a 26 champs : identifiant moteur, cycle, trois réglages et 21 capteurs. Le RUL est mesuré en cycles et plafonné à 125. Le RUL train est `min(125, dernier_cycle_train - cycle)`. Le RUL test terminal provient exclusivement de `RUL_FD001.txt`, plafonné à 125. Les horodatages des événements Kafka sont simulés ; ils ne représentent pas des dates de mesure NASA.

## Vérifications locales

| Élément | Résultat |
| --- | --- |
| SHA-256 `train_FD001.txt` | `963b5e22825b34d8b21c69e1aeb4af3e647050eb672ee8834ba4b5d91d2de0f8` |
| SHA-256 `test_FD001.txt` | `3cda7109ce17bafb5443f2ac926cfcf88154b941b8c4cf95eb55d1ddd6f52851` |
| SHA-256 `RUL_FD001.txt` | `a19c8ec94931949d0485bdc35118206e9c81c4547b422efb9cf86f4ceddbceca` |
| Champs par ligne train/test | 26 pour chaque ligne |
| Champs par ligne RUL | 1 pour chaque ligne |
| Valeurs numériques | Toutes finies |

La vérification a lu les fichiers décompressés, compté les lignes non vides et les identifiants moteur, puis contrôlé le nombre de champs et la finitude de chaque valeur. Les contrôles HDFS Bronze et Silver restent à exécuter après l'archivage d'Ayman.

Le split validation se fait par moteurs train disjoints du fit. Les 100 moteurs test officiels ne participent ni à la sélection du modèle ni au réglage de ses paramètres. Les features excluent identifiants, timestamps, RUL et maximum futur de cycle.
