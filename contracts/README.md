# Contrat FD001 V1

`event.schema.json` utilise JSON Schema Draft 2020-12. Le topic est `engine.measurements.v1` avec la clé Kafka `fd001:<source_split>:<engine_id>`. Un message représente un moteur et un cycle. `event_id` vaut exactement `fd001:<source_split>:<engine_id>:<cycle>` et reste identique entre replays. `run_id` est un UUID de session.

La validation applicative vérifie également l'égalité de `event_id` avec les trois champs source, la finitude des 24 mesures et la sérialisation JSON stricte. Le format `date-time` et les valeurs UTC exigent un validateur de formats. `event_ts` est simulé ; `published_at` est l'heure réelle de publication. Le RUL officiel ne figure jamais dans le message. Les moteurs `train:1` et `test:1` sont distincts.

`feature_columns.json` fixe les 25 colonnes et leur ordre. Le hash SHA-256 porte uniquement sur la liste `features` sérialisée en UTF-8 avec `json.dumps(features, ensure_ascii=False, separators=(',', ':'))`. Le producteur et l'inférence doivent refuser un contrat incompatible. Les mesures Silver de même `(source_split, engine_id, cycle)` avec valeurs divergentes sont des conflits à signaler avant déduplication.

Hash V1 vérifié par `tests/test_event_contract.py` : `05ad82b0be2cdb6ad78a1216b2c4ddfe80f29c58f5082039ec0f273d2c638d6f`.
