# Décisions Reda

- 2026-11-21 — Contrat événement V1 en JSON Schema Draft 2020-12. La cohérence `event_id` et les valeurs numériques finies sont contrôlées au niveau applicatif en plus du schéma.
- 2026-11-21 — Les doublons Bronze aux mesures différentes pour un même split, moteur et cycle sont signalés comme conflits avant toute déduplication Silver. Cette règle applique le contrôle qualité du guide Reda et évite le choix arbitraire d'une mesure.
- 2026-11-21 — Les trois tables PostgreSQL suivent le DDL du cahier d'architecture. Le writer et les producteurs restent sous la responsabilité d'Ayman.
- 2026-11-22 — Les KPI de qualité lisent Bronze physique avant déduplication. `quality_error_count` compte les répétitions identiques éliminées ; une identité invalide, une valeur manquante ou non finie, ou des mesures contradictoires bloquent le lot avant publication. `missing_value_pct` décrit Bronze physique et vaut donc zéro pour un lot accepté. Les bandes RUL comptent les cycles train, et les statistiques capteurs sont séparées par split et bande de cycle.
