## Cas d'utilisation pédagogique: Prévision des prix de l'immobilier

Public cible: Chefs de Projet Data & IA. Objectif: comprendre la chaîne de valeur d'un projet Data/IA (GitHub → Cursor → Docker → Airflow) sur un cas simple et réaliste.

### Schéma mental: à quoi sert chaque outil ?
- **GitHub**: héberge et versionne le code, outil collaboratif.
- **Docker**: encapsule l'environnement (dépendances + scripts + données de démo) pour reproduire à l'identique.
- **Airflow**: orchestre les tâches du pipeline (prétraitement → entraînement), planifie, trace, relance en cas d'échec.

### Contenu du repo
- `data/house_pred.csv`: données brutes (entrée)
- `data/house_pred_for_ml.csv`: données transformées (sortie du prétraitement)
- `scripts/data_processing.py`: nettoyage/feature engineering minimal
- `scripts/model_training.py`: entraînement d'un modèle XGBoost + métriques
- `airflow/dags/pipeline_immo.py`: DAG Airflow (séquence 2 tâches)
- `Dockerfile`, `requirements.txt`: environnement reproductible

## Prérequis (simples)
- **Option recommandée**: Docker Desktop installé et démarré.
- 
## Démarrage rapide (5 minutes)
1) Construire l'image Docker
```bash
docker build -t immo-demo:latest .
```
2) Démarrer Airflow en local (web + scheduler)
```bash
docker run --rm -p 8080:8080 --name immo-airflow immo-demo:latest
```
- Accédez à l'UI: http://localhost:8080 (mode standalone, identifiants affichés dans les logs du conteneur).
3) Déclencher le DAG `pipeline_immo_demo` depuis l'UI (bouton Play) et observer les 2 tâches.

## Comprendre le pipeline
- **Entrée**: `data/house_pred.csv`
- **Tâche 1 – preprocess_data** (`scripts/data_processing.py`)
  - Gère les valeurs manquantes, standardise numériques, encode catégorielles, écrit `data/house_pred_for_ml.csv`.
- **Tâche 2 – train_model** (`scripts/model_training.py`)
  - Lit `data/house_pred_for_ml.csv`, entraîne XGBoost, affiche **RMSE** et **R²**, sauvegarde `immo_model.joblib`.

## Airflow: orchestration et traçabilité
- Le DAG `pipeline_immo_demo` enchaîne 2 tâches:
  1. `preprocess_data` → génère `data/house_pred_for_ml.csv`
  2. `train_model` → imprime RMSE/R² et l'importance des features
- Intérêt pour CDP:
  - **Observabilité**: statut, logs, durée, dépendances.
  - **Opérations**: relance en cas d'échec, planification, historisation.

## Questions de réflexion (CDP)
- **Reproductibilité**: que gagne-t-on à dockeriser vs installer localement ?
- **Orchestration**: quand passer de scripts ad hoc à un DAG Airflow ?
- **Coûts/Opérations**: UI Airflow persistante (Deployment) vs Job ponctuel (batch) ?
