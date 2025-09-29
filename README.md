## Cas d'utilisation pédagogique: Prévision des prix de l'immobilier

Public cible: Chefs de Projet Data & IA. Objectif: comprendre la chaîne de valeur d'un projet Data/IA (GitHub → Docker → Airflow → Kubernetes) sur un cas simple et réaliste.

### Schéma mental: à quoi sert chaque outil ?
- **GitHub**: héberge le code, versionne, permet revue/PR et CI/CD.
- **Docker**: encapsule l'environnement (dépendances + scripts + données de démo) pour reproduire à l'identique.
- **Airflow**: orchestre les tâches du pipeline (prétraitement → entraînement), planifie, trace, relance en cas d'échec.
- **Kubernetes**: déploie et fait évoluer les workloads conteneurisés (UI Airflow persistante ou exécution ponctuelle via Job).

### Contenu du repo
- `data/house_pred.csv`: données brutes (entrée)
- `data/house_pred_for_ml.csv`: données transformées (sortie du prétraitement)
- `scripts/data_processing.py`: nettoyage/feature engineering minimal
- `scripts/model_training.py`: entraînement d'un modèle XGBoost + métriques
- `airflow/dags/pipeline_immo.py`: DAG Airflow (séquence 2 tâches)
- `Dockerfile`, `requirements.txt`: environnement reproductible

## Prérequis (simples)
- **Option recommandée**: Docker Desktop installé et démarré (baleine → "Docker is running").
- **Option Kubernetes** (facultatif pour la démo): Minikube (`brew install minikube`).

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

### Alternative sans UI (exécution directe des scripts)
```bash
docker run --rm --name immo-pipeline immo-demo:latest bash -lc "cd /opt/airflow/data && python /opt/airflow/scripts/data_processing.py && cd /opt/airflow && python /opt/airflow/scripts/model_training.py"
```

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

## Kubernetes (facultatif): déploiement & scalabilité avec Minikube
1) Démarrer Minikube et charger l'image locale
```bash
minikube start
minikube image load immo-demo:latest
```
2) Déployer l'UI Airflow (pod persistant) et l'exposer
```bash
kubectl apply -f k8s/deployment.yaml
minikube service immo-airflow-svc --url
```
3) Lancer le pipeline en exécution "one-shot" (Job Kubernetes)
```bash
kubectl apply -f k8s/job.yaml
kubectl logs job/immo-pipeline-once -f
```

## Critères de réussite (checklist)
- Docker: image construite sans erreur; conteneur Airflow accessible sur http://localhost:8080.
- Airflow: exécution du DAG avec 2 tâches vertes; logs contenant RMSE et R².
- Kubernetes: pods en statut `Running` (Deployment) et `Completed` (Job); logs du Job affichant les 2 étapes.

## Questions de réflexion (CDP)
- **Reproductibilité**: que gagne-t-on à dockeriser vs installer localement ?
- **Orchestration**: quand passer de scripts ad hoc à un DAG Airflow ?
- **Coûts/Opérations**: UI Airflow persistante (Deployment) vs Job ponctuel (batch) ?
- **Scalabilité**: quand et comment paralléliser (plus de tâches, worker/executor Airflow, K8s autoscaling) ?
- **CI/CD**: que mettre dans le pipeline GitHub Actions (build image, tests, scan, déploiement) ?

## Dépannage rapide
- Airflow introuvable dans l'IDE: c'est normal hors conteneur. Deux options:
  - Installer localement: `pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"`
  - Ou ignorer l'alerte IDE: ajouter `# type: ignore` sur l'import Airflow.
- Docker non démarré: lancez Docker Desktop (baleine) et attendez "Docker is running".
- Port 8080 utilisé: exécuter `-p 8081:8080` côté `docker run`.
- Chemins de fichiers:
  - Le prétraitement est exécuté dans `/opt/airflow/data` pour que `house_pred.csv` soit trouvé.
  - La sortie attendue par l'entraînement est `data/house_pred_for_ml.csv`.
- Kubernetes n'accède pas à l'image: exécuter `minikube image load immo-demo:latest` avant `kubectl apply`.

## GitHub (option discussion)
- Versionner code + manifests; PR pour revue; CI pour build/test image; CD pour déployer sur K8s.

## Nettoyage
```bash
kubectl delete -f k8s/deployment.yaml || true
kubectl delete -f k8s/job.yaml || true
minikube stop || true
```
