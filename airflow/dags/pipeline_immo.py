# =======================================================================================================================================
# ImmoBird - DAG Airflow d'Orchestration
# Pipeline automatisé : Prétraitement → Entraînement → Rapport
# ---------------------------------------------------------------------------------------------------------------------------------------
# DAG pédagogique pour CDP Data & IA : démonstration d'orchestration avec Apache Airflow
# Exécution programmée tous les mardis à 9h00 AM avec retry automatique
# =======================================================================================================================================

from datetime import datetime, timedelta         # Gestion des dates et durées pour la planification
from airflow import DAG                          # Classe principale pour définir un workflow Airflow
from airflow.operators.bash import BashOperator  # Opérateur pour exécuter des commandes bash/shell


# =======================================================================================================================================
# CONFIGURATION DU DAG
# =======================================================================================================================================

# Arguments par défaut appliqués à toutes les tâches du DAG
default_args = {
    "owner": "cdp-demo",                    # Propriétaire du DAG (équipe ou personne responsable)
    "retries": 1,                           # Nombre de tentatives en cas d'échec (robustesse)
    "retry_delay": timedelta(minutes=5),    # Délai d'attente entre les tentatives (5 minutes)
}

# Définition du DAG principal avec ses paramètres d'orchestration
with DAG(
    dag_id="pipeline_immo_demo",                                                    # Identifiant unique du DAG
    start_date=datetime(2024, 1, 1),                                                # Date de début de validité
    schedule_interval="0 9 * * 2",                                                  # Cron: mardis 9h00 (minute heure jour_mois mois jour_semaine)
    catchup=False,                                                                  # Pas de rattrapage des exécutions manquées
    default_args=default_args,                                                      # Applcation des arguments par défaut
    description="Pipeline ImmoBird: ML end-to-end automatisé - Mardis 9h00",        # Description pour l'interface Airflow
) as dag:

    # =======================================================================================================================================
    # TÂCHES DU PIPELINE
    # =======================================================================================================================================

    # Tâche unique qui exécute le pipeline complet de machine learning
    immobird_modeling = BashOperator(
        task_id="immobird_modeling",                                                         # Nom de la tâche dans l'interface
        bash_command="cd /opt/airflow && python /opt/airflow/scripts/immobird_modeling.py",  # Commande à exécuter
        env={
            # Variables d'environnement injectées dans le script Python
            "IMAGE_NAME": "immo-demo:latest",                                       # Version de l'image Docker utilisée
            "GIT_SHA": "unknown",                                                   # Hash du commit Git (pour traçabilité)
            # Configuration pour push automatique vers GitHub
            "REPORTS_REPO_URL": "",  # URL du repository pour les rapports (à configurer)
            "REPORTS_REPO_BRANCH": "main",                                          # Branche cible
            "REPORTS_FOLDER": "reports_to_evaluate",                               # Dossier de destination
        },
    )

    # Note pédagogique: Dans un pipeline plus complexe, on aurait plusieurs tâches avec des dépendances :
    # preprocess_task >> train_task >> report_task
    # Ici, tout est regroupé dans une seule tâche pour simplifier la démonstration CDP
