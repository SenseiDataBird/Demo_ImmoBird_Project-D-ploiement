# =======================================================================================================================================
# ImmoBird - Dockerfile pour Environnement Containerisé
# Image Docker : Apache Airflow + Dépendances ML + Code Pipeline
# ---------------------------------------------------------------------------------------------------------------------------------------
# Dockerfile pédagogique pour CDP Data & IA : démonstration de containerisation reproductible
# Garantit un environnement identique en développement, test et production
# =======================================================================================================================================

# Image de base officielle Apache Airflow (inclut Python 3.11 et Airflow 2.9.3 préinstallés)
FROM apache/airflow:2.9.3-python3.11

# =======================================================================================================================================
# PHASE 1 : INSTALLATION DES DÉPENDANCES SYSTÈME
# =======================================================================================================================================

# Passage en utilisateur root pour les installations système (privilèges administrateur requis)
USER root

# Installation des outils système nécessaires pour le pipeline
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# git : Contrôle de version (pour push optionnel des rapports)
# ca-certificates : Certificats SSL pour connexions HTTPS sécurisées
# rm -rf /var/lib/apt/lists/* : Nettoyage du cache APT (réduction taille image)

# =======================================================================================================================================
# PHASE 2 : STRUCTURE DES RÉPERTOIRES
# =======================================================================================================================================

# Création de l'arborescence de travail Airflow
RUN mkdir -p \
    /opt/airflow/scripts \
    /opt/airflow/data \
    /opt/airflow/dags \
    /opt/airflow/reports
# /opt/airflow/scripts : Répertoire des scripts Python ML
# /opt/airflow/data : Répertoire des datasets (bruts et transformés)
# /opt/airflow/dags : Répertoire des DAGs Airflow
# /opt/airflow/reports : Répertoire des rapports générés

# =======================================================================================================================================
# PHASE 3 : COPIE DU CODE ET DES DONNÉES
# =======================================================================================================================================

# Copie des scripts de machine learning dans l'image
COPY scripts/ /opt/airflow/scripts/
# Pipeline de modélisation (prétraitement, entraînement, rapport)

# Copie des datasets dans l'image (données brutes pour le pipeline)
COPY data/ /opt/airflow/data/
# house_pred.csv (données d'entrée)

# =======================================================================================================================================
# PHASE 4 : INSTALLATION DES DÉPENDANCES PYTHON
# =======================================================================================================================================

# Copie du fichier des dépendances Python
COPY requirements.txt /tmp/requirements.txt
# Liste des packages ML requis

# Passage en utilisateur airflow pour l'installation des packages (sécurité)
USER airflow

# Installation des librairies ML avec contraintes de compatibilité Airflow
RUN pip install --no-cache-dir \
    -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
# Installation depuis requirements.txt avec contraintes Airflow pour garantir la compatibilité

# =======================================================================================================================================
# PHASE 5 : CONFIGURATION DES PERMISSIONS
# =======================================================================================================================================

# Retour en root pour ajuster les permissions des fichiers
USER root

# Attribution des droits appropriés à l'utilisateur airflow (sécurité)
RUN chown -R airflow:root /opt/airflow && \
    chmod -R 775 /opt/airflow
# chown : Propriétaire airflow, groupe root
# chmod 775 : Lecture/écriture/exécution pour propriétaire et groupe

# =======================================================================================================================================
# PHASE 6 : COPIE DES DAGS AIRFLOW
# =======================================================================================================================================

# Copie des DAGs Airflow (après installation pour éviter les conflits de permissions)
COPY airflow/dags/ /opt/airflow/dags/
# pipeline_immo.py (orchestration du pipeline)

# =======================================================================================================================================
# PHASE 7 : CONFIGURATION FINALE
# =======================================================================================================================================

# Passage définitif en utilisateur airflow (principe de moindre privilège)
USER airflow

# Définition du répertoire de travail par défaut
WORKDIR /opt/airflow
# Répertoire racine Airflow

# =======================================================================================================================================
# PHASE 8 : POINT D'ENTRÉE ET COMMANDE PAR DÉFAUT
# =======================================================================================================================================

# Point d'entrée avec dumb-init (gestion propre des signaux système)
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Commande par défaut : lancement d'Airflow en mode standalone
# Mode standalone = webserver + scheduler dans un seul processus (idéal pour démo/développement)
CMD ["bash", "-lc", "airflow standalone"]

# Note pédagogique CDP : En production, on séparerait webserver, scheduler et workers
# pour une meilleure scalabilité et résilience
