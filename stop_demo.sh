#!/bin/bash
# =======================================================================================================================================
# Script d'arrêt pour la démonstration ImmoBird
# Usage: ./stop_demo.sh
# =======================================================================================================================================

echo "🛑 Arrêt de la démonstration ImmoBird..."

# Couleurs pour l'affichage
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Arrêter le conteneur
if docker ps | grep -q "immo-airflow"; then
    echo "🐳 Arrêt du conteneur Airflow..."
    docker stop immo-airflow
    docker rm immo-airflow
    echo -e "${GREEN}✅ Conteneur arrêté et supprimé${NC}"
else
    echo -e "${RED}ℹ️  Aucun conteneur immo-airflow en cours d'exécution${NC}"
fi

echo -e "${GREEN}🎯 Démonstration arrêtée. Pour relancer : ./start_demo.sh${NC}"
