#!/bin/bash
# =======================================================================================================================================
# Script de démarrage pour la démonstration ImmoBird
# Usage: ./start_demo.sh
# =======================================================================================================================================

echo "🚀 Démarrage de la démonstration ImmoBird..."

# Couleurs pour l'affichage
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Vérifier si Docker est démarré
if ! docker ps >/dev/null 2>&1; then
    echo -e "${RED}❌ Docker n'est pas démarré. Démarrage de Docker...${NC}"
    open -a Docker
    echo "⏳ Attente du démarrage de Docker (30 secondes)..."
    sleep 30
fi

# Arrêter le conteneur existant s'il existe
echo "🧹 Nettoyage des conteneurs existants..."
docker rm -f immo-airflow 2>/dev/null || true

# Vérifier que l'image existe
if ! docker images | grep -q "immo-demo"; then
    echo -e "${RED}❌ Image immo-demo:latest introuvable. Construction de l'image...${NC}"
    docker build -t immo-demo:latest .
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Erreur lors de la construction de l'image${NC}"
        exit 1
    fi
fi

# Lancer le conteneur Airflow
echo "🐳 Lancement du conteneur Airflow..."
docker run -d \
    --name immo-airflow \
    -p 8080:8080 \
    -v "$(pwd)/reports:/opt/airflow/project/reports" \
    immo-demo:latest

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Erreur lors du lancement du conteneur${NC}"
    exit 1
fi

# Attendre qu'Airflow démarre
echo "⏳ Attente du démarrage d'Airflow (45 secondes)..."
sleep 45

# Créer l'utilisateur admin
echo "👤 Création de l'utilisateur administrateur..."
docker exec immo-airflow airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin 2>/dev/null

# Vérifier que l'interface est accessible
echo "🔍 Vérification de l'interface web..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 | grep -q "302\|200"; then
    echo -e "${GREEN}✅ Airflow est opérationnel !${NC}"
    echo ""
    echo -e "${BLUE}📊 === DÉMONSTRATION PRÊTE ===${NC}"
    echo -e "${GREEN}🌐 Interface Airflow : http://localhost:8080${NC}"
    echo -e "${GREEN}👤 Username : admin${NC}"
    echo -e "${GREEN}🔑 Password : admin${NC}"
    echo -e "${GREEN}📋 DAG disponible : pipeline_immo_demo${NC}"
    echo ""
    echo -e "${BLUE}🎯 Pour arrêter : docker stop immo-airflow${NC}"
    echo -e "${BLUE}🔄 Pour relancer : ./start_demo.sh${NC}"
else
    echo -e "${RED}❌ Problème avec l'interface Airflow${NC}"
    echo "📋 Logs du conteneur :"
    docker logs immo-airflow --tail 20
    exit 1
fi
