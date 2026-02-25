#!/bin/bash
# ============================================================
# JADC2 COP - Docker Cleanup Script
# ============================================================
# Removes all JADC2 containers, volumes, images, and networks.
# Run this before a fresh deployment or to reclaim disk space.
#
# Usage: ./cleanup-docker.sh [--full]
#   --full : Also remove built images (forces rebuild on next start)
# ============================================================

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo -e "${YELLOW}============================================================${NC}"
echo -e "${YELLOW} JADC2 COP - Docker Cleanup${NC}"
echo -e "${YELLOW}============================================================${NC}"
echo ""

# Stop and remove containers + networks + volumes
echo -e "${YELLOW}Stopping and removing containers...${NC}"
docker compose -f docker/docker-compose.yml down -v --remove-orphans 2>/dev/null
echo -e "${GREEN}✓ Containers, networks, and volumes removed${NC}"

# Remove any orphaned JADC2 containers
ORPHANS=$(docker ps -a --filter "name=jadc2" -q 2>/dev/null)
if [ -n "$ORPHANS" ]; then
    echo ""
    echo -e "${YELLOW}Removing orphaned JADC2 containers...${NC}"
    docker rm -f $ORPHANS 2>/dev/null
    echo -e "${GREEN}✓ Orphaned containers removed${NC}"
fi

# Remove JADC2 volumes that might have been missed
VOLUMES=$(docker volume ls -q 2>/dev/null | grep -E "jadc2|cflt_mongo|mongodb-data|kafka-data|connect-plugins")
if [ -n "$VOLUMES" ]; then
    echo ""
    echo -e "${YELLOW}Removing JADC2 volumes...${NC}"
    echo "$VOLUMES" | xargs docker volume rm -f 2>/dev/null
    echo -e "${GREEN}✓ Volumes removed${NC}"
fi

# Remove built images if --full flag
if [ "$1" == "--full" ]; then
    echo ""
    echo -e "${YELLOW}Removing JADC2 images (--full mode)...${NC}"
    IMAGES=$(docker images --filter "reference=*jadc2*" -q 2>/dev/null; \
             docker images --filter "reference=*cflt_mongo*" -q 2>/dev/null)
    if [ -n "$IMAGES" ]; then
        echo "$IMAGES" | sort -u | xargs docker rmi -f 2>/dev/null
        echo -e "${GREEN}✓ Images removed${NC}"
    else
        echo "  No JADC2 images found"
    fi

    echo ""
    echo -e "${YELLOW}Pruning build cache...${NC}"
    docker builder prune -f 2>/dev/null
    echo -e "${GREEN}✓ Build cache pruned${NC}"
fi

# Summary
echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN} Cleanup Complete${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "  To redeploy:"
echo "    docker compose -f docker/docker-compose.yml up -d"
echo "    # Wait ~90 seconds"
echo "    ./rebuild-jadc2.sh"
echo ""
if [ "$1" != "--full" ]; then
    echo "  Tip: Run with --full to also remove built images"
    echo "       ./cleanup-docker.sh --full"
    echo ""
fi
