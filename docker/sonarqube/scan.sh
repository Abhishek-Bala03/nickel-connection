#!/usr/bin/env bash
# Analyse this repository with SonarQube, using the scanner's Docker image (nothing to install).
#
#   uv run pytest                        # writes coverage.xml, which SonarQube imports
#   SONAR_TOKEN=<token> docker/sonarqube/scan.sh
#
# Defaults target the SonarQube started by docker/sonarqube/compose.yaml. For another server set
# SONAR_HOST_URL, and SONAR_DOCKER_NETWORK=host (Linux) or bridge (Docker Desktop).
# Create the token in SonarQube: My Account > Security > Generate Tokens.
set -euo pipefail

cd "$(dirname "$0")/../.."
: "${SONAR_TOKEN:?set SONAR_TOKEN to a SonarQube user token}"
SONAR_HOST_URL="${SONAR_HOST_URL:-http://sonarqube:9000}"
SONAR_DOCKER_NETWORK="${SONAR_DOCKER_NETWORK:-nickel-sonarqube_default}"

if [ ! -f coverage.xml ]; then
  echo "note: no coverage.xml; run 'uv run pytest' first to see coverage in SonarQube" >&2
fi

# The scanner's work files go to /tmp inside the container, so nothing is written into the checkout.
docker run --rm \
  --network "$SONAR_DOCKER_NETWORK" \
  -e SONAR_HOST_URL="$SONAR_HOST_URL" \
  -e SONAR_TOKEN \
  -e SONAR_USER_HOME=/tmp/.sonar \
  -v "$PWD:/usr/src" \
  sonarsource/sonar-scanner-cli:12.2 \
  -Dsonar.working.directory=/tmp/.scannerwork \
  -Dsonar.qualitygate.wait=true
