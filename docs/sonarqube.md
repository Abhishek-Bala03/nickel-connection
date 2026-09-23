# SonarQube for free

Prepared by Divij N.

CI runs SonarQube once a server is configured: repository variable `SONAR_HOST_URL` and secret
`SONAR_TOKEN`. Until then, anyone can run the free **SonarQube Community Build** with Docker, on a laptop
or a dev server. Code and results stay on that machine.

The same setup is in all three repositories (nickel-connection, nickel-connections-ui, nickel-infra). One
running SonarQube serves all three; each repository is its own project (`sonar-project.properties`).

## Start SonarQube

```bash
cp docker/sonarqube/.env.example docker/sonarqube/.env    # set SONAR_DB_PASSWORD (.env is git-ignored)
docker compose -f docker/sonarqube/compose.yaml up -d      # about 70 s to start
```

Open http://localhost:9000, log in as `admin` / `admin` and set a new password when asked. Then create a
token: My Account > Security > Generate Tokens.

## Scan this repository

```bash
uv run pytest                                   # writes coverage.xml, which SonarQube imports
SONAR_TOKEN=<token> docker/sonarqube/scan.sh
```

The scanner runs from its Docker image on SonarQube's Docker network, so there is nothing to install. It
waits for the quality gate and exits non-zero if the gate fails.

## Share, stop, limits

- **Share it with the team:** set `SONAR_BIND=0.0.0.0` in `docker/sonarqube/.env` on a dev server.
  Teammates on the tailnet then scan with `SONAR_HOST_URL=http://<server>:9000` and
  `SONAR_DOCKER_NETWORK=host` (Linux) or `bridge` (Docker Desktop).
- **Stop it:** `docker compose -f docker/sonarqube/compose.yaml down`; add `-v` to wipe its data.
- **Free-edition limits:** one branch per project and no pull-request comments. Branch and pull-request
  analysis need a paid edition, or a server wired into CI.
- The compose file turns off Elasticsearch's memory-map checks, so SonarQube runs without raising the
  host's `vm.max_map_count`, which is not allowed inside containers such as the LXD dev servers. That is
  fine for development; keep the defaults on a production server.
