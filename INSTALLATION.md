# Installation Guide

```bash
docker compose up -d
mvn clean compile install -DskipTests
```

## Testing on Bad Networks
Uses toxiproxy to simulate bad network conditions.
1. To set the bad network params, edit `toxics.json` file.
2. (Re)start toxiproxy and toxiproxy_configurator containers (those containers live in profile `chaos` so they dont start by default):
    ```bash
    docker compose restart toxiproxy toxiproxy_configurator
    ```
3. Uncomment following line in client/application.properties to send traffic through toxiproxy:
    ```
    # Enable toxiproxy port overrides
    spring.profiles.active=toxiproxy
    ```
