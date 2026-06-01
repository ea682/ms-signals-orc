# Profiles: local vs prod

The default profile is `local`:

```bash
SPRING_PROFILES_ACTIVE=local
```

Local is intentionally safe:

- Flyway is disabled by default.
- Kafka listeners are disabled by default.
- The lock provider is local.
- Distributed Hyperliquid dedupe is disabled by default.
- The default DB points to localhost, not production.

Run local against an existing dev database without applying migrations:

```bash
SPRING_PROFILES_ACTIVE=local \
DB_URL="jdbc:postgresql://localhost:5432/trading_futuros?currentSchema=futuros_operaciones" \
DB_USER=postgres \
DB_PASSWORD=postgres \
./mvnw spring-boot:run
```

Production must use the `prod` profile:

```bash
SPRING_PROFILES_ACTIVE=prod \
DB_URL="jdbc:postgresql://<host>:5432/trading_futuros?currentSchema=futuros_operaciones" \
DB_USER="<user>" \
DB_PASSWORD="<password>" \
./mvnw spring-boot:run
```

For the first Flyway-managed deploy on an existing production schema, the prod profile baselines at `20260530` and then applies only the HA migrations from `20260531` onward.

Validate after startup:

```sql
SELECT version, description, success
FROM futuros_operaciones.flyway_schema_history
ORDER BY installed_rank;

SELECT to_regclass('futuros_operaciones.hyperliquid_direct_ingest_dedupe');
```
