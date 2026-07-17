# SDD - PostgreSQL array mapping for economic lifecycle flags

## Incident

`ms-signals-orc` completes Flyway successfully but fails during Hibernate schema
validation because `operation_movement_event.lifecycle_quality_flags` is a
PostgreSQL `text[]` while the runtime mapping is interpreted as JSON.

## Contract

1. `lifecycle_quality_flags` remains a nullable PostgreSQL `text[]`; existing
   rows and the forward-only migration must not be rewritten to JSON.
2. Commands and outbox events keep `List<String>`, while the persistence-only
   `OperationMovementEventEntity.lifecycleQualityFlags` uses `String[]` so
   Hibernate 6.6.33 selects PostgreSQL ARRAY unambiguously.
3. Hibernate `hbm2ddl=validate` must accept the migrated PostgreSQL 16 schema.
4. Persisting and reading zero, one, or several lifecycle flags must preserve
   their order and values.
5. `raw` remains the only JSON-mapped economic payload field in this entity.

## Acceptance tests

- A PostgreSQL 16 integration test creates the migrated economic columns and
  successfully boots an `EntityManagerFactory` with schema validation enabled.
- The same test persists and reloads multiple `lifecycle_quality_flags` values.
- A static contract test verifies that the persistence field is `String[]`,
  never an explicitly JSON-mapped collection.

## Implementation decision

Use Hibernate's native ARRAY inference for `String[]`, convert at the entity
boundary, and keep the database declaration `text[]`. Do not add a Flyway
migration unless the production schema differs from the declared contract.
