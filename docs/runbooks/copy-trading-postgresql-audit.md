# Runbook: auditoria PostgreSQL read-only de copy trading

## Proposito

Repetir la auditoria QA sin exponer secretos ni modificar datos. Este runbook no
aplica migrations, no crea indices y no llama Binance.

## 1. Prerrequisitos

- `.env.audit` local con `DB_URL`, `DB_USER`, `DB_PASSWORD`, `DB_SCHEMA`.
- El archivo debe estar ignorado por Git.
- `psql` en PATH.
- Rol esperado: `copy_audit`.
- Java 21 + Maven para tests locales.
- Docker para Testcontainers, o PostgreSQL local efimero.

Verificar sin imprimir el archivo:

```powershell
git check-ignore -v .env.audit
git status --short
```

`.env.audit` no debe aparecer como versionable.

## 2. Carga segura PowerShell

```powershell
$cfg = @{}
Get-Content -LiteralPath '.env.audit' | ForEach-Object {
    if ($_ -match '^([^#=]+)=(.*)$') {
        $cfg[$matches[1].Trim()] = $matches[2].Trim()
    }
}

$uri = [Uri](($cfg['DB_URL']) -replace '^jdbc:', '')
$env:PGPASSWORD = $cfg['DB_PASSWORD']
$env:PGOPTIONS = @(
    '-c default_transaction_read_only=on'
    '-c statement_timeout=15s'
    '-c lock_timeout=2s'
    '-c idle_in_transaction_session_timeout=30s'
    '-c application_name=copy_performance_audit'
    '-c search_path=futuros_operaciones,public'
) -join ' '
```

No mostrar `$cfg`, `DB_URL`, `DB_PASSWORD`, `PGPASSWORD` ni `PGOPTIONS`.

## 3. Gate obligatorio

Ejecutar primero identidad, flags del role y `SHOW`. Si usuario/base/read-only o
flags no privilegiados no coinciden, detener con
`AUDIT_BLOCKED_PERMISSIONS`. No cambiar a otro usuario.

Cada bloque debe tener esta forma:

```sql
SET application_name = 'copy_performance_audit';
SET default_transaction_read_only = on;
SET statement_timeout = '15s';
SET lock_timeout = '2s';
SET idle_in_transaction_session_timeout = '30s';
SET search_path = futuros_operaciones, public;
BEGIN READ ONLY;
-- SELECT, SHOW o EXPLAIN de SELECT revisado
ROLLBACK;
```

## 4. Ejecutar el script versionado

```powershell
try {
    psql -X `
      -v ON_ERROR_STOP=1 `
      -h $uri.Host `
      -p $uri.Port `
      -U $cfg['DB_USER'] `
      -d $uri.AbsolutePath.TrimStart('/') `
      -f 'src/main/resources/db/validation/copy_trading_postgresql_audit.sql'

    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL audit failed"
    }
}
finally {
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    Remove-Item Env:PGOPTIONS -ErrorAction SilentlyContinue
    $cfg.Clear()
}
```

No redirigir un error a un usuario administrador. Un failure debe conservar su
causa y terminar el runbook.

## 5. EXPLAIN seguro

1. Revisar que sea SELECT sin funciones desconocidas.
2. Ejecutar primero `EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)`.
3. Confirmar timeout, bounds y cardinalidad.
4. Solo entonces usar `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)`.
5. No usar ANALYZE con row locks, DML o funciones no verificadas.

El rol `copy_audit` puede rechazar `FOR UPDATE SKIP LOCKED`; es correcto. Usar
el filtro sin lock en QA y probar la concurrencia localmente.

## 6. Benchmarks

- Separar handshake, first query y conexion warm.
- Usar una conexion JDBC reutilizada para el hot path.
- Warmup antes de capturar p50/p75/p90/p95/p99/max.
- No mezclar SQL con HTTP/Binance.
- No hacer benchmarks de escritura en QA.

## 7. Tests PostgreSQL

### Con Docker

```powershell
$env:JAVA_HOME = 'C:\Users\erika\.jdks\temurin-21.0.11'
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
mvn -Dtest=CopyTradingPostgresConcurrencyTest test
```

La suite inicia PostgreSQL 16 mediante Testcontainers y no usa QA.

### Con PostgreSQL local

Inicializar una base efimera fuera de QA y pasar:

```powershell
mvn `
  "-Dcopy.postgres.test.jdbc-url=<local-jdbc-url>" `
  "-Dcopy.postgres.test.username=<local-user>" `
  "-Dcopy.postgres.test.password=<local-password>" `
  -Dtest=CopyTradingPostgresConcurrencyTest test
```

Detener la instancia local al terminar. No apuntar esas propiedades a QA.

## 8. Verificacion final

```sql
BEGIN READ ONLY;
SELECT count(*) AS audit_sessions
FROM pg_stat_activity
WHERE application_name = 'copy_performance_audit'
  AND pid <> pg_backend_pid();

SELECT count(*) AS blocked
FROM pg_stat_activity
WHERE cardinality(pg_blocking_pids(pid)) > 0;
ROLLBACK;
```

Esperado: cero sesiones de auditoria remanentes. Bloqueados debe registrarse,
no cancelarse.

## 9. Checklist de reporte

- identidad y read-only;
- version/size/cardinalidad;
- indices invalidos/duplicados/faltantes;
- plans y percentiles;
- locks/waits/pool;
- autovacuum/stats;
- migration aplicada o pendiente;
- medido vs proyectado vs no medido;
- Testcontainers/deploy/canary pendientes;
- una clasificacion permitida y conservadora.
