param(
    [string]$JavaHome = "C:\Users\erika\.jdks\temurin-21.0.11"
)

$ErrorActionPreference = "Stop"
$env:JAVA_HOME = $JavaHome
$env:Path = "$JavaHome\bin;$env:Path"

& .\mvnw.cmd '-Dcopy.benchmark.enabled=true' '-Dtest=CopyDispatchLocalPerformanceTest,CopyDispatchConcurrencyBenchmarkTest' test
if ($LASTEXITCODE -ne 0) {
    throw "Copy dispatch benchmark failed with exit code $LASTEXITCODE"
}
