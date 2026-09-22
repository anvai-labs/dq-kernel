@echo off
REM Individual commands to generate PNG files from Mermaid diagrams
REM Run each command separately if the batch file isn't working

echo Creating PNG directory if it doesn't exist...
mkdir ..\png 2>nul

echo.
echo Run these commands one by one:
echo.

echo mmdc -i configuration-structure.mmd -o ..\png\configuration-structure.png --scale 3
mmdc -i configuration-structure.mmd -o ..\png\configuration-structure.png --scale 3

echo.
echo mmdc -i constraint-types.mmd -o ..\png\constraint-types.png --scale 3
mmdc -i constraint-types.mmd -o ..\png\constraint-types.png --scale 3

echo.
echo mmdc -i custom-constraints.mmd -o ..\png\custom-constraints.png --scale 3
mmdc -i custom-constraints.mmd -o ..\png\custom-constraints.png --scale 3

echo.
echo mmdc -i data-flow.mmd -o ..\png\data-flow.png --scale 3
mmdc -i data-flow.mmd -o ..\png\data-flow.png --scale 3

echo.
echo mmdc -i deployment-patterns.mmd -o ..\png\deployment-patterns.png --scale 3
mmdc -i deployment-patterns.mmd -o ..\png\deployment-patterns.png --scale 3

echo.
echo mmdc -i engine-architecture.mmd -o ..\png\engine-architecture.png --scale 3
mmdc -i engine-architecture.mmd -o ..\png\engine-architecture.png --scale 3

echo.
echo mmdc -i error-handling.mmd -o ..\png\error-handling.png --scale 3
mmdc -i error-handling.mmd -o ..\png\error-handling.png --scale 3

echo.
echo mmdc -i framework-overview.mmd -o ..\png\framework-overview.png --scale 3
mmdc -i framework-overview.mmd -o ..\png\framework-overview.png --scale 3

echo.
echo mmdc -i repository-system.mmd -o ..\png\repository-system.png --scale 3
mmdc -i repository-system.mmd -o ..\png\repository-system.png --scale 3

echo.
echo mmdc -i spark-integration.mmd -o ..\png\spark-integration.png --scale 3
mmdc -i spark-integration.mmd -o ..\png\spark-integration.png --scale 3

echo.
echo mmdc -i testing-strategy.mmd -o ..\png\testing-strategy.png --scale 3
mmdc -i testing-strategy.mmd -o ..\png\testing-strategy.png --scale 3

echo.
echo Done!
pause