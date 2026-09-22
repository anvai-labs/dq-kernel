@REM off
REM Generate PNG files from Mermaid diagrams with scale 3 for better resolution
REM Run this from the docs/diagrams directory

REM Generating PNG files from Mermaid diagrams...
REM.

REM Create PNG directory if it doesn't exist
if not exist "..\png" mkdir "..\png"

REM Generate PNG for each Mermaid file
REM Generating configuration-structure.png...
mmdc -i configuration-structure.mmd -o ..\png\configuration-structure.png --scale 3

REM Generating constraint-types.png...
mmdc -i constraint-types.mmd -o ..\png\constraint-types.png --scale 3

REM Generating custom-constraints.png...
mmdc -i custom-constraints.mmd -o ..\png\custom-constraints.png --scale 3

REM Generating data-flow.png...
mmdc -i data-flow.mmd -o ..\png\data-flow.png --scale 3

REM Generating deployment-patterns.png...
mmdc -i deployment-patterns.mmd -o ..\png\deployment-patterns.png --scale 3

REM Generating engine-architecture.png...
mmdc -i engine-architecture.mmd -o ..\png\engine-architecture.png --scale 3

REM Generating error-handling.png...
mmdc -i error-handling.mmd -o ..\png\error-handling.png --scale 3

REM Generating framework-overview.png...
mmdc -i framework-overview.mmd -o ..\png\framework-overview.png --scale 3

REM Generating repository-system.png...
mmdc -i repository-system.mmd -o ..\png\repository-system.png --scale 3

REM Generating spark-integration.png...
mmdc -i spark-integration.mmd -o ..\png\spark-integration.png --scale 3

REM Generating testing-strategy.png...
mmdc -i testing-strategy.mmd -o ..\png\testing-strategy.png --scale 3

REM.
REM All PNG files generated successfully!
REM Files saved to: ..\png\
REM.
pause