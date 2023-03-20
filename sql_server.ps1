podman machine start

New-Item -ItemType Directory -Path ./mssql_data -ErrorAction SilentlyContinue
podman run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=test-1234" -p 1433:1433 -v ./mssql_data:/var/opt/mssql/data -d mcr.microsoft.com/mssql/server:latest