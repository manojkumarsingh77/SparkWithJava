FROM python:3.11-slim

# Install sqlcmd for Synapse connection
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg2 apt-transport-https ca-certificates && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
         -o /usr/share/keyrings/microsoft.asc && \
    echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft.asc] \
    https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/opt/mssql-tools18/bin"

WORKDIR /app
RUN pip install --no-cache-dir flask==3.0.3
COPY app.py .

EXPOSE 5000
CMD ["python", "app.py"]