# Base estável em Debian 12 (Bookworm)
FROM python:3.10-slim-bookworm

WORKDIR /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Dependências do sistema:
# - unixODBC e driver Microsoft para SQL Server
# - bibliotecas SASL/Kerberos para Impala
# - compiladores para eventuais wheels nativas
#
# Observação: as diretivas Verify-Peer/Verify-Host são restritas ao
# packages.microsoft.com por causa de redes corporativas com inspeção SSL.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    gcc \
    g++ \
    unixodbc \
    unixodbc-dev \
    libgssapi-krb5-2 \
    libsasl2-2 \
    libsasl2-dev \
    libsasl2-modules \
    && curl -k -sSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/microsoft-prod.list \
    && printf 'Acquire::https::packages.microsoft.com::Verify-Peer "false";\nAcquire::https::packages.microsoft.com::Verify-Host "false";\n' \
        > /etc/apt/apt.conf.d/99packages-microsoft-insecure \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Instala primeiro as dependências para aproveitar o cache do Docker.
COPY requirements.txt .

# trusted-host: necessário em redes com inspeção SSL corporativa.
RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --trusted-host pypi.python.org \
    --upgrade pip \
    && pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --trusted-host pypi.python.org \
    -r requirements.txt

# O .dockerignore exclui .env, consultas e artefatos locais.
COPY . .

# Pastas que serão montadas no host durante a execução.
RUN mkdir -p consultas log output contract downloads

ENTRYPOINT ["python", "-m", "app.main"]
