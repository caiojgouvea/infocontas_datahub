# infocontas_datahub

## Como executar

O projeto roda via Docker (Debian slim + unixODBC + driver SQL Server, mais os drivers de Impala/PostgreSQL/MySQL/Oracle listados em `requirements.txt`).

### 1. Build da imagem

```bash
docker build -t infocontas_datahub .
```

### 2. Configurar o `.env`

Copie `.env.example` para `.env` e preencha as variáveis abaixo conforme o que for usar.

#### MinIO do hub (obrigatórias sempre — usadas por `download` e `export`)

| Variável | Descrição |
|---|---|
| `MINIO_ACCESS_KEY` | Chave de acesso ao MinIO central do hub. No `export`, também é usada como identificador do TC produtor (`producer_tc`). |
| `MINIO_SECRET_KEY` | Chave secreta correspondente. |
| `MINIO_ENDPOINT` | Endereço do MinIO do hub, **sem** `http(s)://` (ex.: `minio.exemplo.gov.br:9000`). A conexão é sempre HTTPS. |
| `MINIO_BUCKET` | Nome do bucket do hub (não pode conter `/`). |

#### MinIO de destino (opcionais — só necessárias pra usar `--dest minio`/`both` no `download`)

| Variável | Descrição |
|---|---|
| `DEST_MINIO_ACCESS_KEY` | Chave de acesso do seu MinIO próprio. |
| `DEST_MINIO_SECRET_KEY` | Chave secreta correspondente. |
| `DEST_MINIO_ENDPOINT` | Endereço do seu MinIO, sem `http(s)://`. |
| `DEST_MINIO_BUCKET` | Nome do bucket de destino (não pode conter `/`). |
| `DEST_MINIO_PREFIX` | Opcional. Prefixo/pasta dentro do bucket (ex.: bucket `sigma` + prefix `infocontas` → objetos salvos em `sigma/infocontas/...`). |

O mirror só é ativado quando as 4 primeiras (`DEST_MINIO_ACCESS_KEY/SECRET_KEY/ENDPOINT/BUCKET`) estiverem preenchidas; sem elas, `--dest minio`/`both` falha com erro explícito em vez de silenciosamente não fazer nada.

#### Diretórios e desempenho (opcionais, todas têm valor default)

| Variável | Default | Descrição |
|---|---|---|
| `CONTRACT_DIR` | `contratos` | Onde ficam os contratos (`schema.arrow`/`ingest_rules.json`) baixados do hub. |
| `LOG_DIR` | `logs` | Onde ficam os `.log` de cada execução. |
| `OUTPUT_DIR` | `out` | Onde o `export` grava o parquet, antes (e independente) de publicar. |
| `DOWNLOAD_DIR` | `downloads` | Onde o `download` grava o parquet baixado do hub. |
| `DOWNLOAD_MAX_WORKERS` | `6` | Threads paralelas ao baixar arquivos do hub. |
| `EXTRACT_BATCH_ROWS` | `250000` | Tamanho do lote de linhas extraídas do banco por vez. |
| `ENGINE_CHUNK_ROWS` | `250000` | Tamanho do lote de linhas normalizadas/validadas por vez. |
| `MAX_INVALID_SAMPLES` | `100` | Quantas amostras de linha inválida salvar em `invalid_samples.json`; ao atingir esse limite, o processamento do `export` é interrompido (não continua só pra contar o total). |

#### Banco de dados (obrigatórias pra `export`; não usadas pelo `download`)

| Variável | Descrição |
|---|---|
| `DB_DIALECT` | `mssql`, `postgresql`, `mysql`, `oracle`, `sqlite` ou `impala`. Define o driver/engine usado. |
| `DB_HOST` | Host do banco. Não se aplica ao `sqlite`. |
| `DB_NAME` | Nome do banco/schema (no `sqlite`, vira o caminho do arquivo se `DB_SQLITE_PATH` não for definido). |
| `DB_USER` / `DB_PASSWORD` | Credenciais. Dispensáveis se `DB_TRUSTED_CONNECTION=true` (mssql). |
| `DB_PORT` | Opcional; usa a porta padrão do dialect se não informado (`mssql=1433`, `postgresql=5432`, `mysql=3306`, `oracle=1521`, `impala=21050`). |
| `DB_DRIVER` | Opcional; default por dialect (`pyodbc`, `psycopg`, `pymysql`, `oracledb`, `impyla`). |
| `DB_TRUSTED_CONNECTION` | `true`/`false` (mssql). Se `true`, ignora `DB_USER`/`DB_PASSWORD` e usa autenticação Windows integrada. |
| `DB_SCHEMA` | Opcional, schema padrão da conexão. |
| `DB_QUERY` | Opcional. Parâmetros extras de conexão, no formato `chave=valor&chave2=valor2`. |

Variáveis específicas por `DB_DIALECT`:

| Dialect | Variável | Descrição |
|---|---|---|
| `mssql` | `DB_ODBC_DRIVER` | Nome do driver ODBC instalado (default: `ODBC Driver 18 for SQL Server`). |
| `mssql` | `DB_ENCRYPT` | `yes`/`no` (default `yes`). |
| `mssql` | `DB_TRUST_SERVER_CERTIFICATE` | `yes`/`no` (default `yes`). |
| `mssql` | `DB_LOGIN_TIMEOUT` | Opcional, timeout de login em segundos. |
| `postgresql` | `DB_SSLMODE` | Ex.: `require`, `disable`. |
| `oracle` | `DB_SERVICE_NAME` | Nome do serviço Oracle (alternativa a SID). |
| `sqlite` | `DB_SQLITE_PATH` | Caminho do arquivo `.sqlite` (senão usa `DB_NAME`). |
| `impala` | `DB_AUTH_MECHANISM` | `NOSASL`, `PLAIN` (usuário/senha) ou `GSSAPI` (Kerberos). Default `NOSASL`. |
| `impala` | `DB_USE_SSL` | `true`/`false`. |
| `impala` | `DB_TIMEOUT` | Timeout de conexão em segundos. |
| `impala` | `DB_KERBEROS_SERVICE_NAME` | Só relevante se `DB_AUTH_MECHANISM=GSSAPI`. Nome do serviço Kerberos (geralmente `impala`). |

Monte o `.env` como arquivo (`-v "$(pwd)/.env:/app/.env:ro"`), **não use `--env-file`**: o projeto lê o `.env` internamente via `python-dotenv`, que trata aspas nos valores corretamente. O `--env-file` do Docker não remove aspas e quebra valores como `MINIO_ENDPOINT="..."`.

### 3. Baixar dados do hub (`download` / `download-all-years`)

```bash
docker run --rm \
  -v "$(pwd)/.env:/app/.env:ro" \
  -v "$(pwd)/log:/app/log" \
  -v "$(pwd)/contract:/app/contract" \
  -v "$(pwd)/download:/app/download" \
  infocontas_datahub download --dataset empenhos --version v1 --ano 2025 --tc tce_pb
```

`--dest` controla onde os dados baixados ficam:
- `--dest local` (padrão): salva só em disco, em `download/`.
- `--dest minio`: envia os arquivos pro MinIO de destino (`DEST_MINIO_*` no `.env`) e apaga a cópia local depois.
- `--dest both`: mantém a cópia local e também envia pro MinIO de destino.

Pra baixar todos os anos já publicados de um TC, use `download-all-years` (mesmos volumes, mesmo `--dest`):

```bash
docker run --rm \
  -v "$(pwd)/.env:/app/.env:ro" \
  -v "$(pwd)/log:/app/log" \
  -v "$(pwd)/contract:/app/contract" \
  -v "$(pwd)/download:/app/download" \
  infocontas_datahub download-all-years --dataset empenhos --version v1 --tc tce_pb --dest minio
```

### 4. Publicar dados no hub (`export`)

Precisa das variáveis `DB_*` configuradas no `.env` (`mssql`, `postgresql`, `mysql`, `oracle` ou `impala`, conforme `DB_DIALECT`).

```bash
docker run --rm \
  -v "$(pwd)/.env:/app/.env:ro" \
  -v "$(pwd)/output:/app/output" \
  -v "$(pwd)/log:/app/log" \
  -v "$(pwd)/contract:/app/contract" \
  infocontas_datahub export --dataset empenhos --version v1 --ano 2025
```

Use `--no-publish` pra rodar a extração e a validação completas (mesmas regras do contrato) e gravar o parquet em `output/`, sem nunca publicar no hub — útil pra testar/ajustar a query sem risco de mandar dado de teste pro repositório compartilhado:

```bash
docker run --rm \
  -v "$(pwd)/.env:/app/.env:ro" \
  -v "$(pwd)/output:/app/output" \
  -v "$(pwd)/log:/app/log" \
  -v "$(pwd)/contract:/app/contract" \
  infocontas_datahub export --dataset empenhos --version v1 --ano 2025 --no-publish
```

**Importante**: mesmo sem `--no-publish`, o `export` só publica se **todas** as linhas passarem na validação do contrato — se existir uma única linha inválida, nada é publicado (nem as linhas válidas).


## Executando localmente sem Docker

Com as dependências de `requirements.txt` instaladas e o `.env` na raiz do projeto:

```bash
python -m app.main export --dataset empenhos --version v1 --ano 2026
python -m app.main download --dataset empenhos --version v1 --ano 2026 --tc tce_pb
python -m app.main download-all-years --dataset empenhos --version v1 --tc tce_pb
```

Os mesmos argumentos (`--dest`, `--no-publish`) valem aqui também.

## Build do executável (Windows)

Distribuição usada pelas UIEs: um `.exe` standalone gerado via PyInstaller, junto do `.env` e da pasta `consultas/`.

```bash
pyinstaller ^
  --clean ^
  --noconfirm ^
  --name infocontas_datahub ^
  --onedir ^
  --paths . ^
  --collect-all polars ^
  --collect-all pyarrow ^
  --collect-all pyodbc ^
  --hidden-import pyodbc ^
  --hidden-import pyarrow ^
  --hidden-import pyarrow.lib ^
  --hidden-import pyarrow.dataset ^
  --hidden-import pyarrow.parquet ^
  --hidden-import pyarrow.compute ^
  app/main.py
```
