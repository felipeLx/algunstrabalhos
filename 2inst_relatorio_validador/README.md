# Validador de Relatórios — 2ª Instância

Aplicação web interna para validação e monitoramento de qualidade dos dados do Data Lake. Verifica nulos, outliers, integridade de dados e reconcilia contagens entre o banco de origem (Oracle) e o S3/Iceberg.

---

## Páginas

| Página | O que faz |
|--------|-----------|
| **AWS Analysis** | Nulos em colunas críticas, timeline semanal com detecção de anomalias (< média − 2σ), outliers IQR, schema da tabela |
| **Source vs AWS** | Reconcilia contagem de linhas e schema entre Oracle/MySQL/PostgreSQL e S3 |
| **Integrity** | Duplicatas por chave, domínio de valores, ordenação de datas, limites numéricos, tendência de volume período a período |

---

## Execução local (desenvolvimento)

### Instalar dependências

```bash
pip install -r requirements.txt
```

### Configurar credenciais

Crie `.env` na raiz do projeto — **nunca commitar**:

```env
AWS_PROFILE=tjrj
ORACLE_HOST=10.103.0.2
ORACLE_PORT=1525
ORACLE_SERVICE_NAME=srv_tj01
ORACLE_USER=dw_deige
ORACLE_PASSWORD=sua_senha
```

### Iniciar

```bash
streamlit run app.py
# Acessa em http://localhost:8501
```

> **WSL2 no Windows:** se `localhost` der timeout, use o IP da rede WSL ou configure port proxy:
> ```powershell
> # PowerShell como Administrador — substituir pelo IP do WSL (ip addr show eth0)
> netsh interface portproxy add v4tov4 listenport=8501 listenaddress=127.0.0.1 connectport=8501 connectaddress=<IP_WSL2>
> ```

---

## Configuração — `config.yaml`

```yaml
s3_bucket: sgdai-data-lake
s3_prefix: ouro
s3_projeto: dw_2inst_ouro
s3_tabela: tb_tf_situacao_ejud
aws_profile: "tjrj"
date_col: "dthrmov"
not_null_cols:
  - coddoc
  - situacao
outlier_multiplier: 1.5
max_rows: 1000000
bronze_tables:
  - s3_path: bronze/ejud/movimentorelator
    oracle_table: TJ01.MOVIMENTORELATOR
  - s3_path: bronze/ejud/movimento
    oracle_table: TJ01.MOVIMENTO
  - s3_path: bronze/ejud/t_processo
    oracle_table: TJ01.T_PROCESSO
  - s3_path: bronze/ejud/movdistribuicao
    oracle_table: TJ01.MOVDISTRIBUICAO
```

Credenciais Oracle **não ficam** no `config.yaml` — lidas do `.env` (local) ou do AWS Secrets Manager (produção).

---

## Credenciais em produção — AWS Secrets Manager

Em produção (container App Runner), o `secrets_manager.py` detecta o ambiente automaticamente e lê o secret `2inst/relatorio-validador/credentials`.

Para criar o secret (executar uma vez pela SGDAI):

```bash
# Preencher CHANGE_ME no secrets_template.json antes de executar
aws secretsmanager create-secret \
  --name "2inst/relatorio-validador/credentials" \
  --secret-string file://secrets_template.json \
  --profile tjrj
```

---

## Deploy — AWS App Runner

```
Azure DevOps (branch prod)
  → docker build
  → docker push → ECR (2inst-relatorio-validador:latest)
  → App Runner detecta novo push e redeploya automaticamente
```

### Criar serviço (executar uma vez)

```bash
# Editar apprunner-config.json: substituir ACCOUNT_ID pelo ID da conta AWS
aws apprunner create-service \
  --cli-input-json file://apprunner-config.json \
  --profile tjrj
```

### Gerenciar serviço

```bash
# Pausar quando não utilizado (para de cobrar compute)
aws apprunner pause-service --service-arn <ARN> --profile tjrj

# Encerrar definitivamente
aws apprunner delete-service --service-arn <ARN> --profile tjrj
```

---

## Scripts CLI

### Gate de qualidade (CI/CD)

```bash
python ci_check.py
# exit 0 = passou | exit 1 = falhou
python ci_check.py --not-null coddoc situacao --max-rows 100000
```

### Reconciliação Bronze vs Oracle

```bash
python compare_bronze_oracle.py
python compare_bronze_oracle.py --threshold 0.01  # falha se diff > 1%
```

---

## Arquivos

| Arquivo | Finalidade |
|---------|-----------|
| `app.py` | Página inicial Streamlit |
| `config.yaml` | Configuração central (S3, colunas, bronze_tables) |
| `utils.py` | Helpers: S3, SSO, config, detecção de ambiente |
| `db_connector.py` | Conector Oracle / MySQL / PostgreSQL |
| `secrets_manager.py` | Credenciais via Secrets Manager (prod) ou `.env` (local) |
| `pages/1_AWS_Analysis.py` | Verificações na tabela S3/Iceberg |
| `pages/2_Source_vs_AWS.py` | Reconciliação origem vs AWS |
| `pages/3_Integrity.py` | Integridade de dados |
| `ci_check.py` | Gate de qualidade para pipeline CI/CD |
| `compare_bronze_oracle.py` | Reconciliação Bronze vs Oracle via CLI |
| `Dockerfile` | Imagem para App Runner (porta 8080) |
| `apprunner-config.json` | Config do serviço App Runner |
| `iam-instance-role-policy.json` | Política IAM mínima (S3 + Secrets Manager) |
| `secrets_template.json` | Template para criar secret no Secrets Manager |
| `azure-pipeline.yml` | Pipeline CI/CD: sync S3 + build/push ECR |
| `.env` | Credenciais locais — **não commitar** |
