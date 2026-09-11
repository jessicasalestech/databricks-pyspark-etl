# Databricks PySpark · Medallion ETL Pipeline

[![Português](https://img.shields.io/badge/Portugu%C3%AAs-green?style=plastic&logo=openbadges&logoColor=white)](README-pt-BR.md) [![English](https://img.shields.io/badge/English-blue?style=plastic&logo=openbadges&logoColor=white)](README.md)

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Compatible-FF3621?logo=databricks&logoColor=white)
![CI](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue)

Um projeto profissional de portfólio que demonstra um pipeline de dados
medallion (**bronze → prata → ouro**) com **Databricks + PySpark**, funções de
transformação reutilizáveis e testadas por unidade e um pipeline de CI verde.

> **Owner:** Jessica Sales — QA / Engenheira de Software
> Tudo neste repositório está versionado em inglês.

---

## O que este projeto demonstra

- **Arquitetura medallion (multi-hop)** — separação clara de responsabilidades
  nas camadas Bronze, Prata e Ouro.
- **Funções PySpark reutilizáveis e quase puras** em um pacote `etl` que são
  fáceis de testar por unidade contra um SparkSession local.
- **Lógica de ETL orientada a testes** — 10 testes pytest cobrindo tratamento de
  NULL, deduplicação, aplicação de esquema, correção de agregação e tipagem.
- **Notebooks Databricks** como fonte `.py` que espelham jobs do AWS Glue /
  Databricks e encadeiam Bronze → Prata → Ouro.
- **Integração Contínua** — um job do GitHub Actions que instala o PySpark no
  JDK 17 e executa todos os testes.

---

## Arquitetura medallion

```
┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
│      BRONZE          │   │      SILVER          │   │       GOLD          │
│  (landing / raw)     │──▶│  (clean / conformed) │──▶│  (aggregated / BI)  │
│                      │   │                      │   │                      │
│ • raw CSV / Parquet  │   │ • trim / lowercase   │   │ • revenue per order │
│ • source audit cols  │   │ • NULL handling      │   │ • revenue bycategory│
│ • permissive read    │   │ • dedupe on order_id │   │ • revenue by region │
│ • + ingested_at time │   │ • schema enforcement │   │ • only active orders│
└──────────────────────┘   └──────────────────────┘   └──────────────────────┘
        notebooks/               notebooks/                 notebooks/
     BRONZE_ingest.py        SILVER_transform.py          GOLD_report.py
```

Cada notebook se baseia no anterior. Executá-los em ordem (`BRONZE` →
`SILVER` → `GOLD`) produz as tabelas finais de relatório.

---

## Estrutura do repositório

```
databricks-pyspark-etl/
├── notebooks/            # Notebooks Databricks como fonte .py (BRONZE, SILVER, GOLD)
├── etl/                  # Pacote de transformação PySpark reutilizável (testável por unidade)
├── tests/                # Suíte pytest para o pacote etl (SparkSession local)
├── data/                 # CSV de exemplo usado pela demo e pelos testes
├── .github/workflows/    # CI em push/PR para main (PySpark + JDK 17 + pytest)
├── requirements.txt      # pyspark, pandas, pytest
├── pyproject.toml        # Config do projeto + pytest
├── Dockerfile            # execução local containerizada (opcional)
├── README.md
└── .env.example          # copie para .env — nunca commite valores reais
```

---

## Como o Databricks executa isto

1. **Importe o repositório** para um workspace do Databricks (integração Git ou
   **Repos** → adicionar repositório).
2. Crie um **cluster** com um runtime do Databricks (ex.: 13.3 LTS) — o PySpark
   já está instalado.
3. Abra **`notebooks/BRONZE_ingest.py`** e selecione o cluster com
   **Run notebook**.
4. Os notebooks usam `sys.path.insert(0, "/Workspace/Repos/databricks-pyspark-etl")`
   para que o pacote `etl` seja resolvido dentro do Databricks.
5. Cada notebook registra tabelas Delta (`bronze_orders`, `silver_orders`,
   `gold_category_revenue`, `gold_region_revenue`) que você pode consultar a
   partir do **Databricks SQL**.

Você também pode agendar os notebooks como um **Databricks Job** na ordem
BRONZE → SILVER → GOLD para um pipeline multi-hop de produção.

---

## Execute localmente

### 1. Pré-requisitos

- Python 3.9+ (3.11 recomendado)
- Java 8/11/17. **O JDK 17 é fortemente recomendado** — veja CloudOutput abaixo.
- `pip`

### 2. Instalação

```bash
cd databricks-pyspark-etl
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Execute os testes

```bash
export SPARK_LOCAL_IP=127.0.0.1      # defensivo em alguns runners
pytest -v
```

Esperado: todos os testes passam. Exemplo do final:

```text
10 passed in 200.42s
```

### 4. Teste de fumaça da lógica do notebook localmente

Teste de fumaça local do ETL encadeado (carrega o CSV de exemplo, aplica as
mesmas funções e imprime os resultados agregados):

```bash
python tests/run_pipeline_demo.py
```

---

## .env.example

Copie para `.env` se você executar qualquer coisa contra um workspace real do
Databricks (CLI / REST do Databricks). O repositório **não contém segredos** —
apenas placeholders.

---

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) executa em **push/PR
para `main`** e:

1. Faz o checkout do código (`actions/checkout@v4`).
2. Configura o **Python 3.11** (`actions/setup-python@v5`).
3. Configura o **Temurin JDK 17** com cache do Maven (`actions/setup-java@v4`).
4. Instala o PySpark (que usa o JDK 17 fornecido) e executa `pytest -v`.

---

## Notas / ressalvas

- O PySpark no **Windows** pode falhar ao iniciar com **Java 21+**; o job de CI
  fixa o **JDK 17** para uma execução garantidamente verde. Para fidelidade
  total localmente, use o JDK 17.
- Pipelines Spark são inerentemente dependentes ao redor de `.count()` /
  `.collect()`; o conjunto de dados de exemplo é deliberadamente pequeno para
  que as execuções locais continuem rápidas.
- Esta é uma demonstração de portfólio, não infraestrutura de produção (sem
  Autoloader, Unity Catalog ou Delta live tables — embora possa ser estendido
  a eles).

---

## Licença

MIT — livre para uso como peça de aprendizado e portfólio.