# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração do lakehouse Foco Fit
# MAGIC
# MAGIC Notebook incluído pelos outros com `%run ./00_config`. Define parâmetros (widgets),
# MAGIC nomes de schemas e tabelas, o segredo do hash e funções auxiliares.
# MAGIC
# MAGIC | camada | schema padrão | conteúdo |
# MAGIC | --- | --- | --- |
# MAGIC | bronze | `workspace.foco_fit_bronze` | cópia fiel das tabelas de origem, tudo como texto, mais `_ingerido_em`, `_origem`, `_arquivo` |
# MAGIC | prata | `workspace.foco_fit_prata` | limpa, tipada, deduplicada e pseudonimizada (aluno = hash) |
# MAGIC | ouro | `workspace.foco_fit_ouro` | modelo estrela (fatos e dimensões) e tabelas de KPIs |
# MAGIC
# MAGIC Nenhuma credencial fica no repositório: o sal do hash e o acesso ao Supabase vêm do
# MAGIC secret scope `foco_fit` (ver README).

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# Parâmetros. No job do bundle eles chegam como base_parameters; rodando à mão, aparecem como widgets.
dbutils.widgets.text("catalogo", "workspace", "Catálogo (Unity Catalog)")
dbutils.widgets.text("prefixo_schema", "foco_fit", "Prefixo dos schemas")
dbutils.widgets.dropdown("fonte_supabase", "nenhuma", ["nenhuma", "arquivos", "jdbc"], "Supabase: de onde ler")
dbutils.widgets.text("refeicoes_por_dia", "5", "Refeições previstas por dia (sem plano)")
dbutils.widgets.text("treinos_meta_semana", "5", "Meta de treinos por semana")
dbutils.widgets.text("cardio_meta_semana", "150", "Meta de minutos de cardio por semana")
dbutils.widgets.text("data_referencia", "", "Data de referência (vazio = último dia com dados)")

CATALOGO = dbutils.widgets.get("catalogo").strip()
PREFIXO = dbutils.widgets.get("prefixo_schema").strip()
FONTE_SUPABASE = dbutils.widgets.get("fonte_supabase").strip()
REFEICOES_POR_DIA = int(dbutils.widgets.get("refeicoes_por_dia"))
TREINOS_META_SEMANA = int(dbutils.widgets.get("treinos_meta_semana"))
CARDIO_META_SEMANA = int(dbutils.widgets.get("cardio_meta_semana"))
DATA_REFERENCIA = dbutils.widgets.get("data_referencia").strip() or None

# Formato das tabelas gerenciadas. No Databricks é sempre Delta; o teste local troca por parquet.
FORMATO_TABELA = globals().get("FORMATO_TABELA", "delta")


def _schema(camada: str) -> str:
    nome = f"{PREFIXO}_{camada}"
    return f"{CATALOGO}.{nome}" if CATALOGO else nome


SCHEMA_BRONZE = _schema("bronze")
SCHEMA_PRATA = _schema("prata")
SCHEMA_OURO = _schema("ouro")

# Volume onde ficam os arquivos brutos (gerador sintético e exportações do Supabase).
VOLUME = globals().get("VOLUME_LOCAL") or f"/Volumes/{CATALOGO}/{PREFIXO}_bronze/arquivos"
PASTA_SINTETICO = f"{VOLUME}/sintetico"
PASTA_SUPABASE = f"{VOLUME}/supabase"

SECRET_SCOPE = "foco_fit"

# Dia, hora e semana são do ponto de vista do aluno (Brasil). Timestamps sem fuso são lidos neste fuso.
FUSO = "America/Sao_Paulo"
spark.conf.set("spark.sql.session.timeZone", FUSO)

# COMMAND ----------

# Contrato de eventos v1 (pos-dados/contrato-eventos.md) + v2 (telas, cadastro, avisos agendados, experimentos).
TIPOS_EVENTO = [
    "app_aberto", "refeicao_registrada", "refeicao_desfeita", "treino_feito", "treino_desfeito",
    "cardio_registrado", "agua_copo", "peso_registrado", "doce_sos_vencido", "bau_aberto",
    "promessa_feita", "notificacao_tocada",
    "tela_vista", "cadastro_etapa", "notificacao_agendada", "experimento_exposto",
]
COLUNAS_EVENTOS = ["id", "aluno_id", "tipo", "dia", "ocorrido_em", "recebido_em",
                   "plataforma", "app_versao", "sessao_id", "props"]
COLUNAS_PROFILES = ["id", "papel", "nascimento", "criado_em"]  # nome e e-mail nunca saem do Supabase
# O contrato cita `estado`, mas no Supabase e no gerador a coluna é `status`; a prata aceita as duas.
COLUNAS_VINCULOS = ["id", "aluno_id", "profissional_id", "tipo", "estado", "status", "criado_em"]
# Opcional. Sem número de registro nem documento: só conselho, UF e situação da aprovação.
COLUNAS_PROFISSIONAIS = ["id", "conselho", "uf", "status", "criado_em"]

# Esquema das props, já tipado. `texto` (promessa) fica de fora na prata: é texto livre do aluno.
SCHEMA_PROPS = T.StructType([
    T.StructField("refeicao_id", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("grupo", T.StringType()),
    T.StructField("minutos", T.IntegerType()),
    T.StructField("delta", T.IntegerType()),
    T.StructField("total_dia", T.IntegerType()),
    T.StructField("kg", T.DoubleType()),
    T.StructField("premio", T.StringType()),
    T.StructField("texto", T.StringType()),
    T.StructField("tipo", T.StringType()),
    T.StructField("acao", T.StringType()),
    # v2
    T.StructField("tela", T.StringType()),
    T.StructField("origem", T.StringType()),
    T.StructField("etapa", T.StringType()),
    T.StructField("novo", T.BooleanType()),
    T.StructField("quantidade", T.IntegerType()),
    T.StructField("experimento", T.StringType()),
    T.StructField("sintetico", T.BooleanType()),
])

# COMMAND ----------


def segredo(chave: str, padrao=None):
    """Lê do secret scope `foco_fit`; devolve `padrao` se o scope ou a chave não existirem."""
    try:
        return dbutils.secrets.get(SECRET_SCOPE, chave)
    except Exception:
        return padrao


SAL_HASH = segredo("hash_sal")
if not SAL_HASH:
    print("AVISO: segredo foco_fit/hash_sal não encontrado. Usando um sal de desenvolvimento; "
          "crie o segredo antes de ingerir dados reais (ver README).")
    SAL_HASH = "foco-fit-dev-nao-usar-com-dados-reais"


def hash_id(coluna):
    """Pseudonimiza um id (LGPD): SHA-256 de sal + id. Sem o sal não dá para voltar ao id."""
    c = F.lower(F.trim(coluna.cast("string")))
    return F.when(c.isNull() | (c == ""), F.lit(None)).otherwise(F.sha2(F.concat(F.lit(SAL_HASH), c), 256))


def salvar(df: DataFrame, schema: str, tabela: str, comentario: str = None) -> str:
    """Grava uma tabela gerenciada com overwrite (recarga completa, idempotente)."""
    nome = f"{schema}.{tabela}"
    (df.write.format(FORMATO_TABELA).mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(nome))
    if comentario and FORMATO_TABELA == "delta":
        spark.sql(f"COMMENT ON TABLE {nome} IS '{comentario.replace(chr(39), chr(39) * 2)}'")
    print(f"{nome}: {spark.table(nome).count()} linhas")
    return nome


def agora_utc():
    return datetime.now(timezone.utc)
