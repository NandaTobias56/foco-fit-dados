# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingestão fiel
# MAGIC
# MAGIC Lê `eventos`, `profiles`, `vinculos` e (opcional) `profissionais` de até duas origens e grava uma tabela bronze por tabela:
# MAGIC
# MAGIC * **sintético**: arquivos do gerador (`analytics/synthetic/`) enviados ao volume, em
# MAGIC   `.../arquivos/sintetico/` (Parquet ou CSV, um arquivo ou pasta por tabela). A pasta
# MAGIC   `gabarito/` do gerador nunca é lida: é o rótulo das disciplinas, não dado de uso;
# MAGIC * **supabase** (opcional, widget `fonte_supabase`):
# MAGIC   * `arquivos`: CSVs exportados do SQL Editor do Supabase em `.../arquivos/supabase/`;
# MAGIC   * `jdbc`: leitura direta do Postgres, com URL, usuário e senha no secret scope `foco_fit`.
# MAGIC
# MAGIC Regras da bronze: tudo como texto (a tipagem é da prata), `props` como JSON em texto, e as
# MAGIC colunas extras `_ingerido_em`, `_origem` ('sintetico' ou 'supabase') e `_arquivo`.
# MAGIC Recarga completa a cada execução, então rodar duas vezes dá o mesmo resultado.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

for s in (SCHEMA_BRONZE, SCHEMA_PRATA, SCHEMA_OURO):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")
if CATALOGO:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_BRONZE}.arquivos "
              "COMMENT 'Arquivos brutos: sintetico/ (gerador) e supabase/ (exportações)'")
    for pasta in (PASTA_SINTETICO, PASTA_SUPABASE):
        dbutils.fs.mkdirs(pasta)

INGERIDO_EM = agora_utc()

# COMMAND ----------


def _como_texto(df: DataFrame, colunas: list) -> DataFrame:
    """Converte tudo para string; structs/maps (props em Parquet) viram JSON."""
    saida = []
    for c in colunas:
        if c not in df.columns:
            continue
        tipo = df.schema[c].dataType
        if isinstance(tipo, (T.StructType, T.MapType, T.ArrayType)):
            saida.append(F.to_json(F.col(c)).alias(c))
        else:
            saida.append(F.col(c).cast("string").alias(c))
    return df.select(*saida)


def _ler_pasta(pasta: str, tabela: str):
    """Procura `tabela` em Parquet e depois em CSV: arquivo único, partes ou subpasta."""
    tentativas = [
        ("parquet", f"{pasta}/{tabela}.parquet"),
        ("parquet", f"{pasta}/{tabela}"),
        ("parquet", f"{pasta}/{tabela}_*.parquet"),
        ("csv", f"{pasta}/{tabela}.csv"),
        ("csv", f"{pasta}/{tabela}_*.csv"),
    ]
    for formato, caminho in tentativas:
        try:
            leitor = spark.read.format(formato)
            if formato == "csv":
                leitor = (leitor.option("header", "true").option("multiLine", "true")
                          .option("escape", '"').option("inferSchema", "false"))
            df = leitor.load(caminho)
            if not df.columns or (formato == "parquet" and "id" not in df.columns and "aluno_id" not in df.columns):
                continue
            return df.withColumn("_arquivo", F.col("_metadata.file_path"))
        except Exception:
            continue
    return None


def _ler_jdbc(tabela: str, colunas: list):
    url = segredo("supabase_jdbc_url")
    if not url:
        raise ValueError("fonte_supabase=jdbc, mas falta o segredo foco_fit/supabase_jdbc_url (ver README).")
    existentes = {
        "eventos": COLUNAS_EVENTOS,
        "profiles": COLUNAS_PROFILES,
        "vinculos": ["id", "aluno_id", "profissional_id", "tipo", "status", "criado_em"],
        "profissionais": COLUNAS_PROFISSIONAIS,
    }[tabela]
    # Seleciona colunas explícitas: nome, e-mail e código de vínculo nunca entram no lakehouse.
    consulta = f"select {', '.join(existentes)} from public.{tabela}"
    if tabela == "eventos":
        consulta = consulta.replace("props", "props::text as props")
    df = (spark.read.format("jdbc")
          .option("url", url)
          .option("user", segredo("supabase_usuario"))
          .option("password", segredo("supabase_senha"))
          .option("driver", "org.postgresql.Driver")
          .option("query", consulta)
          .load())
    return df.withColumn("_arquivo", F.lit(f"jdbc:public.{tabela}"))


def ingerir(tabela: str, colunas: list, obrigatoria: bool = True):
    partes = []
    df = _ler_pasta(PASTA_SINTETICO, tabela)
    if df is not None:
        partes.append(_como_texto(df, colunas + ["_arquivo"]).withColumn("_origem", F.lit("sintetico")))
    else:
        print(f"[{tabela}] nada em {PASTA_SINTETICO}")

    if FONTE_SUPABASE == "arquivos":
        df = _ler_pasta(PASTA_SUPABASE, tabela)
        if df is not None:
            partes.append(_como_texto(df, colunas + ["_arquivo"]).withColumn("_origem", F.lit("supabase")))
        else:
            print(f"[{tabela}] nada em {PASTA_SUPABASE}")
    elif FONTE_SUPABASE == "jdbc":
        df = _ler_jdbc(tabela, colunas)
        partes.append(_como_texto(df, colunas + ["_arquivo"]).withColumn("_origem", F.lit("supabase")))

    if not partes and not obrigatoria:
        return None
    if not partes:
        raise ValueError(f"Nenhuma origem encontrada para '{tabela}'. Envie os arquivos do gerador para "
                         f"{PASTA_SINTETICO} (ver README).")
    total = partes[0]
    for p in partes[1:]:
        total = total.unionByName(p, allowMissingColumns=True)
    # Garante todas as colunas do contrato, mesmo que alguma origem não tenha (ex.: estado x status).
    for c in colunas:
        if c not in total.columns:
            total = total.withColumn(c, F.lit(None).cast("string"))
    return total.select(*colunas, "_arquivo", "_origem").withColumn("_ingerido_em", F.lit(INGERIDO_EM).cast("timestamp"))

# COMMAND ----------

bronze_eventos = ingerir("eventos", COLUNAS_EVENTOS)
salvar(bronze_eventos, SCHEMA_BRONZE, "eventos", "Eventos de uso do app, cópia fiel (texto). Origem em _origem.")

bronze_profiles = ingerir("profiles", COLUNAS_PROFILES)
salvar(bronze_profiles, SCHEMA_BRONZE, "profiles", "Perfis sem nome nem e-mail: id, papel, nascimento, criado_em.")

bronze_vinculos = ingerir("vinculos", COLUNAS_VINCULOS)
salvar(bronze_vinculos, SCHEMA_BRONZE, "vinculos", "Vínculos aluno x profissional, cópia fiel.")

bronze_profissionais = ingerir("profissionais", COLUNAS_PROFISSIONAIS, obrigatoria=False)
if bronze_profissionais is None:  # tabela opcional: grava vazia para a prata não quebrar
    bronze_profissionais = spark.createDataFrame(
        [], ", ".join(f"{c} string" for c in COLUNAS_PROFISSIONAIS + ["_arquivo", "_origem"]) + ", _ingerido_em timestamp")
salvar(bronze_profissionais, SCHEMA_BRONZE, "profissionais", "Conselho, UF e situação dos profissionais (sem registro).")

# COMMAND ----------

display(spark.table(f"{SCHEMA_BRONZE}.eventos").groupBy("_origem", "tipo").count().orderBy("_origem", "tipo"))
