# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Qualidade dos dados
# MAGIC
# MAGIC Checagens que fazem o job falhar quando algo essencial quebra, e um resumo gravado em
# MAGIC `ouro.qualidade_execucoes` (histórico, um registro por checagem por execução). Serve de
# MAGIC evidência para a disciplina de Governança de Dados.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

execucao = agora_utc()
resultados = []


def checar(nome, ok, detalhe="", critica=True):
    resultados.append((nome, bool(ok), str(detalhe), critica))
    print(("OK   " if ok else ("FALHA" if critica else "AVISO")), nome, detalhe)


b = spark.table(f"{SCHEMA_BRONZE}.eventos")
p = spark.table(f"{SCHEMA_PRATA}.eventos")
r = spark.table(f"{SCHEMA_PRATA}.eventos_rejeitados")
n_b, n_p, n_r = b.count(), p.count(), r.count()

checar("bronze tem eventos", n_b > 0, n_b)
checar("prata tem eventos", n_p > 0, n_p)
taxa_rej = n_r / n_b if n_b else 0
checar("rejeitados abaixo de 5%", taxa_rej < 0.05, f"{taxa_rej:.2%}", critica=False)
checar("evento_id único na prata", p.groupBy("evento_id").count().filter("count > 1").count() == 0)

# LGPD: nenhuma coluna de identificação direta nas camadas prata e ouro.
proibidas = {"aluno_id", "profissional_id", "nome", "email", "e_mail", "nascimento", "codigo_vinculo", "texto"}
for schema in (SCHEMA_PRATA, SCHEMA_OURO):
    for t in spark.catalog.listTables(schema):
        if t.name == "qualidade_execucoes":
            continue
        cols = set(spark.table(f"{schema}.{t.name}").columns)
        vazou = cols & proibidas
        checar(f"sem identificador direto em {schema}.{t.name}", not vazou, ", ".join(sorted(vazou)))

checar("aluno_hash com 64 caracteres",
       p.filter(F.length("aluno_hash") != 64).count() == 0)

k = spark.table(f"{SCHEMA_OURO}.kpi_aluno_semana")
checar("adesão à dieta entre 0 e 1", k.filter((F.col("adesao_dieta") < 0) | (F.col("adesao_dieta") > 1)).count() == 0)
checar("no máximo 7 treinos por semana", k.filter(F.col("treinos_semana") > 7).count() == 0)
checar("uma linha por aluno e semana",
       k.groupBy("aluno_hash", "semana_inicio").count().filter("count > 1").count() == 0)

fd = spark.table(f"{SCHEMA_OURO}.fato_aluno_dia")
orfaos = fd.join(spark.table(f"{SCHEMA_OURO}.dim_aluno"), "aluno_hash", "left_anti").count()
checar("todo fato_aluno_dia tem aluno na dimensão", orfaos == 0, orfaos)
sem_data = fd.join(spark.table(f"{SCHEMA_OURO}.dim_data"), "data_key", "left_anti").count()
checar("todo fato_aluno_dia tem data na dimensão", sem_data == 0, sem_data)

mistura = fd.groupBy("aluno_hash").agg(F.countDistinct("_origem").alias("n")).filter("n > 1").count()
checar("nenhum aluno mistura origem sintética e real", mistura == 0, mistura)

# COMMAND ----------

hist = spark.createDataFrame(resultados, "checagem string, ok boolean, detalhe string, critica boolean") \
    .withColumn("execucao_em", F.lit(execucao).cast("timestamp"))
(hist.write.format(FORMATO_TABELA).mode("append").saveAsTable(f"{SCHEMA_OURO}.qualidade_execucoes"))

falhas = [r for r in resultados if r[3] and not r[1]]
if falhas:
    raise AssertionError("Checagens críticas falharam: " + "; ".join(f[0] for f in falhas))
print("Todas as checagens críticas passaram.")
