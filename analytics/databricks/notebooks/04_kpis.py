# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Ouro: KPIs
# MAGIC
# MAGIC Definições de `analytics/docs/kpis.md` (catálogo de KPIs). Grão base: **aluno × semana**
# MAGIC (segunda a domingo), incluindo semanas sem nenhum registro depois que o aluno começou, que é
# MAGIC o que permite medir abandono.
# MAGIC
# MAGIC | tabela | grão |
# MAGIC | --- | --- |
# MAGIC | `kpi_aluno_semana` | aluno × semana |
# MAGIC | `kpi_profissional_semana` | personal × semana × origem |
# MAGIC | `kpi_semana` | semana × origem (base inteira) |
# MAGIC | `kpi_retencao_coorte` | coorte (semana do primeiro evento) × origem: retenção D7 e D30 |
# MAGIC
# MAGIC Escolhas registradas (onde o catálogo deixa espaço):
# MAGIC
# MAGIC * Semanas parciais (a primeira do aluno e a semana corrente) usam como denominador só os dias em
# MAGIC   que o aluno já estava no app (`dias_no_periodo`); a coluna `semana_completa` permite filtrar.
# MAGIC * Refeições previstas = `refeicoes_por_dia` (padrão 5, sem plano) × `dias_no_periodo`.
# MAGIC * Hidratação = média de copos nos dias em que o aluno registrou água.
# MAGIC * Sequência = dias seguidos com registro terminando no último dia da semana (0 se não registrou nele).
# MAGIC * Variação de peso = último peso − primeiro peso na janela das 4 semanas que terminam nesta.
# MAGIC * Retenção DN = aluno com algum evento exatamente N dias após o primeiro; só entra no
# MAGIC   denominador quem começou há pelo menos N dias.
# MAGIC * Alunos em risco: colunas `prob_desistencia` e `em_risco` ficam nulas até o modelo da
# MAGIC   disciplina de Machine Learning gravar a previsão.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import Window

dia_ = spark.table(f"{SCHEMA_OURO}.fato_aluno_dia")
dim_aluno = spark.table(f"{SCHEMA_OURO}.dim_aluno").filter(F.col("primeiro_dia").isNotNull())
dim_data = spark.table(f"{SCHEMA_OURO}.dim_data")
peso = spark.table(f"{SCHEMA_OURO}.fato_peso_dia")
fato_ref = spark.table(f"{SCHEMA_OURO}.fato_refeicao")

data_ref = DATA_REFERENCIA or str(dim_data.agg(F.max("data")).first()[0])
DATA_REF = F.to_date(F.lit(data_ref))

# COMMAND ----------

# Esqueleto aluno × semana: da semana do primeiro dia até a semana da data de referência.
semanas = dim_data.select("semana_inicio").distinct()
base = (dim_aluno.select("aluno_hash", "primeiro_dia", "personal_hash", "_origem")
    .join(semanas, F.col("semana_inicio") >= F.date_trunc("week", "primeiro_dia").cast("date"))
    .withColumn("semana_fim", F.date_add("semana_inicio", 6))
    .withColumn("inicio_efetivo", F.greatest("semana_inicio", "primeiro_dia"))
    .withColumn("fim_efetivo", F.least("semana_fim", DATA_REF))
    .withColumn("dias_no_periodo", F.datediff("fim_efetivo", "inicio_efetivo") + 1)
    .withColumn("semana_completa", F.col("dias_no_periodo") == 7)
    .withColumn("semana_num", (F.datediff("semana_inicio", F.date_trunc("week", "primeiro_dia").cast("date")) / 7).cast("int") + 1))

dia_s = dia_.withColumn("semana_inicio", F.date_trunc("week", "dia").cast("date"))
agg_dia = dia_s.groupBy("aluno_hash", "semana_inicio").agg(
    F.sum(F.col("registrou").cast("int")).alias("dias_registrados"),
    F.sum("aberturas").alias("aberturas"),
    F.sum("refeicoes_ok").alias("refeicoes_ok"),
    F.sum("refeicoes_troca").alias("refeicoes_troca"),
    F.sum("refeicoes_puladas").alias("refeicoes_puladas"),
    F.sum("treino_feito").alias("treinos_semana"),
    F.sum("cardio_minutos").alias("cardio_minutos"),
    F.avg("agua_copos").alias("hidratacao_media_copos"),
    F.sum("doces_sos_vencidos").alias("doces_sos_vencidos"),
)

# Sequência: ilhas de dias consecutivos com registro.
reg = dia_.filter("registrou").select("aluno_hash", "dia")
w_seq = Window.partitionBy("aluno_hash").orderBy("dia")
reg = reg.withColumn("ilha", F.date_sub("dia", F.row_number().over(w_seq)))
reg = reg.withColumn("sequencia", F.row_number().over(Window.partitionBy("aluno_hash", "ilha").orderBy("dia")))
seq = reg.select("aluno_hash", F.col("dia").alias("fim_efetivo"), F.col("sequencia").alias("sequencia_fim_semana"))

# Peso: janela de 4 semanas terminando nesta semana.
w_p = Window.partitionBy("aluno_hash", "semana_inicio")
pesos_janela = (base.select("aluno_hash", "semana_inicio", "fim_efetivo")
    .join(peso.select("aluno_hash", "dia", "peso_kg"), "aluno_hash")
    .filter((F.col("dia") <= F.col("fim_efetivo")) & (F.col("dia") >= F.date_sub("semana_inicio", 21)))
    .withColumn("_primeiro", F.first("peso_kg").over(w_p.orderBy("dia")))
    .withColumn("_ultimo", F.first("peso_kg").over(w_p.orderBy(F.col("dia").desc())))
    .groupBy("aluno_hash", "semana_inicio").agg(
        F.first("_ultimo").alias("peso_ultimo_kg"),
        F.round(F.first("_ultimo") - F.first("_primeiro"), 2).alias("variacao_peso_4sem_kg")))

def razao(num, den):
    return F.round(F.try_divide(F.col(num), F.col(den)), 4)

kpi_aluno_semana = (base
    .join(agg_dia, ["aluno_hash", "semana_inicio"], "left")
    .join(seq, ["aluno_hash", "fim_efetivo"], "left")
    .join(pesos_janela, ["aluno_hash", "semana_inicio"], "left")
    .fillna(0, ["dias_registrados", "aberturas", "refeicoes_ok", "refeicoes_troca", "refeicoes_puladas",
                "treinos_semana", "cardio_minutos", "doces_sos_vencidos", "sequencia_fim_semana"])
    .withColumn("refeicoes_previstas", F.col("dias_no_periodo") * F.lit(REFEICOES_POR_DIA))
    .withColumn("refeicoes_seguidas", F.col("refeicoes_ok") + F.col("refeicoes_troca"))
    .withColumn("refeicoes_registradas", F.col("refeicoes_seguidas") + F.col("refeicoes_puladas"))
    .withColumn("adesao_dieta", F.least(F.lit(1.0), razao("refeicoes_seguidas", "refeicoes_previstas")))
    .withColumn("taxa_troca", razao("refeicoes_troca", "refeicoes_registradas"))
    .withColumn("treinos_meta", F.lit(TREINOS_META_SEMANA))
    .withColumn("cardio_meta_minutos", F.lit(CARDIO_META_SEMANA))
    .withColumn("dias_registrados_pct", razao("dias_registrados", "dias_no_periodo"))
    .withColumn("semana_ativa", F.col("dias_registrados") > 0)
    .withColumn("bateu_dieta", F.col("adesao_dieta") >= 0.8)
    .withColumn("bateu_treino", F.col("treinos_semana") >= F.col("treinos_meta"))
    .withColumn("bateu_cardio", F.col("cardio_minutos") >= F.col("cardio_meta_minutos"))
    .withColumn("prob_desistencia", F.lit(None).cast("double"))
    .withColumn("em_risco", F.lit(None).cast("boolean"))
    .select("aluno_hash", "semana_inicio", "semana_fim", "semana_num", "dias_no_periodo", "semana_completa",
            "personal_hash", "semana_ativa", "dias_registrados", "dias_registrados_pct", "aberturas",
            "refeicoes_previstas", "refeicoes_ok", "refeicoes_troca", "refeicoes_puladas", "refeicoes_registradas",
            "adesao_dieta", "taxa_troca", "bateu_dieta", "treinos_semana", "treinos_meta", "bateu_treino",
            "cardio_minutos", "cardio_meta_minutos", "bateu_cardio",
            F.round("hidratacao_media_copos", 2).alias("hidratacao_media_copos"),
            "sequencia_fim_semana", "peso_ultimo_kg", "variacao_peso_4sem_kg", "doces_sos_vencidos",
            "prob_desistencia", "em_risco", "_origem"))
salvar(kpi_aluno_semana, SCHEMA_OURO, "kpi_aluno_semana", "KPIs por aluno e semana (catálogo analytics/docs/kpis.md).")

# COMMAND ----------

k = spark.table(f"{SCHEMA_OURO}.kpi_aluno_semana")


def agregar(df, grupos):
    return df.groupBy(*grupos).agg(
        F.countDistinct("aluno_hash").alias("alunos"),
        F.sum(F.col("semana_ativa").cast("int")).alias("alunos_semana_ativa"),
        F.round(F.try_divide(F.sum("refeicoes_seguidas_"), F.sum("refeicoes_previstas")), 4).alias("adesao_dieta"),
        F.round(F.try_divide(F.sum("refeicoes_troca"), F.sum("refeicoes_registradas")), 4).alias("taxa_troca"),
        F.round(F.avg("treinos_semana"), 2).alias("treinos_semana_media"),
        F.round(F.avg("cardio_minutos"), 1).alias("cardio_minutos_media"),
        F.round(F.avg("hidratacao_media_copos"), 2).alias("hidratacao_media_copos"),
        F.round(F.avg("dias_registrados_pct"), 4).alias("dias_registrados_pct"),
        F.round(F.avg("sequencia_fim_semana"), 2).alias("sequencia_media"),
        F.round(F.avg("variacao_peso_4sem_kg"), 2).alias("variacao_peso_4sem_kg_media"),
        F.round(F.avg(F.col("bateu_dieta").cast("int")), 4).alias("pct_bateu_dieta"),
        F.round(F.avg(F.col("bateu_treino").cast("int")), 4).alias("pct_bateu_treino"),
        F.round(F.avg(F.col("bateu_cardio").cast("int")), 4).alias("pct_bateu_cardio"),
        F.sum(F.coalesce(F.col("em_risco").cast("int"), F.lit(0))).alias("alunos_em_risco"),
    )


k2 = k.withColumn("refeicoes_seguidas_", F.col("refeicoes_ok") + F.col("refeicoes_troca"))
salvar(agregar(k2, ["semana_inicio", "_origem"]).orderBy("semana_inicio"), SCHEMA_OURO, "kpi_semana",
       "KPIs da base inteira por semana e origem.")
salvar(agregar(k2.filter(F.col("personal_hash").isNotNull()), ["personal_hash", "semana_inicio", "_origem"]),
       SCHEMA_OURO, "kpi_profissional_semana", "KPIs dos alunos de cada personal por semana.")

# COMMAND ----------

# Retenção D7 / D30 por coorte (semana do primeiro evento).
dias_ativos = dia_.select("aluno_hash", "dia").distinct()
coorte = dim_aluno.select("aluno_hash", "primeiro_dia", "coorte_semana", "_origem")
for n in (7, 30):
    alvo = dias_ativos.select("aluno_hash", F.col("dia").alias(f"_d{n}"))
    coorte = (coorte.withColumn(f"_alvo{n}", F.date_add("primeiro_dia", n))
        .join(alvo, (alvo.aluno_hash == coorte.aluno_hash) & (F.col(f"_d{n}") == F.col(f"_alvo{n}")), "left")
        .drop(alvo.aluno_hash)
        .withColumn(f"elegivel_d{n}", F.col(f"_alvo{n}") <= DATA_REF)
        .withColumn(f"retido_d{n}", F.col(f"_d{n}").isNotNull()))

kpi_retencao = (coorte.groupBy("coorte_semana", "_origem").agg(
        F.count("*").alias("alunos"),
        F.sum(F.col("elegivel_d7").cast("int")).alias("elegiveis_d7"),
        F.sum((F.col("elegivel_d7") & F.col("retido_d7")).cast("int")).alias("retidos_d7"),
        F.sum(F.col("elegivel_d30").cast("int")).alias("elegiveis_d30"),
        F.sum((F.col("elegivel_d30") & F.col("retido_d30")).cast("int")).alias("retidos_d30"))
    .withColumn("retencao_d7", razao("retidos_d7", "elegiveis_d7"))
    .withColumn("retencao_d30", razao("retidos_d30", "elegiveis_d30"))
    .orderBy("coorte_semana"))
salvar(kpi_retencao, SCHEMA_OURO, "kpi_retencao_coorte", "Retenção D7 e D30 por coorte semanal.")

# COMMAND ----------

display(spark.table(f"{SCHEMA_OURO}.kpi_semana").orderBy("semana_inicio"))
