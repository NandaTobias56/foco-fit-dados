# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro: modelo estrela
# MAGIC
# MAGIC ```
# MAGIC                 dim_data            dim_tipo_treino
# MAGIC                    |                      |
# MAGIC  dim_aluno ── fato_refeicao · fato_treino_dia · fato_cardio_dia · fato_agua_dia · fato_peso_dia
# MAGIC     |                      \______ fato_aluno_dia (visão larga, uma linha por aluno e dia)
# MAGIC  dim_profissional
# MAGIC ```
# MAGIC
# MAGIC Grão dos fatos:
# MAGIC
# MAGIC | fato | grão | medida principal |
# MAGIC | --- | --- | --- |
# MAGIC | `fato_refeicao` | aluno × dia × refeição | status final (`ok`, `troca`, `pulei`), já descontando `refeicao_desfeita` |
# MAGIC | `fato_treino_dia` | aluno × dia | treino feito (líquido de `treino_desfeito`) e grupo |
# MAGIC | `fato_cardio_dia` | aluno × dia | minutos de cardio |
# MAGIC | `fato_agua_dia` | aluno × dia | copos (último `total_dia` do dia) |
# MAGIC | `fato_peso_dia` | aluno × dia | último peso do dia |
# MAGIC | `fato_aluno_dia` | aluno × dia | todas as medidas lado a lado, para BI e para o modelo de ML |
# MAGIC
# MAGIC Todas as tabelas levam `_origem`, para nunca misturar sintético e real sem perceber.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import Window

ev = spark.table(f"{SCHEMA_PRATA}.eventos")
alunos_prata = spark.table(f"{SCHEMA_PRATA}.alunos")
prof_prata = spark.table(f"{SCHEMA_PRATA}.profissionais")
vinc_prata = spark.table(f"{SCHEMA_PRATA}.vinculos")

data_ref = DATA_REFERENCIA or ev.agg(F.max("dia")).first()[0]
if data_ref is None:
    raise ValueError("Prata sem eventos válidos: rode 01_bronze e 02_prata antes.")
data_ref = str(data_ref)
DATA_REF = F.to_date(F.lit(data_ref))
print("Data de referência:", data_ref)
ev = ev.filter(F.col("dia") <= DATA_REF)


def data_key(c):
    return F.date_format(c, "yyyyMMdd").cast("int")

# COMMAND ----------

# MAGIC %md ## Dimensões

# COMMAND ----------

dia_min = ev.agg(F.min("dia")).first()[0]
nomes_dia = F.element_at(F.array(*[F.lit(n) for n in
    ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]]), F.col("dia_semana"))
nomes_mes = F.element_at(F.array(*[F.lit(n) for n in
    ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto",
     "setembro", "outubro", "novembro", "dezembro"]]), F.month("data"))

dim_data = (spark.sql(f"select explode(sequence(date'{dia_min}', date'{data_ref}', interval 1 day)) as data")
    .withColumn("dia_semana", ((F.dayofweek("data") + 5) % 7) + 1)  # 1 = segunda ... 7 = domingo
    .select(data_key(F.col("data")).alias("data_key"), "data",
            F.year("data").alias("ano"), F.quarter("data").alias("trimestre"), F.month("data").alias("mes"),
            nomes_mes.alias("nome_mes"), F.dayofmonth("data").alias("dia_mes"), "dia_semana",
            nomes_dia.alias("nome_dia"), F.date_trunc("week", "data").cast("date").alias("semana_inicio"),
            F.weekofyear("data").alias("semana_iso"), (F.col("dia_semana") >= 6).alias("fim_de_semana")))
salvar(dim_data, SCHEMA_OURO, "dim_data", "Calendário (semana de segunda a domingo).")

# COMMAND ----------

# Tipos de treino: grupos musculares do app (src/exercises.js) mais o que aparecer nos dados.
grupos_app = [
    ("gluteo", "Glúteos", "inferiores"), ("quadriceps", "Quadríceps", "inferiores"),
    ("posterior", "Posterior", "inferiores"), ("panturrilha", "Panturrilha", "inferiores"),
    ("costas", "Costas", "superiores"), ("peito", "Peito", "superiores"), ("ombro", "Ombro", "superiores"),
    ("triceps", "Tríceps", "superiores"), ("biceps", "Bíceps", "superiores"), ("abdomen", "Abdômen", "core"),
    ("corpo_todo", "Corpo todo", "corpo todo"), ("nao_informado", "Não informado", "não informado"),
]
SINONIMOS = {"gluteos": "gluteo", "ombros": "ombro", "abdominal": "abdomen", "abdome": "abdomen",
             "quadricipes": "quadriceps", "biceps_femoral": "posterior", "corpo_inteiro": "corpo_todo"}


def grupo_id(c):
    g = F.regexp_replace(F.lower(F.trim(c)), r"[\s\-]+", "_")
    g = F.translate(g, "áàâãéêíóôõúç", "aaaaeeiooouc")
    mapa = F.create_map(*[F.lit(x) for kv in SINONIMOS.items() for x in kv])
    return F.when(g.isNull() | (g == ""), F.lit("nao_informado")).otherwise(F.coalesce(F.element_at(mapa, g), g))


seed = spark.createDataFrame(grupos_app, "grupo_id string, nome_grupo string, regiao string")
observados = (ev.filter(F.col("tipo") == "treino_feito").select(grupo_id(F.col("treino_grupo")).alias("grupo_id"))
    .distinct().join(seed, "grupo_id", "left_anti")
    .select("grupo_id", F.initcap(F.regexp_replace("grupo_id", "_", " ")).alias("nome_grupo"), F.lit("misto").alias("regiao")))
dim_tipo_treino = seed.unionByName(observados)
salvar(dim_tipo_treino, SCHEMA_OURO, "dim_tipo_treino", "Grupos musculares do treino.")

# COMMAND ----------

origem_aluno = ev.groupBy("aluno_hash").agg(
    F.min("dia").alias("primeiro_dia"), F.max("dia").alias("ultimo_dia"), F.first("_origem").alias("_origem_ev"))

# Profissional atual = vínculo ativo mais recente de cada tipo.
vinc = (vinc_prata.join(prof_prata.select("profissional_hash", "papel"), "profissional_hash", "left")
    .withColumn("tipo", F.coalesce("tipo", "papel")))
w_v = Window.partitionBy("aluno_hash", "tipo").orderBy(F.col("criado_em").desc())
ativos = vinc.filter(F.col("estado") == "ativo").withColumn("_n", F.row_number().over(w_v)).filter("_n = 1")
personal = ativos.filter(F.col("tipo") == "personal").select("aluno_hash", F.col("profissional_hash").alias("personal_hash"))
nutri = ativos.filter(F.col("tipo") == "nutri").select("aluno_hash", F.col("profissional_hash").alias("nutri_hash"))

dim_aluno = (alunos_prata.join(origem_aluno, "aluno_hash", "full")
    .join(personal, "aluno_hash", "left").join(nutri, "aluno_hash", "left")
    .select("aluno_hash", F.coalesce("faixa_etaria", F.lit("não informado")).alias("faixa_etaria"),
            "ano_nascimento", "criado_em", "primeiro_dia", "ultimo_dia",
            F.date_trunc("week", "primeiro_dia").cast("date").alias("coorte_semana"),
            "personal_hash", "nutri_hash",
            (F.datediff(DATA_REF, F.col("ultimo_dia")) < 7).alias("ativo_ultimos_7d"),
            F.coalesce("_origem", "_origem_ev").alias("_origem")))
salvar(dim_aluno, SCHEMA_OURO, "dim_aluno", "Alunos (hash), faixa etária, coorte e profissionais atuais.")

alunos_ativos_prof = (ativos.groupBy("profissional_hash").agg(F.countDistinct("aluno_hash").alias("alunos_ativos")))
dim_profissional = (prof_prata.join(alunos_ativos_prof, "profissional_hash", "left")
    .select("profissional_hash", "papel", "conselho", "uf", "status_aprovacao", "criado_em",
            F.coalesce("alunos_ativos", F.lit(0)).alias("alunos_ativos"), "_origem"))
salvar(dim_profissional, SCHEMA_OURO, "dim_profissional", "Personais e nutricionistas (hash) e alunos ativos.")

# COMMAND ----------

# MAGIC %md ## Fatos

# COMMAND ----------

chaves = ["aluno_hash", "dia"]


def ultimo_por(df, particao):
    w = Window.partitionBy(*particao).orderBy(F.col("ocorrido_em").desc(), F.col("recebido_em").desc_nulls_last(),
                                              F.col("evento_id").desc())
    return df.withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n")


# Refeição: o último evento do dia para aquela refeição decide (registrada ou desfeita).
ref = ev.filter(F.col("tipo").isin("refeicao_registrada", "refeicao_desfeita"))
fato_refeicao = (ultimo_por(ref, chaves + ["refeicao_id"])
    .filter(F.col("tipo") == "refeicao_registrada")
    .join(ref.groupBy(*chaves, "refeicao_id").agg(F.count("*").alias("eventos")), chaves + ["refeicao_id"])
    .select("aluno_hash", data_key(F.col("dia")).alias("data_key"), "dia", "refeicao_id",
            F.col("refeicao_status").alias("status"),
            F.col("refeicao_status").isin("ok", "troca").cast("int").alias("seguiu"),
            F.col("hora_local").alias("hora_registro"), "eventos", "_origem"))
salvar(fato_refeicao, SCHEMA_OURO, "fato_refeicao", "Status final de cada refeição por aluno e dia.")

tr = ev.filter(F.col("tipo").isin("treino_feito", "treino_desfeito"))
grupo_do_dia = (ultimo_por(tr.filter(F.col("tipo") == "treino_feito"), chaves)
    .select(*chaves, grupo_id(F.col("treino_grupo")).alias("grupo_id")))
fato_treino_dia = (ultimo_por(tr, chaves)
    .filter(F.col("tipo") == "treino_feito")
    .join(grupo_do_dia, chaves, "left")
    .select("aluno_hash", data_key(F.col("dia")).alias("data_key"), "dia", "grupo_id",
            F.lit(1).alias("treino_feito"), F.col("hora_local").alias("hora_registro"), "_origem"))
salvar(fato_treino_dia, SCHEMA_OURO, "fato_treino_dia", "Dias com treino feito (líquido de desfeito) e grupo.")

fato_cardio_dia = (ev.filter(F.col("tipo") == "cardio_registrado")
    .groupBy(*chaves).agg(F.sum("cardio_minutos").alias("minutos"), F.count("*").alias("sessoes"),
                          F.first("_origem").alias("_origem"))
    .select("aluno_hash", data_key(F.col("dia")).alias("data_key"), "dia", "minutos", "sessoes", "_origem"))
salvar(fato_cardio_dia, SCHEMA_OURO, "fato_cardio_dia", "Minutos de cardio por aluno e dia.")

ag = ev.filter(F.col("tipo") == "agua_copo")
fato_agua_dia = (ultimo_por(ag, chaves)
    .join(ag.groupBy(*chaves).agg(F.count("*").alias("toques")), chaves)
    .select("aluno_hash", data_key(F.col("dia")).alias("data_key"), "dia",
            F.col("agua_total_dia").alias("copos"), "toques", "_origem"))
salvar(fato_agua_dia, SCHEMA_OURO, "fato_agua_dia", "Copos de água por aluno e dia (último total do dia).")

fato_peso_dia = (ultimo_por(ev.filter(F.col("tipo") == "peso_registrado"), chaves)
    .select("aluno_hash", data_key(F.col("dia")).alias("data_key"), "dia", "peso_kg", "_origem"))
salvar(fato_peso_dia, SCHEMA_OURO, "fato_peso_dia", "Último peso registrado no dia.")

# COMMAND ----------

outros = (ev.groupBy(*chaves).agg(
    F.first("_origem").alias("_origem"),
    F.sum((F.col("tipo") == "app_aberto").cast("int")).alias("aberturas"),
    F.sum((F.col("tipo") != "app_aberto").cast("int")).alias("eventos_registro"),
    F.sum((F.col("tipo") == "doce_sos_vencido").cast("int")).alias("doces_sos_vencidos"),
    F.sum((F.col("tipo") == "bau_aberto").cast("int")).alias("baus_abertos"),
    F.sum((F.col("tipo") == "promessa_feita").cast("int")).alias("promessas"),
    F.sum((F.col("tipo") == "notificacao_tocada").cast("int")).alias("notificacoes_tocadas"),
    F.countDistinct("sessao_id").alias("sessoes_app")))

refeicoes_dia = fato_refeicao.groupBy(*chaves).agg(
    F.sum((F.col("status") == "ok").cast("int")).alias("refeicoes_ok"),
    F.sum((F.col("status") == "troca").cast("int")).alias("refeicoes_troca"),
    F.sum((F.col("status") == "pulei").cast("int")).alias("refeicoes_puladas"))

fato_aluno_dia = (outros
    .join(refeicoes_dia, chaves, "left")
    .join(fato_treino_dia.select(*chaves, "treino_feito", "grupo_id"), chaves, "left")
    .join(fato_cardio_dia.select(*chaves, F.col("minutos").alias("cardio_minutos")), chaves, "left")
    .join(fato_agua_dia.select(*chaves, F.col("copos").alias("agua_copos")), chaves, "left")
    .join(fato_peso_dia.select(*chaves, "peso_kg"), chaves, "left")
    .fillna(0, ["refeicoes_ok", "refeicoes_troca", "refeicoes_puladas", "treino_feito", "cardio_minutos"])
    .withColumn("registrou", F.col("eventos_registro") > 0)
    .withColumn("data_key", data_key(F.col("dia")))
    .select("aluno_hash", "data_key", "dia", "registrou", "aberturas", "sessoes_app", "eventos_registro",
            "refeicoes_ok", "refeicoes_troca", "refeicoes_puladas", "treino_feito", "grupo_id",
            "cardio_minutos", "agua_copos", "peso_kg", "doces_sos_vencidos", "baus_abertos", "promessas",
            "notificacoes_tocadas", "_origem"))
salvar(fato_aluno_dia, SCHEMA_OURO, "fato_aluno_dia", "Uma linha por aluno e dia com todas as medidas.")
