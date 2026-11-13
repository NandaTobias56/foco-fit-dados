# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata: limpa, tipa, deduplica e pseudonimiza
# MAGIC
# MAGIC * `eventos`: tipos do contrato v1, props viram colunas tipadas, duplicatas saem, `aluno_id`
# MAGIC   vira `aluno_hash` (SHA-256 com sal secreto). O texto livre das promessas não passa daqui.
# MAGIC * `eventos_rejeitados`: o que falhou nas regras, com o motivo (vira indicador de qualidade).
# MAGIC * `alunos` e `profissionais`: a partir de `profiles`, só com hash, papel, ano e faixa etária.
# MAGIC * `vinculos`: aluno e profissional como hash; aceita `estado` (contrato) ou `status` (Supabase).
# MAGIC
# MAGIC Deduplicação: primeiro por `id`; depois por conteúdo (mesmo aluno, tipo, instante, sessão e
# MAGIC props), que é o caso de um envio em lote repetido pela fila offline do app.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import Window

ev = spark.table(f"{SCHEMA_BRONZE}.eventos")


def _ts(c):
    """Aceita ISO 8601 com ou sem fuso; sem fuso é lido no fuso da sessão (America/Sao_Paulo).
    Usa as funções try_* porque o Databricks roda em modo ANSI: valor ruim vira nulo e é rejeitado."""
    return F.try_to_timestamp(c)


def _data(c):
    return F.to_date(F.try_to_timestamp(F.substring(c, 1, 10), F.lit("yyyy-MM-dd")))


def _int(nome):
    return F.expr(f"try_cast({nome} as int)")


props = F.from_json(F.coalesce(F.col("props"), F.lit("{}")), SCHEMA_PROPS)

tipado = (ev
    .withColumn("p", props)
    .select(
        F.lower(F.trim("id")).alias("evento_id"),
        F.lower(F.trim("aluno_id")).alias("_aluno_id"),
        F.lower(F.trim("tipo")).alias("tipo"),
        _data(F.col("dia")).alias("dia"),
        _ts(F.col("ocorrido_em")).alias("ocorrido_em"),
        _ts(F.col("recebido_em")).alias("recebido_em"),
        F.lower(F.trim("plataforma")).alias("plataforma"),
        _int("app_versao").alias("app_versao"),
        F.lower(F.trim("sessao_id")).alias("sessao_id"),
        F.col("p.refeicao_id").alias("refeicao_id"),
        F.lower(F.col("p.status")).alias("refeicao_status"),
        F.col("p.grupo").alias("treino_grupo"),
        F.col("p.minutos").alias("cardio_minutos"),
        F.col("p.delta").alias("agua_delta"),
        F.col("p.total_dia").alias("agua_total_dia"),
        F.col("p.kg").alias("peso_kg"),
        F.col("p.premio").alias("bau_premio"),
        F.length("p.texto").alias("promessa_tamanho"),
        F.col("p.tipo").alias("notificacao_tipo"),
        F.col("p.acao").alias("notificacao_acao"),
        F.col("p.quantidade").alias("notificacao_quantidade"),
        F.lower(F.col("p.tela")).alias("tela"),
        F.lower(F.col("p.origem")).alias("tela_origem"),
        F.lower(F.col("p.etapa")).alias("cadastro_etapa"),
        F.col("p.novo").alias("cadastro_novo"),
        F.col("p.experimento").alias("experimento"),
        F.upper(F.col("p.grupo")).alias("experimento_grupo"),
        F.coalesce(F.col("p.sintetico"), F.lit(False)).alias("sintetico"),
        F.col("props").alias("_props_json"),
        "_origem", "_ingerido_em",
    ))

# COMMAND ----------

# Os eventos tipados ficam numa tabela de passagem na bronze (que já guarda o dado cru) antes da
# validação. Sem essa parada, o Spark junta a tipagem e as 17 regras numa expressão só e estoura a
# memória do otimizador.
tipado = spark.table(salvar(tipado, SCHEMA_BRONZE, "_eventos_tipados",
                            "Passagem interna da prata: eventos tipados antes da validação."))

# COMMAND ----------

# Regras de validação. Cada regra devolve o motivo quando falha.
regras = [
    (F.col("evento_id").isNull(), "sem id"),
    (F.col("_aluno_id").isNull(), "sem aluno"),
    (~F.col("tipo").isin(TIPOS_EVENTO) | F.col("tipo").isNull(), "tipo fora do contrato"),
    (F.col("dia").isNull(), "dia inválido"),
    (F.col("ocorrido_em").isNull(), "ocorrido_em inválido"),
    (F.col("plataforma").isNotNull() & ~F.col("plataforma").isin("android", "web"), "plataforma inválida"),
    ((F.col("tipo") == "refeicao_registrada") &
     (F.col("refeicao_id").isNull() | ~F.coalesce(F.col("refeicao_status"), F.lit("")).isin("ok", "troca", "pulei")),
     "refeição sem id ou status"),
    ((F.col("tipo") == "refeicao_desfeita") & F.col("refeicao_id").isNull(), "refeição desfeita sem id"),
    ((F.col("tipo") == "cardio_registrado") &
     (F.col("cardio_minutos").isNull() | (F.col("cardio_minutos") <= 0) | (F.col("cardio_minutos") > 600)),
     "minutos de cardio fora de 1..600"),
    ((F.col("tipo") == "agua_copo") &
     (~F.coalesce(F.col("agua_delta"), F.lit(0)).isin(1, -1) | F.col("agua_total_dia").isNull() |
      (F.col("agua_total_dia") < 0) | (F.col("agua_total_dia") > 40)),
     "copo de água inválido"),
    ((F.col("tipo") == "peso_registrado") &
     (F.col("peso_kg").isNull() | (F.col("peso_kg") < 25) | (F.col("peso_kg") > 350)),
     "peso fora de 25..350 kg"),
    # v2
    ((F.col("tipo") == "tela_vista") & F.col("tela").isNull(), "tela vista sem tela"),
    ((F.col("tipo") == "cadastro_etapa") & F.col("cadastro_etapa").isNull(), "etapa do cadastro sem etapa"),
    ((F.col("tipo") == "notificacao_agendada") &
     (F.col("notificacao_tipo").isNull() | F.col("notificacao_quantidade").isNull() | (F.col("notificacao_quantidade") < 0)),
     "aviso agendado sem tipo ou quantidade"),
    ((F.col("tipo") == "experimento_exposto") &
     (F.col("experimento").isNull() | ~F.coalesce(F.col("experimento_grupo"), F.lit("")).isin("A", "B")),
     "experimento sem nome ou grupo A/B"),
    # O dia local pode diferir da data UTC em até um dia; mais que isso é relógio errado.
    (F.abs(F.datediff(F.col("dia"), F.to_date("ocorrido_em"))) > 1, "dia incoerente com ocorrido_em"),
]
# A primeira regra que falha dá o motivo.
motivo = F.coalesce(*[F.when(F.coalesce(cond, F.lit(False)), F.lit(texto)) for cond, texto in regras])

validado = tipado.withColumn("motivo_rejeicao", motivo)

# COMMAND ----------

# Deduplicação. 1) mesmo id: fica o recebido mais recente. 2) mesmo conteúdo: fica o primeiro recebido.
w_id = Window.partitionBy("evento_id").orderBy(F.col("recebido_em").desc_nulls_last(), F.col("_ingerido_em").desc())
w_conteudo = (Window.partitionBy("_aluno_id", "tipo", "ocorrido_em", "sessao_id", "_props_json")
              .orderBy(F.col("recebido_em").asc_nulls_last(), F.col("evento_id")))

validos = (validado.filter(F.col("motivo_rejeicao").isNull())
    .withColumn("_n", F.row_number().over(w_id)).filter("_n = 1").drop("_n")
    .withColumn("_n", F.row_number().over(w_conteudo)).filter("_n = 1").drop("_n"))

eventos_prata = (validos
    .withColumn("aluno_hash", hash_id(F.col("_aluno_id")))
    .withColumn("hora_local", F.hour("ocorrido_em"))
    # a prop `grupo` é o grupo muscular no treino e o grupo A/B no experimento
    .withColumn("treino_grupo", F.when(F.col("tipo") != "experimento_exposto", F.col("treino_grupo")))
    .withColumn("experimento_grupo", F.when(F.col("tipo") == "experimento_exposto", F.col("experimento_grupo")))
    .drop("_aluno_id", "_props_json", "motivo_rejeicao")
    .select("evento_id", "aluno_hash", "tipo", "dia", "ocorrido_em", "recebido_em", "hora_local",
            "plataforma", "app_versao", "sessao_id", "refeicao_id", "refeicao_status", "treino_grupo",
            "cardio_minutos", "agua_delta", "agua_total_dia", "peso_kg", "bau_premio", "promessa_tamanho",
            "notificacao_tipo", "notificacao_acao", "notificacao_quantidade", "tela", "tela_origem",
            "cadastro_etapa", "cadastro_novo", "experimento", "experimento_grupo",
            "sintetico", "_origem", "_ingerido_em"))

rejeitados = (validado.filter(F.col("motivo_rejeicao").isNotNull())
    .withColumn("aluno_hash", hash_id(F.col("_aluno_id")))
    .select("evento_id", "aluno_hash", "tipo", "dia", "ocorrido_em", "motivo_rejeicao", "_origem", "_ingerido_em"))

salvar(eventos_prata, SCHEMA_PRATA, "eventos",
       "Eventos limpos, tipados, deduplicados; aluno pseudonimizado (aluno_hash).")
salvar(rejeitados, SCHEMA_PRATA, "eventos_rejeitados", "Eventos que falharam nas regras da prata, com motivo.")

# COMMAND ----------

# Perfis: só o necessário para análise. Data de nascimento vira ano e faixa etária.
pf = spark.table(f"{SCHEMA_BRONZE}.profiles")
w_pf = Window.partitionBy("id").orderBy(F.col("_ingerido_em").desc(), F.col("_origem"))
pf = (pf.withColumn("id", F.lower(F.trim("id")))
        .filter(F.col("id").isNotNull())
        .withColumn("_n", F.row_number().over(w_pf)).filter("_n = 1")
        .withColumn("nasc", _data(F.col("nascimento")))
        .withColumn("idade", F.floor(F.months_between(F.current_date(), F.col("nasc")) / 12).cast("int"))
        .withColumn("papel", F.lower(F.trim("papel"))))

faixa = (F.when(F.col("idade").isNull(), "não informado")
          .when(F.col("idade") < 18, "<18").when(F.col("idade") < 25, "18-24")
          .when(F.col("idade") < 35, "25-34").when(F.col("idade") < 45, "35-44")
          .when(F.col("idade") < 55, "45-54").otherwise("55+"))

alunos = (pf.filter(F.col("papel") == "aluno")
    .select(hash_id(F.col("id")).alias("aluno_hash"),
            F.year("nasc").alias("ano_nascimento"), faixa.alias("faixa_etaria"),
            _ts(F.col("criado_em")).alias("criado_em"), "_origem"))
prof_extra = (spark.table(f"{SCHEMA_BRONZE}.profissionais")
    .withColumn("id", F.lower(F.trim("id"))).dropDuplicates(["id"])
    .select("id", F.upper(F.trim("conselho")).alias("conselho"), F.upper(F.trim("uf")).alias("uf"),
            F.lower(F.trim("status")).alias("status_aprovacao")))
profissionais = (pf.filter(F.col("papel").isin("personal", "nutri"))
    .join(prof_extra, "id", "left")
    .select(hash_id(F.col("id")).alias("profissional_hash"), F.col("papel"), "conselho", "uf", "status_aprovacao",
            _ts(F.col("criado_em")).alias("criado_em"), "_origem"))

salvar(alunos, SCHEMA_PRATA, "alunos", "Alunos pseudonimizados: hash, ano de nascimento, faixa etária.")
salvar(profissionais, SCHEMA_PRATA, "profissionais", "Personais e nutricionistas pseudonimizados.")

# COMMAND ----------

vc = spark.table(f"{SCHEMA_BRONZE}.vinculos")
w_vc = Window.partitionBy("aluno_id", "profissional_id", "criado_em").orderBy(F.col("_ingerido_em").desc())
vinculos = (vc
    .withColumn("aluno_id", F.lower(F.trim("aluno_id")))
    .withColumn("profissional_id", F.lower(F.trim("profissional_id")))
    .filter(F.col("aluno_id").isNotNull() & F.col("profissional_id").isNotNull())
    .withColumn("_n", F.row_number().over(w_vc)).filter("_n = 1")
    .select(hash_id(F.col("aluno_id")).alias("aluno_hash"),
            hash_id(F.col("profissional_id")).alias("profissional_hash"),
            F.lower(F.trim("tipo")).alias("tipo"),
            F.lower(F.trim(F.coalesce("estado", "status"))).alias("estado"),
            _ts(F.col("criado_em")).alias("criado_em"), "_origem"))

salvar(vinculos, SCHEMA_PRATA, "vinculos", "Vínculos aluno x profissional, ambos como hash.")

# COMMAND ----------

display(spark.table(f"{SCHEMA_PRATA}.eventos_rejeitados").groupBy("motivo_rejeicao").count())
