# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Ouro: jornada, funil e tagueamento
# MAGIC
# MAGIC Análise de **produto digital** sobre os eventos de navegação do app (contrato de eventos v1).
# MAGIC Responde às perguntas que um time de produto faz sobre qualquer app:
# MAGIC
# MAGIC * **Funil de ativação:** de quem criou a conta, quantos registram a primeira refeição, o primeiro
# MAGIC   treino, completam um dia e continuam ativos nas semanas 2 e 4? Onde está o maior vazamento?
# MAGIC * **Jornada:** o que o aluno faz depois de abrir o app? Quais caminhos levam a registrar algo e
# MAGIC   quais terminam sem ação (atrito)?
# MAGIC * **Notificação:** sessões que começam por notificação convertem mais que as diretas?
# MAGIC * **Atrito:** quanto o aluno desfaz, pula, registra atrasado ou abre e sai sem fazer nada?
# MAGIC * **Tagueamento:** os eventos chegam completos, em ordem, com sessão e versão? O plano de tags
# MAGIC   está sendo cumprido em cada versão e plataforma?
# MAGIC
# MAGIC | tabela | grão |
# MAGIC | --- | --- |
# MAGIC | `jornada_sessao` | sessão (abertura do app até 30 min parado) |
# MAGIC | `funil_ativacao` | origem × dimensão × segmento × etapa do funil |
# MAGIC | `jornada_transicoes` | origem × plataforma × evento → próximo evento na sessão |
# MAGIC | `jornada_caminhos` | origem × plataforma × caminho (4 primeiras ações da sessão) |
# MAGIC | `notificacao_efeito` | origem × entrada da sessão (notificação ou direta) |
# MAGIC | `notificacao_atribuicao` | origem × tipo × ação da notificação, com janela de 3 h |
# MAGIC | `atrito_registro` | origem × plataforma × tipo de dia (útil ou fim de semana) |
# MAGIC | `tagueamento_qualidade` | origem × plataforma × versão × tipo de evento |
# MAGIC | `tagueamento_checagens` | uma linha por regra do plano de tags, com status ok ou alerta |
# MAGIC | `funil_cadastro` | origem × etapa do cadastro (contrato v2) |
# MAGIC | `notificacao_taxa_toque` | origem × tipo de aviso × semana: agendados x tocados (contrato v2) |
# MAGIC | `experimento_resultado` | origem × experimento × grupo A/B, com teste t de Welch, sem e com CUPED (contrato v2) |
# MAGIC
# MAGIC Quase tudo é Spark SQL com window functions (LAG, LEAD, SUM acumulado, ROW_NUMBER), agregações
# MAGIC com FILTER, MIN_BY, PERCENTILE_APPROX e funções de array. As mesmas ideias, em consultas avulsas
# MAGIC para o SQL Editor, estão em `analytics/databricks/sql/case_funil_jornada.sql`.
# MAGIC
# MAGIC Escolhas registradas:
# MAGIC
# MAGIC * Sessão = `sessao_id` gravado pelo app. Evento sem `sessao_id` ganha uma sessão derivada pela regra
# MAGIC   de 30 minutos sem atividade (a mesma do app), e a tabela de checagens compara as duas contagens.
# MAGIC * Ações = `refeicao_registrada`, `treino_feito`, `cardio_registrado`, `agua_copo`, `peso_registrado`.
# MAGIC   Sessão que **converteu** = teve pelo menos uma ação. Sessão sem ação = "abriu e saiu".
# MAGIC * Funil só com alunos que têm 28 dias ou mais desde o cadastro (coorte madura), para a etapa
# MAGIC   "ativo na semana 4" não ser injusta com quem acabou de chegar.
# MAGIC * "Dia completo" = `bau_aberto`, porque o app só libera o baú quando o dia foi cumprido.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

P, O, B = SCHEMA_PRATA, SCHEMA_OURO, SCHEMA_BRONZE
ACOES = "('refeicao_registrada', 'treino_feito', 'cardio_registrado', 'agua_copo', 'peso_registrado')"
# Contrato v2: navegação (tela, cadastro) entra na jornada, mas não conta como ação do caminho;
# eventos de sistema (aviso agendado, exposição a experimento) ficam fora das sessões e vão para as
# tabelas próprias no fim do notebook.
NAVEGACAO = "('app_aberto', 'tela_vista', 'cadastro_etapa')"
SISTEMA = "('notificacao_agendada', 'experimento_exposto')"

data_ref = DATA_REFERENCIA or str(spark.table(f"{P}.eventos").agg(F.max("dia")).first()[0])
print("Data de referência:", data_ref)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eventos ordenados, com sessão
# MAGIC
# MAGIC Sessionização em SQL: `LAG` traz o evento anterior do aluno; se passou mais de 30 min, começa uma
# MAGIC sessão nova (`nova = 1`) e a `SUM` acumulada numera as sessões. Quem tem `sessao_id` do app fica
# MAGIC com ele.

# COMMAND ----------

spark.sql(f"""
create or replace temp view ev_sessao as
with ev as (
  select e.*, unix_timestamp(ocorrido_em) as t
  from {P}.eventos e
  where tipo not in {SISTEMA}
),
marcado as (
  select *,
         case when lag(t) over w is null or t - lag(t) over w > 1800 then 1 else 0 end as nova
  from ev
  window w as (partition by aluno_hash order by ocorrido_em, evento_id)
),
numerado as (
  select *,
         sum(nova) over (partition by aluno_hash order by ocorrido_em, evento_id
                         rows between unbounded preceding and current row) as sessao_30min
  from marcado
),
com_chave as (
  select *,
         coalesce(sessao_id, concat('d-', substr(aluno_hash, 1, 16), '-', sessao_30min)) as sessao
  from numerado
)
select *,
       min(case when tipo = 'app_aberto' then t end) over (partition by aluno_hash, sessao) as t_abertura,
       lead(tipo) over (partition by aluno_hash, sessao order by t, evento_id) as proximo_tipo,
       lead(t)    over (partition by aluno_hash, sessao order by t, evento_id) as proximo_t,
       lag(agua_total_dia) over (partition by aluno_hash, dia, tipo order by t, evento_id) as agua_total_anterior
from com_chave
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `jornada_sessao`

# COMMAND ----------

sessao = spark.sql(f"""
with s as (
  select aluno_hash, sessao, _origem,
         bool_or(sessao_id is null)                                   as sessao_derivada,
         min(plataforma)                                              as plataforma,
         min(app_versao)                                              as app_versao,
         min(ocorrido_em)                                             as inicio,
         max(ocorrido_em)                                             as fim,
         max(t) - min(t)                                              as duracao_seg,
         count(*)                                                     as eventos,
         count_if(tipo = 'app_aberto')                                as aberturas,
         count_if(tipo in {ACOES})                                    as acoes,
         count_if(tipo = 'refeicao_registrada')                       as refeicoes,
         min(t) filter (where tipo in {ACOES}) - min(t)               as seg_ate_primeira_acao,
         min_by(tipo, struct(t, evento_id)) filter (where tipo not in {NAVEGACAO}) as primeira_acao,
         min_by(notificacao_tipo, t) filter (where tipo = 'notificacao_tocada') as notificacao_tipo,
         min_by(notificacao_acao, t) filter (where tipo = 'notificacao_tocada') as notificacao_acao,
         array_join(slice(transform(
             array_sort(collect_list(struct(t, evento_id, tipo)) filter (where tipo not in {NAVEGACAO})),
             x -> x.tipo), 1, 4), ' > ')                              as caminho,
         array_join(slice(transform(
             array_sort(collect_list(struct(t, evento_id, tela)) filter (where tipo = 'tela_vista')),
             x -> x.tela), 1, 5), ' > ')                              as caminho_telas
  from ev_sessao
  group by aluno_hash, sessao, _origem
)
select s.aluno_hash, s.sessao as sessao_id, s.sessao_derivada, s._origem, s.plataforma, s.app_versao,
       to_date(s.inicio) as dia, s.inicio, s.fim, s.duracao_seg, hour(s.inicio) as hora_inicio,
       dayofweek(s.inicio) as dia_semana,
       case when dayofweek(s.inicio) in (1, 7) then 'fim_de_semana' else 'dia_util' end as tipo_dia,
       floor(datediff(to_date(s.inicio), a.primeiro_dia) / 7) as semana_desde_inicio,
       s.eventos, s.aberturas, s.acoes, s.refeicoes, s.acoes > 0 as converteu, s.seg_ate_primeira_acao,
       coalesce(s.primeira_acao, '(abriu e saiu)') as primeira_acao,
       case when s.primeira_acao = 'notificacao_tocada' then 'notificacao' else 'direta' end as entrada,
       case when s.primeira_acao = 'notificacao_tocada' then s.notificacao_tipo end as notificacao_tipo,
       case when s.primeira_acao = 'notificacao_tocada' then s.notificacao_acao end as notificacao_acao,
       case when s.caminho = '' then '(abriu e saiu)' else s.caminho end as caminho,
       nullif(s.caminho_telas, '') as caminho_telas
from s
left join {O}.dim_aluno a using (aluno_hash)
""")
salvar(sessao, O, "jornada_sessao", "Uma linha por sessão do app: duração, ações, entrada e caminho.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `funil_ativacao`
# MAGIC
# MAGIC Marcos de cada aluno → `stack` vira uma linha por etapa → um mesmo aluno entra em três recortes
# MAGIC (total, plataforma, personal) com `explode` → `LAG` sobre a etapa dá a conversão passo a passo.

# COMMAND ----------

funil = spark.sql(f"""
with plataforma_aluno as (
  select aluno_hash, plataforma
  from (select aluno_hash, plataforma,
               row_number() over (partition by aluno_hash order by count(*) desc, plataforma) as n
        from {P}.eventos group by aluno_hash, plataforma)
  where n = 1
),
base as (
  select a.aluno_hash, a._origem,
         coalesce(a.criado_em, cast(a.primeiro_dia as timestamp)) as cadastro,
         coalesce(p.plataforma, 'sem_evento') as plataforma,
         case when a.personal_hash is not null then 'com_personal' else 'sem_personal' end as personal
  from {O}.dim_aluno a
  left join plataforma_aluno p using (aluno_hash)
  where coalesce(to_date(a.criado_em), a.primeiro_dia) <= date_sub(date'{data_ref}', 28)
),
marcos as (
  select b.aluno_hash,
         min(e.ocorrido_em) filter (where e.tipo = 'app_aberto')          as m_abriu,
         min(e.ocorrido_em) filter (where e.tipo = 'refeicao_registrada') as m_refeicao,
         min(e.ocorrido_em) filter (where e.tipo = 'treino_feito')        as m_treino,
         min(e.ocorrido_em) filter (where e.tipo = 'bau_aberto')          as m_dia_completo,
         min(e.ocorrido_em) filter (where datediff(e.dia, to_date(b.cadastro)) between 7 and 13)  as m_semana2,
         min(e.ocorrido_em) filter (where datediff(e.dia, to_date(b.cadastro)) between 21 and 27) as m_semana4
  from base b left join {P}.eventos e using (aluno_hash)
  group by b.aluno_hash
),
etapas as (
  select b.*, stack(7,
           1, 'conta criada',          b.cadastro,
           2, 'abriu o app',           m.m_abriu,
           3, '1ª refeição',           m.m_refeicao,
           4, '1º treino',             m.m_treino,
           5, '1º dia completo (baú)', m.m_dia_completo,
           6, 'ativo na semana 2',     m.m_semana2,
           7, 'ativo na semana 4',     m.m_semana4) as (etapa_ordem, etapa, quando)
  from base b join marcos m using (aluno_hash)
),
recortes as (
  select e.*, r.dimensao, r.segmento
  from etapas e
  lateral view explode(array(named_struct('dimensao', 'total', 'segmento', 'todos'),
                             named_struct('dimensao', 'plataforma', 'segmento', e.plataforma),
                             named_struct('dimensao', 'personal', 'segmento', e.personal))) x as r
),
agregado as (
  select _origem, dimensao, segmento, etapa_ordem, etapa,
         count_if(quando is not null) as alunos,
         percentile_approx((unix_timestamp(quando) - unix_timestamp(cadastro)) / 3600.0, 0.5)
           filter (where quando is not null and etapa_ordem > 1) as horas_mediana_desde_cadastro
  from recortes
  group by _origem, dimensao, segmento, etapa_ordem, etapa
)
select *,
       try_divide(alunos, lag(alunos) over w)        as conversao_etapa,
       try_divide(alunos, first_value(alunos) over w) as conversao_topo,
       (lag(alunos) over w) - alunos                  as perdidos_na_etapa
from agregado
window w as (partition by _origem, dimensao, segmento order by etapa_ordem)
""")
salvar(funil, O, "funil_ativacao",
       "Funil de ativação (coorte com 28+ dias): conta → 1ª refeição → 1º treino → dia completo → semanas 2 e 4.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `jornada_transicoes` e `jornada_caminhos`

# COMMAND ----------

transicoes = spark.sql(f"""
select _origem, plataforma, tipo as evento, coalesce(proximo_tipo, '(fim da sessão)') as proximo_evento,
       count(*) as vezes,
       try_divide(count(*), sum(count(*)) over (partition by _origem, plataforma, tipo)) as probabilidade,
       percentile_approx(proximo_t - t, 0.5) as seg_mediana_ate_proximo
from ev_sessao
group by _origem, plataforma, tipo, coalesce(proximo_tipo, '(fim da sessão)')
""")
salvar(transicoes, O, "jornada_transicoes", "Matriz de transição: depois de cada evento, qual vem a seguir na sessão.")

caminhos = spark.sql(f"""
select * from (
  select _origem, plataforma, caminho,
         count(*) as sessoes,
         try_divide(count(*), sum(count(*)) over (partition by _origem, plataforma)) as pct_sessoes,
         avg(cast(converteu as int)) as taxa_conversao,
         percentile_approx(duracao_seg, 0.5) as duracao_mediana_seg,
         row_number() over (partition by _origem, plataforma order by count(*) desc, caminho) as ranking
  from {O}.jornada_sessao
  group by _origem, plataforma, caminho
) where ranking <= 25
""")
salvar(caminhos, O, "jornada_caminhos", "Os 25 caminhos mais comuns por plataforma (4 primeiras ações da sessão).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `notificacao_efeito`
# MAGIC
# MAGIC Só Android (a web não tem notificação). Compara sessões que começam tocando numa notificação com
# MAGIC as que começam direto. É uma comparação observacional, não um teste A/B: quem toca na notificação
# MAGIC pode já ser mais engajado. O desenho do teste A/B fica no backlog (sprint 4).

# COMMAND ----------

notificacao = spark.sql(f"""
with g as (
  select _origem, entrada, coalesce(notificacao_tipo, '-') as notificacao_tipo,
         coalesce(notificacao_acao, '-') as notificacao_acao,
         count(*) as sessoes,
         avg(cast(converteu as int)) as taxa_conversao,
         avg(cast(refeicoes > 0 as int)) as taxa_registro_refeicao,
         percentile_approx(seg_ate_primeira_acao, 0.5) as seg_mediana_ate_acao
  from {O}.jornada_sessao
  where plataforma = 'android'
  group by _origem, entrada, coalesce(notificacao_tipo, '-'), coalesce(notificacao_acao, '-')
)
select *,
       taxa_conversao - max(case when entrada = 'direta' then taxa_conversao end) over (partition by _origem)
         as diferenca_conversao_vs_direta,
       try_divide(taxa_conversao, max(case when entrada = 'direta' then taxa_conversao end)
                  over (partition by _origem)) as lift_vs_direta
from g
""")
salvar(notificacao, O, "notificacao_efeito", "Conversão de sessões que começam por notificação x diretas (Android).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `notificacao_atribuicao`: a janela muda a resposta
# MAGIC
# MAGIC Olhar só a sessão subestima notificações que avisam com antecedência (a de treino toca antes da
# MAGIC hora do treino, e o registro vem numa sessão seguinte). Aqui cada notificação é ligada à primeira
# MAGIC ação **do mesmo assunto** em até 3 h (refeição → `refeicao_registrada`, treino → `treino_feito`,
# MAGIC água → `agua_copo`, lembrete → qualquer ação), com um range join.

# COMMAND ----------

atribuicao = spark.sql(f"""
with notif as (
  select aluno_hash, _origem, sessao, evento_id, t, notificacao_tipo, notificacao_acao
  from ev_sessao where tipo = 'notificacao_tocada'
),
acoes as (
  select aluno_hash, sessao, t, tipo from ev_sessao where tipo in {ACOES}
),
ligado as (
  select n.aluno_hash, n._origem, n.evento_id, n.t, n.notificacao_tipo, n.notificacao_acao,
         min(a.t) filter (where a.sessao = n.sessao) as t_acao_sessao,
         min(a.t)                                    as t_acao_3h
  from notif n
  left join acoes a
    on a.aluno_hash = n.aluno_hash
   and a.t > n.t and a.t <= n.t + 3 * 3600
   and case n.notificacao_tipo
         when 'refeicao' then a.tipo = 'refeicao_registrada'
         when 'treino'   then a.tipo = 'treino_feito'
         when 'agua'     then a.tipo = 'agua_copo'
         else true end
  group by n.aluno_hash, n._origem, n.evento_id, n.t, n.notificacao_tipo, n.notificacao_acao
)
select _origem, notificacao_tipo, notificacao_acao,
       count(*) as notificacoes,
       avg(cast(t_acao_sessao is not null as int)) as taxa_acao_na_sessao,
       avg(cast(t_acao_3h is not null as int))     as taxa_acao_em_3h,
       percentile_approx((t_acao_3h - t) / 60.0, 0.5) as minutos_mediana_ate_acao
from ligado
group by _origem, notificacao_tipo, notificacao_acao
""")
salvar(atribuicao, O, "notificacao_atribuicao",
       "Cada notificação tocada x primeira ação do mesmo assunto: na sessão e em até 3 h.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `atrito_registro`

# COMMAND ----------

atrito = spark.sql(f"""
with ev as (
  select *, case when dayofweek(dia) in (1, 7) then 'fim_de_semana' else 'dia_util' end as tipo_dia
  from ev_sessao
),
por_evento as (
  select _origem, plataforma, tipo_dia,
         count_if(tipo = 'refeicao_registrada') as refeicoes,
         try_divide(count_if(tipo = 'refeicao_registrada' and refeicao_status = 'pulei'),
                    count_if(tipo = 'refeicao_registrada')) as pct_refeicao_pulei,
         try_divide(count_if(tipo = 'refeicao_registrada' and refeicao_status = 'troca'),
                    count_if(tipo = 'refeicao_registrada')) as pct_refeicao_troca,
         try_divide(count_if(tipo = 'refeicao_desfeita'), count_if(tipo = 'refeicao_registrada')) as taxa_desfazer_refeicao,
         try_divide(count_if(tipo = 'treino_desfeito'), count_if(tipo = 'treino_feito')) as taxa_desfazer_treino,
         try_divide(count_if(tipo = 'agua_copo' and agua_delta < 0), count_if(tipo = 'agua_copo')) as taxa_agua_removida,
         try_divide(count_if(tipo = 'refeicao_registrada' and hora_local >= 21),
                    count_if(tipo = 'refeicao_registrada')) as pct_refeicao_registrada_apos_21h
  from ev group by _origem, plataforma, tipo_dia
),
por_sessao as (
  select _origem, plataforma, tipo_dia,
         count(*) as sessoes,
         avg(cast(not converteu as int)) as taxa_abriu_e_saiu,
         percentile_approx(eventos, 0.5) as eventos_mediana_por_sessao,
         percentile_approx(duracao_seg, 0.5) as duracao_mediana_seg
  from {O}.jornada_sessao group by _origem, plataforma, tipo_dia
)
select * from por_sessao join por_evento using (_origem, plataforma, tipo_dia)
""")
salvar(atrito, O, "atrito_registro", "Sinais de atrito por plataforma e tipo de dia: desfazer, pular, abrir e sair, registro tardio.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `tagueamento_qualidade`
# MAGIC
# MAGIC Validação do plano de tags: cada evento tem que chegar com sessão, versão e as props que o contrato
# MAGIC exige, depois da abertura do app e sem atraso grande. `agua_copo` também é conferido pela soma:
# MAGIC `total_dia` precisa ser o total anterior mais `delta`.

# COMMAND ----------

spark.sql(f"""
create or replace temp view ev_tag as
select *,
       sessao_id is null as sem_sessao,
       app_versao is null as sem_versao,
       recebido_em is null or ocorrido_em is null as sem_horario,
       unix_timestamp(recebido_em) - t as atraso_seg,
       t_abertura is null as sessao_sem_abertura,
       t < t_abertura as antes_da_abertura,
       case tipo
         when 'refeicao_registrada' then refeicao_id is null or refeicao_status is null
         when 'refeicao_desfeita'   then refeicao_id is null
         when 'treino_feito'        then treino_grupo is null
         when 'cardio_registrado'   then cardio_minutos is null
         when 'agua_copo'           then agua_delta is null or agua_total_dia is null
         when 'peso_registrado'     then peso_kg is null
         when 'bau_aberto'          then bau_premio is null
         when 'notificacao_tocada'  then notificacao_tipo is null or notificacao_acao is null
         when 'tela_vista'          then tela is null or tela_origem is null
         when 'cadastro_etapa'      then cadastro_etapa is null
         else false end as props_incompletas,
       tipo = 'agua_copo' and agua_total_anterior is not null
         and agua_total_dia <> agua_total_anterior + agua_delta as agua_soma_inconsistente,
       max(app_versao) over (partition by plataforma, dia) - app_versao as versoes_atras
from ev_sessao
""")

tag = spark.sql(f"""
select _origem, plataforma, coalesce(cast(app_versao as string), '(sem versão)') as app_versao, tipo,
       count(*) as eventos,
       avg(cast(sem_sessao as int))           as pct_sem_sessao,
       avg(cast(sem_versao as int))           as pct_sem_versao,
       avg(cast(props_incompletas as int))    as pct_props_incompletas,
       avg(cast(antes_da_abertura as int))    as pct_antes_da_abertura,
       avg(cast(sessao_sem_abertura as int))  as pct_sessao_sem_abertura,
       percentile_approx(atraso_seg, 0.5)     as atraso_p50_seg,
       percentile_approx(atraso_seg, 0.95)    as atraso_p95_seg,
       avg(cast(atraso_seg > 3600 as int))    as pct_atraso_mais_1h
from ev_tag
group by _origem, plataforma, coalesce(cast(app_versao as string), '(sem versão)'), tipo
""")
salvar(tag, O, "tagueamento_qualidade", "Qualidade do tagueamento por plataforma, versão do app e tipo de evento.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `tagueamento_checagens`
# MAGIC
# MAGIC Uma linha por regra, para o dashboard e para a conversa com quem implementa as tags. Diferente do
# MAGIC notebook 05, aqui um alerta **não** derruba o job: é um achado para levar ao time do app.

# COMMAND ----------

regras = [
    # regra, descrição, expressão SQL (proporção), limite máximo
    ("sem_sessao", "Eventos sem sessao_id", "avg(cast(sem_sessao as int))", 0.01),
    ("sem_versao", "Eventos sem app_versao", "avg(cast(sem_versao as int))", 0.02),
    ("props_incompletas", "Eventos sem as props obrigatórias do contrato", "avg(cast(props_incompletas as int))", 0.005),
    ("antes_da_abertura", "Eventos com horário antes do app_aberto da sessão", "avg(cast(antes_da_abertura as int))", 0.005),
    ("sessao_sem_abertura", "Eventos em sessão sem app_aberto", "avg(cast(sessao_sem_abertura as int))", 0.01),
    ("atraso_mais_1h", "Eventos que chegaram ao servidor mais de 1 h depois (fila offline)", "avg(cast(atraso_seg > 3600 as int))", 0.05),
    ("agua_soma", "agua_copo com total_dia diferente do anterior + delta",
     "try_divide(count_if(agua_soma_inconsistente), count_if(tipo = 'agua_copo'))", 0.01),
    ("versao_antiga", "Eventos de versões 3+ builds atrás da mais nova em uso no mesmo dia", "avg(cast(versoes_atras >= 3 as int))", 0.10),
]
selects = ",\n".join(f"{expr} as {nome}" for nome, _, expr, _ in regras)
grupos = spark.sql(f"select _origem, plataforma, {selects} from ev_tag group by _origem, plataforma "
                   f"union all select _origem, 'todas' as plataforma, {selects} from ev_tag group by _origem")

linhas = []
for g in grupos.collect():
    for nome, descricao, _, limite in regras:
        v = g[nome]
        linhas.append((g["_origem"], g["plataforma"], nome, descricao, float(v) if v is not None else None, limite,
                       "ok" if v is None or v <= limite else "alerta"))

# Regras que comparam camadas: duplicados removidos, rejeitados e sessões do app x regra de 30 min.
bronze = spark.table(f"{B}.eventos").groupBy("_origem").count().withColumnRenamed("count", "bronze")
validos = spark.table(f"{P}.eventos").groupBy("_origem").count().withColumnRenamed("count", "validos")
rejeit = spark.table(f"{P}.eventos_rejeitados").groupBy("_origem").count().withColumnRenamed("count", "rejeitados")
sess = spark.sql("""
select _origem,
       count(distinct aluno_hash, sessao) as sessoes_app,
       count(distinct aluno_hash, sessao_30min) as sessoes_30min
from ev_sessao group by _origem""")
camadas = bronze.join(validos, "_origem", "left").join(rejeit, "_origem", "left").join(sess, "_origem", "left").fillna(0)
for c in camadas.collect():
    total = c["bronze"] or 1
    dup = (c["bronze"] - c["validos"] - c["rejeitados"]) / total
    rej = c["rejeitados"] / total
    dif = abs(c["sessoes_app"] - c["sessoes_30min"]) / max(c["sessoes_app"], 1)
    linhas += [
        (c["_origem"], "todas", "rejeitados", "Eventos rejeitados na prata (fora do contrato)", rej, 0.01,
         "ok" if rej <= 0.01 else "alerta"),
        (c["_origem"], "todas", "duplicados", "Eventos repetidos removidos (reenvio da fila offline)", dup, 0.02,
         "ok" if dup <= 0.02 else "alerta"),
        (c["_origem"], "todas", "sessao_vs_30min", "Diferença entre sessões do app e a regra de 30 min", dif, 0.05,
         "ok" if dif <= 0.05 else "alerta"),
    ]

checagens = spark.createDataFrame(
    linhas, "_origem string, plataforma string, regra string, descricao string, valor double, limite double, status string"
).withColumn("verificado_em", F.lit(agora_utc()))
salvar(checagens, O, "tagueamento_checagens", "Plano de tags x realidade: uma linha por regra, com status ok ou alerta.")
display(checagens.orderBy("_origem", "plataforma", "regra"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrato v2: funil de cadastro, taxa de toque e experimento
# MAGIC
# MAGIC Tabelas que só se enchem com os eventos do contrato v2 (`tela_vista`, `cadastro_etapa`,
# MAGIC `notificacao_agendada`, `experimento_exposto`). Sem esses eventos, saem vazias.
# MAGIC
# MAGIC ### `funil_cadastro`
# MAGIC
# MAGIC Do pedido do código à primeira refeição registrada. Quem recebe o plano pronto do professor pula
# MAGIC as etapas de metas e dieta, por isso a etapa 4 é "dieta pronta" (montada no app ou vinda do plano).
# MAGIC O pedido do código só sobe para quem confirmou o login (antes disso não existe aluno no banco), então
# MAGIC a etapa 1 mede o tempo até confirmar, e a perda real começa na etapa 2.

# COMMAND ----------

funil_cad = spark.sql(f"""
with marcos as (
  select aluno_hash, _origem,
         min(ocorrido_em) filter (where cadastro_etapa = 'codigo_pedido')                    as m1,
         min(ocorrido_em) filter (where cadastro_etapa = 'login_ok')                         as m2,
         min(ocorrido_em) filter (where cadastro_etapa = 'avatar_ok')                        as m3,
         min(ocorrido_em) filter (where cadastro_etapa in ('dieta_ok', 'plano_do_professor')) as m4,
         min(ocorrido_em) filter (where cadastro_etapa = 'lembretes_ok')                     as m5,
         min(ocorrido_em) filter (where tipo = 'refeicao_registrada')                        as m6,
         bool_or(cadastro_etapa = 'plano_do_professor')                                      as veio_do_plano
  from {P}.eventos
  group by aluno_hash, _origem
  having count_if(tipo = 'cadastro_etapa') > 0
),
etapas as (
  select aluno_hash, _origem, veio_do_plano, coalesce(m1, m2) as inicio,
         stack(6, 1, 'pediu o código', m1, 2, 'confirmou o login', m2, 3, 'escolheu o personagem', m3,
                  4, 'dieta pronta', m4, 5, 'lembretes', m5, 6, '1ª refeição registrada', m6)
           as (etapa_ordem, etapa, quando)
  from marcos
),
agregado as (
  select _origem, etapa_ordem, etapa,
         count_if(quando is not null) as alunos,
         count_if(quando is not null and veio_do_plano) as alunos_com_plano_do_professor,
         percentile_approx((unix_timestamp(quando) - unix_timestamp(inicio)) / 60.0, 0.5)
           filter (where quando is not null) as minutos_mediana_desde_inicio
  from etapas group by _origem, etapa_ordem, etapa
)
select *,
       try_divide(alunos, lag(alunos) over w) as conversao_etapa,
       (lag(alunos) over w) - alunos          as perdidos_na_etapa
from agregado
window w as (partition by _origem order by etapa_ordem)
""")
salvar(funil_cad, O, "funil_cadastro", "Funil do cadastro (contrato v2): código, login, personagem, dieta, lembretes, 1ª refeição.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `notificacao_taxa_toque`
# MAGIC
# MAGIC Toques ÷ avisos agendados, por tipo e semana. O app registra o agendamento uma vez por dia, quando é
# MAGIC aberto; em dia sem abertura os avisos repetidos continuam tocando sem registro, então a taxa é um
# MAGIC teto (o denominador real é um pouco maior).

# COMMAND ----------

toque = spark.sql(f"""
select _origem, notificacao_tipo, date_trunc('week', dia) as semana,
       sum(notificacao_quantidade) filter (where tipo = 'notificacao_agendada') as agendadas,
       count_if(tipo = 'notificacao_tocada')                                     as tocadas,
       count_if(tipo = 'notificacao_tocada' and notificacao_acao <> 'abrir')     as tocadas_no_botao,
       try_divide(count_if(tipo = 'notificacao_tocada'),
                  sum(notificacao_quantidade) filter (where tipo = 'notificacao_agendada')) as taxa_toque
from {P}.eventos
where tipo in ('notificacao_agendada', 'notificacao_tocada')
group by _origem, notificacao_tipo, date_trunc('week', dia)
""")
salvar(toque, O, "notificacao_taxa_toque", "Avisos agendados x tocados por tipo e semana (contrato v2).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `experimento_resultado`
# MAGIC
# MAGIC Experimento `botao_fechar_dia`: o grupo B ganha o botão "Fechar o dia" no aviso das 21 h. Cada aluno
# MAGIC conta uma vez (a taxa dele), e os grupos são comparados pela média dessas taxas com o teste t de
# MAGIC Welch, para não tratar dias do mesmo aluno como independentes.
# MAGIC
# MAGIC * **Métrica principal:** % de dias completos (baú aberto) desde a primeira exposição.
# MAGIC * **Secundárias:** chegou a 7 dias seguidos depois da exposição; ativo nos últimos 7 dias.
# MAGIC * **Guarda-corpo:** toques no botão por exposição (o grupo A tem 0 por definição).
# MAGIC
# MAGIC Como a taxa de dias completos varia muito de um aluno para outro, a diferença entre grupos tem muito
# MAGIC ruído. O CUPED reduz esse ruído: usa a taxa do mesmo aluno nos 28 dias antes da exposição (que o
# MAGIC teste não pode ter afetado) e tira da métrica a parte que ela já explicava,
# MAGIC `y_ajustado = y − θ·(x − média de x)`, com `θ = cov(y, x) / var(x)`. Quem entrou depois do início
# MAGIC do teste não tem período anterior e fica sem ajuste. A diferença esperada não muda; o intervalo encolhe.
# MAGIC
# MAGIC `efeito_minimo_detectavel` diz o menor efeito que o teste, do tamanho que está, acharia 80% das vezes.
# MAGIC Se ele é maior que o efeito plausível do botão, a resposta certa é "inconclusivo, rodar por mais tempo",
# MAGIC e não "o botão não funciona".

# COMMAND ----------

import math

por_aluno = spark.sql(f"""
with exp as (
  select aluno_hash, _origem, experimento, min(experimento_grupo) as grupo, min(dia) as desde
  from {P}.eventos where tipo = 'experimento_exposto'
  group by aluno_hash, _origem, experimento
  having count(distinct experimento_grupo) = 1
),
dias as (
  select x.aluno_hash, x._origem, x.experimento, x.grupo, x.desde,
         datediff(date'{data_ref}', x.desde) + 1 as dias_observados,
         count(distinct e.dia) filter (where e.tipo = 'bau_aberto' and e.dia >= x.desde) as dias_completos,
         count_if(e.tipo = 'notificacao_tocada' and e.notificacao_acao = 'fechar_dia' and e.dia >= x.desde) as toques_botao,
         max(e.dia) as ultimo_dia
  from exp x left join {P}.eventos e using (aluno_hash, _origem)
  group by x.aluno_hash, x._origem, x.experimento, x.grupo, x.desde
),
ilhas as (
  select x.aluno_hash, x.experimento, e.dia,
         date_sub(e.dia, cast(row_number() over (partition by x.aluno_hash, x.experimento order by e.dia) as int)) as ilha
  from exp x join (select distinct aluno_hash, dia from {P}.eventos where tipo = 'bau_aberto') e
    on e.aluno_hash = x.aluno_hash and e.dia >= x.desde
),
antes as (  -- covariável do CUPED: taxa de dias completos nos 28 dias antes da exposição
  select x.aluno_hash, x.experimento,
         try_divide(count(distinct e.dia) filter (where e.tipo = 'bau_aberto'),
                    least(28, datediff(x.desde, min(e.dia)))) as taxa_antes
  from exp x join {P}.eventos e
    on e.aluno_hash = x.aluno_hash and e._origem = x._origem
   and e.dia >= date_sub(x.desde, 28) and e.dia < x.desde
  group by x.aluno_hash, x.experimento, x.desde
),
seq as (
  select aluno_hash, experimento, max(n) as maior_sequencia
  from (select aluno_hash, experimento, ilha, count(*) as n from ilhas group by aluno_hash, experimento, ilha)
  group by aluno_hash, experimento
)
select d.*, coalesce(s.maior_sequencia, 0) as maior_sequencia,
       try_divide(d.dias_completos, d.dias_observados) as taxa_dias_completos,
       coalesce(s.maior_sequencia, 0) >= 7 as chegou_a_7_dias,
       d.ultimo_dia >= date_sub(date'{data_ref}', 6) as ativo_ultimos_7d
       , a.taxa_antes
from dias d left join seq s using (aluno_hash, experimento)
            left join antes a using (aluno_hash, experimento)
where d.dias_observados >= 7
""")
por_aluno.createOrReplaceTempView("exp_aluno_bruto")

# CUPED com funções de janela: θ e a média da covariável por origem e experimento (os dois grupos juntos).
spark.sql("""
select *, taxa_dias_completos
          - coalesce(theta * (taxa_antes - media_antes), 0) as taxa_ajustada
from (select *, covar_samp(taxa_dias_completos, taxa_antes) over w / var_samp(taxa_antes) over w as theta,
             avg(taxa_antes) over w as media_antes
      from exp_aluno_bruto
      window w as (partition by _origem, experimento))
""").createOrReplaceTempView("exp_aluno")

grupos = spark.sql("""
select _origem, experimento, grupo, count(*) as alunos,
       avg(taxa_dias_completos) as media_dias_completos, stddev_samp(taxa_dias_completos) as dp_dias_completos,
       avg(taxa_ajustada) as media_ajustada, stddev_samp(taxa_ajustada) as dp_ajustada,
       avg(cast(chegou_a_7_dias as int)) as taxa_7_dias, avg(cast(ativo_ultimos_7d as int)) as taxa_ativo_7d,
       try_divide(sum(toques_botao), sum(dias_observados)) as toques_botao_por_dia
from exp_aluno group by _origem, experimento, grupo
""").collect()

def welch(a, b, media="media_dias_completos", dp="dp_dias_completos"):
    """Diferença B − A, estatística t de Welch e valor-p bicaudal (aproximação normal, n grande)."""
    if not a or not b or a["alunos"] < 2 or b["alunos"] < 2 or a[media] is None or b[media] is None:
        return None, None, None
    va, vb = (a[dp] or 0) ** 2 / a["alunos"], (b[dp] or 0) ** 2 / b["alunos"]
    dif = b[media] - a[media]
    if va + vb == 0:
        return dif, None, None
    t = dif / math.sqrt(va + vb)
    return dif, t, math.erfc(abs(t) / math.sqrt(2))

leitura = lambda p: None if p is None else ("significativo a 5%" if p < 0.05 else "inconclusivo a 5%")
chave = {(g["_origem"], g["experimento"], g["grupo"]): g for g in grupos}
linhas = []
for (origem, exp_, grupo), g in chave.items():
    a, b = chave.get((origem, exp_, "A")), chave.get((origem, exp_, "B"))
    dif, t, p = welch(a, b) if grupo == "B" else (None, None, None)
    dif_aj, t_aj, p_aj = welch(a, b, "media_ajustada", "dp_ajustada") if grupo == "B" else (None, None, None)
    ep = abs(dif_aj / t_aj) if t_aj else None
    ic = 1.96 * ep if ep else None   # meia largura do IC 95%
    mde = 2.8 * ep if ep else None   # efeito mínimo detectável com 80% de poder a 5% (1,96 + 0,84 erros-padrão)
    linhas.append((origem, exp_, grupo, g["alunos"], g["media_dias_completos"], g["taxa_7_dias"], g["taxa_ativo_7d"],
                   g["toques_botao_por_dia"], dif, t, p, leitura(p), dif_aj, ic, p_aj, leitura(p_aj), mde))
resultado = spark.createDataFrame(linhas, """_origem string, experimento string, grupo string, alunos long,
    media_dias_completos double, taxa_7_dias double, taxa_ativo_7d double, toques_botao_por_dia double,
    diferenca_b_menos_a double, estatistica_t double, valor_p double, leitura string,
    diferenca_cuped double, ic95_cuped double, valor_p_cuped double, leitura_cuped string,
    efeito_minimo_detectavel double""")
salvar(resultado, O, "experimento_resultado", "Resultado dos experimentos A/B (contrato v2), alunos com 7+ dias de exposição.")
display(resultado)
