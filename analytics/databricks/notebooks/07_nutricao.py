# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Ouro: o que a nutricionista acompanha (contrato v3)
# MAGIC
# MAGIC Leva para o lakehouse as análises do painel da nutri no app (`src/painel.js`): adesão por
# MAGIC refeição, escapes e vontades de doce, e consumido × plano. Fonte: eventos v3 da prata
# MAGIC (`dieta_recebida`, porção e nota em `refeicao_registrada`, `sos_doce_*`) e os fatos do `03`.
# MAGIC
# MAGIC | tabela | grão |
# MAGIC | --- | --- |
# MAGIC | `fato_plano_dieta` | aluno × dieta da nutri, com vigência (de, até) |
# MAGIC | `fato_plano_refeicao` | aluno × dieta × refeição do plano: fatia e kcal planejadas |
# MAGIC | `fato_refeicao_plano` | aluno × dia × refeição do plano: desfecho, porção, kcal estimada |
# MAGIC | `fato_consumo_dia` | aluno × dia com plano: consumido estimado × meta, macros estimados |
# MAGIC | `fato_sos_dia` | aluno × dia: vontades de doce, vencidas, cedidas, trocas ruins, escapes |
# MAGIC | `kpi_refeicao_semana` | aluno × semana × refeição do plano |
# MAGIC | `kpi_nutri_aluno_semana` | aluno × semana: adesão, refeição mais difícil, escapes, SOS, consumido × plano |
# MAGIC | `kpi_nutricionista_semana` | nutricionista × semana × origem (carteira) |
# MAGIC | `kpi_refeicao_base` | refeição × tipo de dia × origem nas últimas 4 semanas (qual refeição mais falha) |
# MAGIC
# MAGIC Regras (as mesmas do app e do contrato v3):
# MAGIC * Dias que contam: dias com algum registro do aluno dentro da vigência do plano.
# MAGIC * Desfecho da refeição: seguiu, troca boa, troca com moderação, troca ruim, pulou ou não marcou.
# MAGIC   Aderiu = seguiu ou trocou bem/com moderação. À risca = só seguiu.
# MAGIC * Consumido estimado do dia = Σ kcal × fatia × porção × fator da troca (1; 1,25; 1,6), só nos dias em
# MAGIC   que todas as refeições principais do plano foram marcadas. Macros acompanham a mesma proporção.
# MAGIC * Escape = troca ruim + SOS cedido. Taxa de vitória no SOS = vencidas ÷ vontades, só nos dias com
# MAGIC   SOS registrado (antes do v3 a vontade não virava evento).
# MAGIC * Refeição mais difícil da semana: a principal com menor adesão (empate: menos "à risca"); nula se
# MAGIC   todas empataram.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

P, O = SCHEMA_PRATA, SCHEMA_OURO
data_ref = DATA_REFERENCIA or str(spark.table(f"{O}.dim_data").agg(F.max("data")).first()[0])
fator = " ".join(f"when '{k}' then {v}" for k, v in FATOR_TROCA.items())
print("Data de referência:", data_ref)

# COMMAND ----------

# Planos: um por dieta (a primeira vez que chegou); vale até o dia anterior ao próximo plano do aluno.
fato_plano_dieta = spark.sql(f"""
with d as (
  select *, row_number() over (partition by aluno_hash, dieta_id order by ocorrido_em, evento_id) as n
  from {P}.eventos where tipo = 'dieta_recebida'
), p as (
  select aluno_hash, dieta_id, dia as vigente_de, ocorrido_em as recebida_em,
         date_sub(lead(dia) over (partition by aluno_hash order by ocorrido_em, evento_id), 1) as vigente_ate,
         dieta_kcal as kcal_meta, dieta_proteina_g as proteina_g, dieta_carbo_g as carbo_g,
         dieta_gordura_g as gordura_g, dieta_refeicoes, _origem
  from d where n = 1
)
select aluno_hash, dieta_id, vigente_de, vigente_ate, vigente_ate is null as vigente_hoje, recebida_em,
       kcal_meta, proteina_g, carbo_g, gordura_g,
       size(dieta_refeicoes) as refeicoes_plano,
       size(filter(dieta_refeicoes, r -> r.principal)) as refeicoes_principais,
       row_number() over (partition by aluno_hash order by recebida_em) as versao_plano, _origem
from p
where vigente_ate is null or vigente_ate >= vigente_de   -- dois planos no mesmo dia: vale o último
""")
salvar(fato_plano_dieta, O, "fato_plano_dieta", "Dietas da nutri recebidas pelo aluno, com vigência (contrato v3).")

fato_plano_refeicao = spark.sql(f"""
with d as (
  select aluno_hash, dieta_id, dieta_kcal, dieta_refeicoes,
         row_number() over (partition by aluno_hash, dieta_id order by ocorrido_em, evento_id) as n
  from {P}.eventos where tipo = 'dieta_recebida'
)
select p.aluno_hash, p.dieta_id, p.vigente_de, p.vigente_ate, r.refeicao_id, r.tipo as tipo_refeicao,
       coalesce(r.principal, true) as principal, r.fatia,
       round(p.kcal_meta * r.fatia, 1) as kcal_plano, p._origem
from {O}.fato_plano_dieta p
join d on d.aluno_hash = p.aluno_hash and d.dieta_id = p.dieta_id and d.n = 1
lateral view explode(d.dieta_refeicoes) e as r
""")
salvar(fato_plano_refeicao, O, "fato_plano_refeicao", "Refeições de cada plano da nutri: fatia e kcal planejadas.")

# COMMAND ----------

fato_refeicao_plano = spark.sql(f"""
select p.aluno_hash, a.data_key, a.dia, p.dieta_id, p.refeicao_id, p.tipo_refeicao, p.principal, p.fatia,
       p.kcal_plano, f.status, f.troca_nota, f.porcao,
       case when f.status is null then 'nao_marcou' when f.status = 'ok' then 'seguiu'
            when f.status = 'pulei' then 'pulou' when f.troca_nota = 'no' then 'troca_ruim'
            when f.troca_nota = 'mod' then 'troca_moderada' else 'troca_boa' end as desfecho,
       cast(coalesce(f.status = 'ok' or (f.status = 'troca' and coalesce(f.troca_nota, 'ok') <> 'no'), false) as int) as aderiu,
       cast(coalesce(f.status = 'ok', false) as int) as a_risca,
       case when f.status in ('ok', 'troca') then
         round(p.kcal_plano * coalesce(f.porcao, 1) *
               case when f.status = 'troca' then case coalesce(f.troca_nota, 'ok') {fator} end else 1 end, 1)
         else 0 end as kcal_estimado,
       a._origem
from {O}.fato_plano_refeicao p
join {O}.fato_aluno_dia a
  on a.aluno_hash = p.aluno_hash and a.registrou
 and a.dia between p.vigente_de and coalesce(p.vigente_ate, date'{data_ref}')
left join {O}.fato_refeicao f
  on f.aluno_hash = p.aluno_hash and f.dia = a.dia and f.refeicao_id = p.refeicao_id
""")
salvar(fato_refeicao_plano, O, "fato_refeicao_plano", "Cada refeição do plano da nutri em cada dia registrado: desfecho, porção e kcal estimada.")

fato_consumo_dia = spark.sql(f"""
with d as (
  select aluno_hash, data_key, dia, dieta_id, first(_origem) as _origem,
         sum(case when principal then 1 else 0 end) as principais,
         sum(case when principal and status is not null then 1 else 0 end) as principais_marcadas,
         sum(kcal_estimado) as kcal_soma
  from {O}.fato_refeicao_plano group by aluno_hash, data_key, dia, dieta_id
)
select d.aluno_hash, d.data_key, d.dia, d.dieta_id, p.kcal_meta, d.principais, d.principais_marcadas,
       d.principais_marcadas = d.principais as dia_completo,
       case when d.principais_marcadas = d.principais then round(d.kcal_soma, 1) end as kcal_estimado,
       case when d.principais_marcadas = d.principais then round(try_divide(d.kcal_soma, p.kcal_meta), 4) end as razao_consumo,
       case when d.principais_marcadas = d.principais then round(p.proteina_g * try_divide(d.kcal_soma, p.kcal_meta), 1) end as proteina_g_estimada,
       case when d.principais_marcadas = d.principais then round(p.carbo_g * try_divide(d.kcal_soma, p.kcal_meta), 1) end as carbo_g_estimado,
       case when d.principais_marcadas = d.principais then round(p.gordura_g * try_divide(d.kcal_soma, p.kcal_meta), 1) end as gordura_g_estimada,
       d._origem
from d join {O}.fato_plano_dieta p using (aluno_hash, dieta_id)
""")
salvar(fato_consumo_dia, O, "fato_consumo_dia", "Consumido estimado × meta da nutri por aluno e dia (só dias completos têm estimativa).")

# COMMAND ----------

fato_sos_dia = spark.sql(f"""
with s as (
  select aluno_hash, dia, first(_origem) as _origem,
         sum(cast(tipo = 'sos_doce_iniciado' as int)) as vontades,
         sum(cast(tipo = 'doce_sos_vencido' as int)) as vencidas,
         sum(cast(tipo = 'sos_doce_cedeu' as int)) as cedidas
  from {P}.eventos where tipo in ('sos_doce_iniciado', 'doce_sos_vencido', 'sos_doce_cedeu')
  group by aluno_hash, dia
), t as (
  select aluno_hash, dia, first(_origem) as _origem, count(*) as trocas_ruins
  from {O}.fato_refeicao where troca_nota = 'no' group by aluno_hash, dia
)
select aluno_hash, cast(date_format(dia, 'yyyyMMdd') as int) as data_key, dia,
       coalesce(s.vontades, 0) as vontades, coalesce(s.vencidas, 0) as vencidas, coalesce(s.cedidas, 0) as cedidas,
       greatest(coalesce(s.vontades, 0) - coalesce(s.vencidas, 0) - coalesce(s.cedidas, 0), 0) as vontades_sem_desfecho,
       coalesce(t.trocas_ruins, 0) as trocas_ruins,
       coalesce(t.trocas_ruins, 0) + coalesce(s.cedidas, 0) as escapes,
       coalesce(s._origem, t._origem) as _origem
from s full join t using (aluno_hash, dia)
""")
salvar(fato_sos_dia, O, "fato_sos_dia", "Vontades de doce (SOS), vitórias, cedidas, trocas ruins e escapes por aluno e dia.")

# COMMAND ----------

# MAGIC %md ## KPIs

# COMMAND ----------

kpi_refeicao_semana = spark.sql(f"""
select r.aluno_hash, date_trunc('week', r.dia)::date as semana_inicio, r.refeicao_id, r.tipo_refeicao, r.principal,
       a.nutri_hash, count(*) as dias,
       sum(cast(desfecho = 'seguiu' as int)) as seguiu,
       sum(cast(desfecho = 'troca_boa' as int)) as troca_boa,
       sum(cast(desfecho = 'troca_moderada' as int)) as troca_moderada,
       sum(cast(desfecho = 'troca_ruim' as int)) as troca_ruim,
       sum(cast(desfecho = 'pulou' as int)) as pulou,
       sum(cast(desfecho = 'nao_marcou' as int)) as nao_marcou,
       round(avg(aderiu), 4) as adesao,
       round(avg(a_risca), 4) as adesao_a_risca,
       round(try_divide(sum(cast(porcao > 1 as int)), sum(cast(status in ('ok', 'troca') and porcao is not null as int))), 4) as porcao_mais_pct,
       round(try_divide(sum(cast(porcao < 1 as int)), sum(cast(status in ('ok', 'troca') and porcao is not null as int))), 4) as porcao_menos_pct,
       first(r._origem) as _origem
from {O}.fato_refeicao_plano r left join {O}.dim_aluno a using (aluno_hash)
group by r.aluno_hash, date_trunc('week', r.dia)::date, r.refeicao_id, r.tipo_refeicao, r.principal, a.nutri_hash
""")
salvar(kpi_refeicao_semana, O, "kpi_refeicao_semana", "Adesão, trocas e porção por aluno, semana e refeição do plano da nutri.")

kpi_nutri_aluno_semana = spark.sql(f"""
with base as (
  select aluno_hash, date_trunc('week', dia)::date as semana_inicio, first(_origem) as _origem,
         count(distinct dia) as dias_com_plano,
         round(avg(case when principal then aderiu end), 4) as adesao_principais,
         round(avg(case when principal then a_risca end), 4) as adesao_a_risca
  from {O}.fato_refeicao_plano group by aluno_hash, date_trunc('week', dia)::date
), pior as (
  -- a refeição principal com menor adesão; empate vai para a que menos seguiu à risca e mais trocou mal.
  -- Se todas tiveram a mesma adesão na semana, não há refeição mais difícil (nulo).
  select aluno_hash, semana_inicio, refeicao_id as refeicao_mais_dificil, adesao as adesao_refeicao_mais_dificil
  from (select *, row_number() over (partition by aluno_hash, semana_inicio
                                     order by adesao, adesao_a_risca, troca_ruim desc, refeicao_id) as n,
               max(adesao) over (partition by aluno_hash, semana_inicio) as maior_adesao
        from {O}.kpi_refeicao_semana where principal)
  where n = 1 and adesao < maior_adesao
), sos as (
  -- vitórias contam só nos dias com SOS registrado (v3): antes do v3 não existia o evento da vontade
  select aluno_hash, date_trunc('week', dia)::date as semana_inicio,
         sum(vontades) as sos_vontades, sum(least(vencidas, vontades)) as sos_vencidas, sum(cedidas) as sos_cedidas,
         sum(trocas_ruins) as trocas_ruins, sum(escapes) as escapes,
         try_divide(sum(case when vontades > 0 then least(vencidas, vontades) end),
                    sum(case when vontades > 0 then vontades end)) as taxa_vitoria_sos
  from {O}.fato_sos_dia group by aluno_hash, date_trunc('week', dia)::date
), cons as (
  select aluno_hash, date_trunc('week', dia)::date as semana_inicio,
         sum(cast(dia_completo as int)) as dias_completos, max(kcal_meta) as kcal_meta,
         avg(kcal_estimado) as kcal_media_estimada, avg(razao_consumo) as razao_consumo,
         avg(proteina_g_estimada) as proteina_g_estimada, avg(carbo_g_estimado) as carbo_g_estimado,
         avg(gordura_g_estimada) as gordura_g_estimada
  from {O}.fato_consumo_dia group by aluno_hash, date_trunc('week', dia)::date
), porc as (
  select aluno_hash, date_trunc('week', dia)::date as semana_inicio,
         try_divide(sum(cast(porcao > 1 as int)), sum(cast(status in ('ok', 'troca') and porcao is not null as int))) as porcao_mais_pct
  from {O}.fato_refeicao_plano group by aluno_hash, date_trunc('week', dia)::date
)
select b.aluno_hash, b.semana_inicio, a.nutri_hash, b.dias_com_plano, b.adesao_principais, b.adesao_a_risca,
       b.adesao_principais >= 0.8 as bateu_adesao,
       p.refeicao_mais_dificil, p.adesao_refeicao_mais_dificil,
       coalesce(s.escapes, 0) as escapes, coalesce(s.trocas_ruins, 0) as trocas_ruins,
       coalesce(s.sos_vontades, 0) as sos_vontades, coalesce(s.sos_vencidas, 0) as sos_vencidas,
       coalesce(s.sos_cedidas, 0) as sos_cedidas, round(s.taxa_vitoria_sos, 4) as taxa_vitoria_sos,
       coalesce(c.dias_completos, 0) as dias_completos, c.kcal_meta,
       round(c.kcal_media_estimada, 0) as kcal_media_estimada, round(c.razao_consumo, 4) as razao_consumo,
       c.razao_consumo between {FAIXA_CONSUMO[0]} and {FAIXA_CONSUMO[1]} as consumo_na_faixa,
       round(c.proteina_g_estimada, 0) as proteina_g_estimada, round(c.carbo_g_estimado, 0) as carbo_g_estimado,
       round(c.gordura_g_estimada, 0) as gordura_g_estimada, round(o.porcao_mais_pct, 4) as porcao_mais_pct,
       b._origem
from base b
left join pior p using (aluno_hash, semana_inicio)
left join sos s using (aluno_hash, semana_inicio)
left join cons c using (aluno_hash, semana_inicio)
left join porc o using (aluno_hash, semana_inicio)
left join {O}.dim_aluno a using (aluno_hash)
""")
salvar(kpi_nutri_aluno_semana, O, "kpi_nutri_aluno_semana", "KPIs da nutri por aluno e semana: adesão, refeição mais difícil, escapes, SOS, consumido × plano.")

# COMMAND ----------

kpi_nutricionista_semana = spark.sql(f"""
with pior as (
  select nutri_hash, semana_inicio, _origem, refeicao_id as refeicao_mais_dificil
  from (select nutri_hash, semana_inicio, _origem, refeicao_id,
               row_number() over (partition by nutri_hash, semana_inicio, _origem
                                  order by sum(seguiu + troca_boa + troca_moderada) / sum(dias), sum(seguiu) / sum(dias),
                                           refeicao_id) as n
        from {O}.kpi_refeicao_semana where principal and nutri_hash is not null
        group by nutri_hash, semana_inicio, _origem, refeicao_id)
  where n = 1
)
select k.nutri_hash, k.semana_inicio, k._origem,
       count(*) as alunos_com_plano,
       round(avg(k.adesao_principais), 4) as adesao_media,
       round(avg(cast(k.bateu_adesao as int)), 4) as alunos_na_meta_pct,
       round(sum(k.escapes) / count(*), 2) as escapes_por_aluno,
       round(try_divide(sum(k.sos_vencidas), sum(k.sos_vontades)), 4) as taxa_vitoria_sos,
       round(avg(k.razao_consumo), 4) as razao_consumo_media,
       sum(cast(not k.consumo_na_faixa as int)) as alunos_fora_da_faixa,
       max(p.refeicao_mais_dificil) as refeicao_mais_dificil
from {O}.kpi_nutri_aluno_semana k
left join pior p using (nutri_hash, semana_inicio, _origem)
where k.nutri_hash is not null
group by k.nutri_hash, k.semana_inicio, k._origem
""")
salvar(kpi_nutricionista_semana, O, "kpi_nutricionista_semana", "Carteira de cada nutricionista por semana.")

kpi_refeicao_base = spark.sql(f"""
select r.refeicao_id, r.principal,
       case when dayofweek(r.dia) in (1, 7) then 'fim_de_semana' else 'dia_util' end as tipo_dia,
       r._origem, count(distinct r.aluno_hash) as alunos, count(*) as refeicoes_previstas,
       round(avg(r.aderiu), 4) as adesao, round(avg(r.a_risca), 4) as adesao_a_risca,
       round(avg(cast(r.desfecho = 'troca_ruim' as int)), 4) as troca_ruim_pct,
       round(avg(cast(r.desfecho in ('pulou', 'nao_marcou') as int)), 4) as pulou_ou_nao_marcou_pct,
       round(try_divide(sum(cast(r.porcao > 1 as int)), sum(cast(r.status in ('ok', 'troca') and r.porcao is not null as int))), 4) as porcao_mais_pct,
       round(try_divide(sum(r.kcal_estimado), sum(case when r.status is not null then r.kcal_plano end)), 4) as kcal_estimado_sobre_plano
from {O}.fato_refeicao_plano r
where r.dia > date_sub(date'{data_ref}', 28)
group by r.refeicao_id, r.principal, case when dayofweek(r.dia) in (1, 7) then 'fim_de_semana' else 'dia_util' end, r._origem
""")
salvar(kpi_refeicao_base, O, "kpi_refeicao_base", "Qual refeição mais falha: adesão, trocas ruins e porção por refeição e tipo de dia (últimas 4 semanas).")

# COMMAND ----------

# MAGIC %md ## Checagens (falham o job)

# COMMAND ----------

problemas = spark.sql(f"""
select 'soma das fatias do plano diferente de 1' as checagem, count(*) as n from (
  select aluno_hash, dieta_id, sum(fatia) s from {O}.fato_plano_refeicao group by aluno_hash, dieta_id) where abs(s - 1) > 0.02
union all
select 'planos do mesmo aluno com vigências sobrepostas', count(*) from {O}.fato_plano_dieta a
  join {O}.fato_plano_dieta b on a.aluno_hash = b.aluno_hash and a.dieta_id < b.dieta_id
   and a.vigente_de <= coalesce(b.vigente_ate, date'9999-12-31') and b.vigente_de <= coalesce(a.vigente_ate, date'9999-12-31')
union all
select 'consumo estimado em dia incompleto ou negativo', count(*) from {O}.fato_consumo_dia
  where (kcal_estimado is not null and not dia_completo) or kcal_estimado < 0
union all
select 'refeição do plano repetida no mesmo dia', count(*) from (
  select aluno_hash, dia, refeicao_id, count(*) n from {O}.fato_refeicao_plano group by 1, 2, 3) where n > 1
union all
select 'sos com vitórias a mais que vontades num dia v3', count(*) from {O}.fato_sos_dia
  where vontades > 0 and vencidas > vontades
""").filter("n > 0").collect()
for p in problemas:
    print("FALHOU:", p["checagem"], p["n"])
assert not problemas, "Checagens da nutrição falharam: " + "; ".join(p["checagem"] for p in problemas)
print("Checagens da nutrição ok")
display(spark.table(f"{O}.kpi_refeicao_base").orderBy("_origem", "tipo_dia", "adesao"))
