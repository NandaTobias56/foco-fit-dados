-- Dashboard da nutricionista (contrato de eventos v3, notebook 07_nutricao.py)
-- Um bloco por visual. No Databricks: Dashboards → Create dashboard → aba Data → "Create from SQL",
-- cole um bloco por dataset (o nome sugerido está no comentário) e monte o visual indicado.
-- Os dados de hoje são simulados (_origem = 'sintetico'). Quando houver dados reais, troque por 'supabase'.

-- [1] cartoes_nutri · visual: Counter (um por coluna) · últimas 4 semanas completas
with k as (
  select * from workspace.foco_fit_ouro.kpi_nutri_aluno_semana
  where _origem = 'sintetico'
    and semana_inicio >= date_sub((select max(semana_inicio) from workspace.foco_fit_ouro.kpi_nutri_aluno_semana), 28)
    and semana_inicio < (select max(semana_inicio) from workspace.foco_fit_ouro.kpi_nutri_aluno_semana)
)
select count(distinct aluno_hash)                              as alunos_com_plano,
       round(avg(adesao_principais) * 100, 1)                  as adesao_media_pct,
       round(avg(escapes), 2)                                  as escapes_por_aluno_semana,
       round(sum(sos_vencidas) / nullif(sum(sos_vontades), 0) * 100, 1) as vitoria_no_sos_pct,
       round(avg(razao_consumo) * 100, 1)                      as consumido_sobre_plano_pct,
       round(avg(cast(consumo_na_faixa as int)) * 100, 1)      as semanas_na_faixa_pct
from k;

-- [2] adesao_por_refeicao · visual: Bar (horizontal, empilhado 100%) · eixo Y refeicao, cor desfecho
--     Responde "qual refeição mais falha". Ordene pela adesão.
select r.refeicao_id as refeicao,
       case when dayofweek(r.dia) in (1, 7) then 'fim de semana' else 'dia útil' end as tipo_dia,
       case r.desfecho when 'seguiu' then '1 seguiu' when 'troca_boa' then '2 trocou bem'
                       when 'troca_moderada' then '3 trocou com moderação' when 'troca_ruim' then '4 trocou mal'
                       when 'pulou' then '5 pulou' else '6 não marcou' end as desfecho,
       count(*) as refeicoes
from workspace.foco_fit_ouro.fato_refeicao_plano r
where r._origem = 'sintetico' and r.principal
  and r.dia > date_sub((select max(dia) from workspace.foco_fit_ouro.fato_refeicao_plano), 28)
group by all;

-- [3] refeicao_mais_dificil · visual: Table · a refeição com menor adesão, dia útil × fim de semana
select refeicao_id as refeicao, tipo_dia, alunos,
       round(adesao * 100, 1) as adesao_pct, round(adesao_a_risca * 100, 1) as a_risca_pct,
       round(troca_ruim_pct * 100, 1) as troca_ruim_pct, round(porcao_mais_pct * 100, 1) as porcao_mais_pct,
       round(kcal_estimado_sobre_plano * 100, 1) as kcal_sobre_plano_pct
from workspace.foco_fit_ouro.kpi_refeicao_base
where _origem = 'sintetico' and principal
order by tipo_dia, adesao;

-- [4] escapes_por_semana · visual: Line (ou Bar empilhado) · x semana, y quantidade, cor tipo
select semana_inicio, 'trocas ruins' as tipo, sum(trocas_ruins) as quantidade
from workspace.foco_fit_ouro.kpi_nutri_aluno_semana where _origem = 'sintetico' group by all
union all
select semana_inicio, 'SOS cedido', sum(sos_cedidas) from workspace.foco_fit_ouro.kpi_nutri_aluno_semana
where _origem = 'sintetico' group by all
union all
select semana_inicio, 'vontade vencida no SOS', sum(sos_vencidas) from workspace.foco_fit_ouro.kpi_nutri_aluno_semana
where _origem = 'sintetico' group by all
order by semana_inicio, tipo;

-- [5] consumido_x_plano · visual: Combo · barras kcal_media_estimada e kcal_meta_media, linha % na faixa
select semana_inicio,
       round(avg(kcal_media_estimada)) as kcal_media_estimada,
       round(avg(case when kcal_media_estimada is not null then kcal_meta end)) as kcal_meta_media,
       round(avg(razao_consumo) * 100, 1) as consumido_sobre_plano_pct,
       round(avg(cast(consumo_na_faixa as int)) * 100, 1) as semanas_na_faixa_pct,
       round(avg(proteina_g_estimada)) as proteina_g, round(avg(carbo_g_estimado)) as carbo_g,
       round(avg(gordura_g_estimada)) as gordura_g
from workspace.foco_fit_ouro.kpi_nutri_aluno_semana
where _origem = 'sintetico'
group by semana_inicio order by semana_inicio;

-- [6] consumo_dia_util_x_fds · visual: Bar · consumido ÷ plano nos dias completos
select case when dayofweek(dia) in (1, 7) then 'fim de semana' else 'dia útil' end as tipo_dia,
       count(*) as dias_completos, round(avg(razao_consumo) * 100, 1) as consumido_sobre_plano_pct,
       round(avg(cast(razao_consumo > 1.1 as int)) * 100, 1) as dias_acima_da_faixa_pct
from workspace.foco_fit_ouro.fato_consumo_dia
where _origem = 'sintetico' and dia_completo
group by all;

-- [7] vontade_de_doce_por_hora · visual: Heatmap · x hora, y dia da semana, cor vontades (ou cedidas)
select element_at(array('dom', 'seg', 'ter', 'qua', 'qui', 'sex', 'sáb'), dayofweek(dia)) as dia_semana,
       dayofweek(dia) as ordem_dia, hour(ocorrido_em) as hora,
       sum(cast(tipo = 'sos_doce_iniciado' as int)) as vontades,
       sum(cast(tipo = 'sos_doce_cedeu' as int)) as cedidas
from workspace.foco_fit_prata.eventos
where _origem = 'sintetico' and tipo in ('sos_doce_iniciado', 'sos_doce_cedeu')
group by all order by ordem_dia, hora;

-- [8] porcao_por_refeicao · visual: Bar empilhado 100% · menos / o do plano / mais
select refeicao_id as refeicao,
       case porcao when 0.5 then '1 menos' when 1 then '2 o do plano' else '3 mais' end as porcao,
       count(*) as refeicoes
from workspace.foco_fit_ouro.fato_refeicao_plano
where _origem = 'sintetico' and porcao is not null
group by all;

-- [9] carteira_da_nutri · visual: Table · uma linha por nutricionista na última semana completa
select n.nutri_hash, d.uf, n.semana_inicio, n.alunos_com_plano,
       round(n.adesao_media * 100, 1) as adesao_pct, round(n.alunos_na_meta_pct * 100, 1) as alunos_na_meta_pct,
       n.escapes_por_aluno, round(n.taxa_vitoria_sos * 100, 1) as vitoria_sos_pct,
       round(n.razao_consumo_media * 100, 1) as consumido_sobre_plano_pct, n.alunos_fora_da_faixa,
       n.refeicao_mais_dificil
from workspace.foco_fit_ouro.kpi_nutricionista_semana n
left join workspace.foco_fit_ouro.dim_profissional d on d.profissional_hash = n.nutri_hash
where n._origem = 'sintetico'
  and n.semana_inicio = (select date_sub(max(semana_inicio), 7) from workspace.foco_fit_ouro.kpi_nutricionista_semana)
order by n.adesao_media;

-- [10] alunos_para_olhar · visual: Table · última semana completa, quem precisa de atenção da nutri
select left(aluno_hash, 10) as aluno, nutri_hash, round(adesao_principais * 100) as adesao_pct,
       refeicao_mais_dificil, escapes, sos_vontades, sos_cedidas,
       round(razao_consumo * 100) as consumido_sobre_plano_pct,
       concat_ws(' · ',
         case when adesao_principais < 0.6 then 'adesão baixa' end,
         case when escapes >= 3 then 'muitos escapes' end,
         case when razao_consumo > 1.1 then 'acima do plano' when razao_consumo < 0.9 then 'abaixo do plano' end) as motivo
from workspace.foco_fit_ouro.kpi_nutri_aluno_semana
where _origem = 'sintetico'
  and semana_inicio = (select date_sub(max(semana_inicio), 7) from workspace.foco_fit_ouro.kpi_nutri_aluno_semana)
  and (adesao_principais < 0.6 or escapes >= 3 or not consumo_na_faixa)
order by escapes desc, adesao_principais;
