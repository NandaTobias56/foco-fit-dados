-- =====================================================================================
-- Foco Fit · case de analytics de produto digital (funil, jornada e tagueamento)
-- =====================================================================================
-- Consultas avulsas para o SQL Editor do Databricks (Spark SQL), sobre as tabelas do lakehouse.
-- Cada bloco responde a uma pergunta de produto. Rode depois dos notebooks 01 a 06.
--
-- Dados: eventos de navegação do app (contrato v1) gerados pelo simulador em analytics/synthetic,
-- com padrões de comportamento escondidos. Os números servem para mostrar o método; os achados
-- só valem para o produto quando rodarem sobre dados reais.
--
-- Técnicas usadas: LAG/LEAD, SUM acumulada, ROW_NUMBER, gaps and islands, FILTER em agregações,
-- MIN_BY, PERCENTILE_APPROX, PIVOT, funções de array, range join e GROUPING SETS.
-- QUALIFY existe no Databricks SQL; aqui as consultas usam subquery para rodarem também no
-- Spark local dos testes, e o equivalente com QUALIFY vem comentado.

USE CATALOG workspace;

-- -------------------------------------------------------------------------------------
-- Q1. Sessionização do zero: o sessao_id do app bate com a regra "30 min parado = nova sessão"?
--     (validação de tagueamento: se a diferença for grande, o app está gerando sessão errado)
-- -------------------------------------------------------------------------------------
WITH ordenado AS (
  SELECT aluno_hash, plataforma, sessao_id, ocorrido_em,
         unix_timestamp(ocorrido_em)
           - unix_timestamp(LAG(ocorrido_em) OVER (PARTITION BY aluno_hash ORDER BY ocorrido_em, evento_id))
           AS seg_desde_anterior
  FROM foco_fit_prata.eventos
),
numerado AS (
  SELECT *,
         SUM(CASE WHEN seg_desde_anterior IS NULL OR seg_desde_anterior > 1800 THEN 1 ELSE 0 END)
           OVER (PARTITION BY aluno_hash ORDER BY ocorrido_em
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sessao_30min
  FROM ordenado
)
SELECT plataforma,
       COUNT(DISTINCT aluno_hash, sessao_id)    AS sessoes_do_app,
       COUNT(DISTINCT aluno_hash, sessao_30min) AS sessoes_regra_30min,
       ROUND(COUNT(DISTINCT aluno_hash, sessao_id) / COUNT(DISTINCT aluno_hash, sessao_30min) - 1, 4) AS diferenca
FROM numerado
GROUP BY plataforma;

-- -------------------------------------------------------------------------------------
-- Q2. Funil de ativação ESTRITO por coorte semanal: cada etapa só conta se aconteceu depois
--     da anterior. Onde está o maior vazamento?
-- -------------------------------------------------------------------------------------
WITH marcos AS (
  SELECT a.aluno_hash,
         date_trunc('week', coalesce(a.criado_em, a.primeiro_dia)) AS coorte,
         coalesce(a.criado_em, cast(a.primeiro_dia AS timestamp))   AS cadastro,
         MIN(e.ocorrido_em) FILTER (WHERE e.tipo = 'refeicao_registrada') AS t_refeicao,
         MIN(e.ocorrido_em) FILTER (WHERE e.tipo = 'treino_feito')        AS t_treino,
         MIN(e.ocorrido_em) FILTER (WHERE e.tipo = 'bau_aberto')          AS t_bau
  FROM foco_fit_ouro.dim_aluno a
  LEFT JOIN foco_fit_prata.eventos e USING (aluno_hash)
  GROUP BY ALL
),
estrito AS (
  SELECT coorte,
         COUNT(*)                                                     AS contas,
         COUNT_IF(t_refeicao IS NOT NULL)                             AS fez_refeicao,
         COUNT_IF(t_refeicao IS NOT NULL AND t_treino > t_refeicao)   AS depois_treinou,
         COUNT_IF(t_refeicao IS NOT NULL AND t_treino > t_refeicao AND t_bau > t_treino) AS depois_completou_dia
  FROM marcos
  GROUP BY coorte
)
SELECT coorte, contas,
       ROUND(fez_refeicao / contas, 3)                     AS conv_refeicao,
       ROUND(try_divide(depois_treinou, fez_refeicao), 3)  AS conv_treino,
       ROUND(try_divide(depois_completou_dia, depois_treinou), 3) AS conv_dia_completo
FROM estrito
ORDER BY coorte;

-- -------------------------------------------------------------------------------------
-- Q3. Retenção semanal por coorte em matriz (PIVOT): % de alunos com algum evento na semana N.
-- -------------------------------------------------------------------------------------
WITH atividade AS (
  SELECT DISTINCT a.aluno_hash,
         date_trunc('week', a.primeiro_dia)                        AS coorte,
         floor(datediff(e.dia, a.primeiro_dia) / 7)                AS semana
  FROM foco_fit_ouro.dim_aluno a
  JOIN foco_fit_prata.eventos e USING (aluno_hash)
),
tamanho AS (
  SELECT coorte, COUNT(DISTINCT aluno_hash) AS alunos FROM atividade GROUP BY coorte
)
SELECT * FROM (
  SELECT t.coorte, t.alunos, x.semana, x.aluno_hash
  FROM atividade x JOIN tamanho t USING (coorte)
  WHERE x.semana <= 8
)
PIVOT (COUNT(DISTINCT aluno_hash) FOR semana IN (0 AS s0, 1 AS s1, 2 AS s2, 3 AS s3, 4 AS s4,
                                                 5 AS s5, 6 AS s6, 7 AS s7, 8 AS s8))
ORDER BY coorte;

-- -------------------------------------------------------------------------------------
-- Q4. Os 5 caminhos mais comuns de cada plataforma e quanto cada um converte.
--     Databricks: troque a subquery por  ... QUALIFY ranking <= 5
-- -------------------------------------------------------------------------------------
SELECT * FROM (
  SELECT plataforma, caminho,
         COUNT(*)                                                         AS sessoes,
         ROUND(AVG(CAST(converteu AS INT)), 3)                            AS taxa_conversao,
         ROW_NUMBER() OVER (PARTITION BY plataforma ORDER BY COUNT(*) DESC) AS ranking
  FROM foco_fit_ouro.jornada_sessao
  GROUP BY plataforma, caminho
)
WHERE ranking <= 5
ORDER BY plataforma, ranking;

-- -------------------------------------------------------------------------------------
-- Q5. Onde o aluno abre o app e sai sem fazer nada? (atrito por entrada e hora)
-- -------------------------------------------------------------------------------------
SELECT entrada, coalesce(notificacao_tipo, '-') AS notificacao,
       CASE WHEN hora_inicio < 12 THEN 'manhã' WHEN hora_inicio < 18 THEN 'tarde' ELSE 'noite' END AS periodo,
       COUNT(*)                                  AS sessoes,
       ROUND(AVG(CAST(NOT converteu AS INT)), 3) AS taxa_abriu_e_saiu
FROM foco_fit_ouro.jornada_sessao
WHERE plataforma = 'android'
GROUP BY GROUPING SETS ((entrada, coalesce(notificacao_tipo, '-')),
                        (entrada, coalesce(notificacao_tipo, '-'),
                         CASE WHEN hora_inicio < 12 THEN 'manhã' WHEN hora_inicio < 18 THEN 'tarde' ELSE 'noite' END))
ORDER BY entrada, notificacao, periodo NULLS FIRST;

-- -------------------------------------------------------------------------------------
-- Q6. Gaps and islands: maior sequência de dias seguidos com registro de cada aluno, e
--     quem chegou a 7+ dias abandona menos? (hipótese de produto para a gamificação)
-- -------------------------------------------------------------------------------------
WITH dias AS (  -- dia com registro: navegação e eventos de sistema (contrato v2) não contam
  SELECT DISTINCT aluno_hash, dia FROM foco_fit_prata.eventos
  WHERE tipo NOT IN ('app_aberto', 'tela_vista', 'cadastro_etapa', 'notificacao_agendada', 'experimento_exposto')
),
ilhas AS (
  SELECT aluno_hash, dia,
         date_sub(dia, CAST(ROW_NUMBER() OVER (PARTITION BY aluno_hash ORDER BY dia) AS INT)) AS ilha
  FROM dias
),
sequencias AS (
  SELECT aluno_hash, ilha, COUNT(*) AS dias_seguidos FROM ilhas GROUP BY aluno_hash, ilha
),
por_aluno AS (
  SELECT s.aluno_hash, MAX(s.dias_seguidos) AS maior_sequencia,
         MAX(a.ultimo_dia) < date_sub((SELECT MAX(dia) FROM foco_fit_prata.eventos), 14) AS abandonou
  FROM sequencias s JOIN foco_fit_ouro.dim_aluno a USING (aluno_hash)
  GROUP BY s.aluno_hash
)
SELECT CASE WHEN maior_sequencia >= 7 THEN '7+ dias' ELSE 'menos de 7' END AS chegou_a,
       COUNT(*)                                  AS alunos,
       ROUND(AVG(CAST(abandonou AS INT)), 3)     AS taxa_abandono
FROM por_aluno
GROUP BY 1;

-- -------------------------------------------------------------------------------------
-- Q7. Validação de tagueamento por versão: alguma tag parou de disparar num build?
--     Eventos por sessão de cada tipo, por versão, comparados com a versão anterior (LAG).
-- -------------------------------------------------------------------------------------
WITH por_versao AS (
  SELECT plataforma, app_versao, tipo,
         COUNT(DISTINCT aluno_hash, sessao_id) AS sessoes
  FROM foco_fit_prata.eventos
  WHERE app_versao IS NOT NULL
  GROUP BY plataforma, app_versao, tipo
),
sessoes_versao AS (
  SELECT plataforma, app_versao, COUNT(DISTINCT aluno_hash, sessao_id) AS sessoes_total
  FROM foco_fit_prata.eventos WHERE app_versao IS NOT NULL GROUP BY plataforma, app_versao
),
taxa AS (
  SELECT p.plataforma, p.app_versao, p.tipo, s.sessoes_total,
         ROUND(p.sessoes / s.sessoes_total, 4) AS pct_sessoes_com_tipo
  FROM por_versao p JOIN sessoes_versao s USING (plataforma, app_versao)
)
SELECT plataforma, app_versao, tipo, sessoes_total, pct_sessoes_com_tipo,
       ROUND(pct_sessoes_com_tipo - LAG(pct_sessoes_com_tipo) OVER w, 4) AS variacao_vs_build_anterior
FROM taxa
WHERE sessoes_total >= 200
WINDOW w AS (PARTITION BY plataforma, tipo ORDER BY app_versao)
ORDER BY abs(variacao_vs_build_anterior) DESC NULLS LAST
LIMIT 20;

-- -------------------------------------------------------------------------------------
-- Q8. Plano de tags x realidade, pronto para levar ao time do app (saída do notebook 06).
-- -------------------------------------------------------------------------------------
SELECT plataforma, regra, descricao, ROUND(valor, 4) AS valor, limite, status
FROM foco_fit_ouro.tagueamento_checagens
ORDER BY status DESC, plataforma, regra;

-- -------------------------------------------------------------------------------------
-- Otimização (só no Databricks, com Delta). Com dados reais e volume grande:
--  * agrupar os eventos por quem mais filtra/junta: aluno e dia (liquid clustering ou Z-ORDER);
--  * filtrar por dia antes das janelas, para a partição de cada window ser pequena;
--  * APPROX_COUNT_DISTINCT em painéis onde 1% de erro não muda a decisão;
--  * dimensões pequenas (dim_aluno, dim_data) entram por broadcast sem precisar de hint.
-- -------------------------------------------------------------------------------------
-- ALTER TABLE foco_fit_prata.eventos CLUSTER BY (aluno_hash, dia);
-- OPTIMIZE foco_fit_prata.eventos;
