# Lakehouse do Foco Fit no Databricks

Arquitetura medalhão (bronze → prata → ouro) para os eventos de uso do app, pensada para rodar no
**Databricks Free Edition**. Lê os dados do gerador sintético (`analytics/synthetic/`) e,
opcionalmente, os dados reais do Supabase. Segue o contrato de eventos v1 e o catálogo de KPIs
(`analytics/docs/kpis.md`).

```
analytics/databricks/
├── databricks.yml            # Asset Bundle: job agendado com as 5 tarefas
├── notebooks/
│   ├── 00_config.py          # parâmetros, nomes, hash, funções comuns (%run nos outros)
│   ├── 01_bronze.py          # ingestão fiel (texto) + _ingerido_em, _origem, _arquivo
│   ├── 02_prata.py           # limpa, tipa, deduplica, pseudonimiza; separa rejeitados
│   ├── 03_ouro.py            # modelo estrela: dimensões e fatos diários
│   ├── 04_kpis.py            # KPIs por aluno/semana, por personal, base e retenção por coorte
│   └── 05_qualidade.py       # checagens (falham o job) + histórico de qualidade
└── tests/                    # roda o pipeline inteiro localmente com PySpark
```

## O que sai em cada camada

| camada | tabelas | regras |
| --- | --- | --- |
| **bronze** `foco_fit_bronze` | `eventos`, `profiles`, `vinculos` | cópia fiel, tudo como texto; `_origem` = `sintetico` ou `supabase`; recarga completa |
| **prata** `foco_fit_prata` | `eventos`, `eventos_rejeitados`, `alunos`, `profissionais`, `vinculos` | tipos do contrato, props em colunas, dedupe por id e por conteúdo, `aluno_hash` = SHA-256(sal + id), nascimento vira ano e faixa etária, texto das promessas descartado |
| **ouro** `foco_fit_ouro` | `dim_data`, `dim_aluno`, `dim_profissional`, `dim_tipo_treino`, `fato_refeicao`, `fato_treino_dia`, `fato_cardio_dia`, `fato_agua_dia`, `fato_peso_dia`, `fato_aluno_dia` | modelo estrela; desfeitos já descontados; tudo com `_origem` |
| **ouro (produto digital)** | `jornada_sessao`, `funil_ativacao`, `jornada_transicoes`, `jornada_caminhos`, `notificacao_efeito`, `notificacao_atribuicao`, `atrito_registro`, `tagueamento_qualidade`, `tagueamento_checagens` | sessões, funil de ativação, caminhos e transições entre eventos, efeito das notificações, sinais de atrito e validação do plano de tags (notebook `06_jornada_funil.py`) |
| **ouro (KPIs)** | `kpi_aluno_semana`, `kpi_profissional_semana`, `kpi_semana`, `kpi_retencao_coorte`, `qualidade_execucoes` | adesão à dieta, taxa de troca, treinos/semana, cardio, hidratação, dias registrados, sequência, variação de peso (4 sem.), retenção D7/D30 |

```
                dim_data            dim_tipo_treino
                   |                      |
 dim_aluno ── fato_refeicao · fato_treino_dia · fato_cardio_dia · fato_agua_dia · fato_peso_dia
    |                      \______ fato_aluno_dia (uma linha por aluno e dia)
 dim_profissional
```

As escolhas onde o catálogo de KPIs deixa espaço (semana parcial, hidratação, sequência,
retenção exata no dia N) estão no topo de `04_kpis.py`. `prob_desistencia` e `em_risco` ficam
vazios até o modelo da disciplina de Machine Learning gravar a previsão.

**LGPD:** o lakehouse nunca recebe nome, e-mail nem código de vínculo; aluno e profissional
aparecem só como hash com sal secreto. `05_qualidade.py` falha o job se alguma coluna de
identificação aparecer na prata ou na ouro, ou se um mesmo aluno misturar dados sintéticos e reais.

---

## Passo a passo no Databricks Free Edition

### 1. Criar a conta
Entre em <https://www.databricks.com/learn/free-edition> e crie a conta (pode usar o Google).
O workspace já vem com o catálogo `workspace` e computação serverless.

### 2. Trazer o repositório para o workspace
**Workspace → Create → Git folder**, cole `https://github.com/NandaTobias56/FOCO_FIT` e confirme.
Os notebooks aparecem em `FOCO_FIT/analytics/databricks/notebooks/`.

### 3. Criar o volume e enviar os dados sintéticos
No **SQL Editor**, rode:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.foco_fit_bronze;
CREATE VOLUME IF NOT EXISTS workspace.foco_fit_bronze.arquivos;
```

Gere os dados com o gerador (instruções em `analytics/synthetic/README.md`). Depois, em
**Catalog → workspace → foco_fit_bronze → Volumes → arquivos**, crie a pasta `sintetico` e use
**Upload to this volume** para enviar os arquivos de `eventos`, `profiles` e `vinculos`.
Vale Parquet ou CSV, como arquivo único (`eventos.parquet`), em partes (`eventos_001.parquet`)
ou em pasta (`eventos/`).

### 4. Rodar
**Opção A, tudo pelo navegador:** abra os notebooks `01` a `06` em ordem e clique em **Run all**
em cada um (escolha *Serverless* como computação). Para criar o job agendado, abra a pasta
`analytics/databricks` no Git folder, clique no `databricks.yml` e use **Deploy** no painel do
bundle; depois **Run** no job `foco_fit_lakehouse`.

**Opção B, pela CLI** (no computador):

```bash
# instalar: https://docs.databricks.com/dev-tools/cli/install.html
databricks auth login --host https://<seu-workspace>.cloud.databricks.com
cd analytics/databricks
databricks bundle validate
databricks bundle deploy               # alvo dev: o agendamento fica pausado
databricks bundle run foco_fit_lakehouse
databricks bundle deploy -t prod       # quando quiser o agendamento diário às 6h valendo
```

### 5. Conferir
No SQL Editor:

```sql
SELECT * FROM workspace.foco_fit_ouro.kpi_semana ORDER BY semana_inicio;
SELECT * FROM workspace.foco_fit_ouro.kpi_retencao_coorte ORDER BY coorte_semana;
SELECT * FROM workspace.foco_fit_ouro.qualidade_execucoes ORDER BY execucao_em DESC;
```

Essas tabelas alimentam o dashboard (Databricks AI/BI ou Power BI).

### 6. Case de produto digital (funil, jornada e tagueamento)
`06_jornada_funil.py` olha os eventos como dados de navegação: sessões, funil de ativação,
caminhos, efeito das notificações, atrito e qualidade das tags. As perguntas de negócio, em SQL
avulso para o SQL Editor (sessionização com LAG, funil estrito, retenção em PIVOT, gaps and
islands, comparação entre builds), estão em [`sql/case_funil_jornada.sql`](sql/case_funil_jornada.sql).
Diferente do `05`, um alerta de tagueamento não derruba o job: vira achado em `tagueamento_checagens`.

---

## Segredos (antes de usar dados reais)

Nada sensível fica no repositório. Tudo vem do secret scope `foco_fit`, criado pela CLI:

```bash
databricks secrets create-scope foco_fit
databricks secrets put-secret foco_fit hash_sal          # uma frase longa e aleatória; não perca
# só para o modo jdbc:
databricks secrets put-secret foco_fit supabase_jdbc_url  # jdbc:postgresql://<host-do-pooler>:5432/postgres?sslmode=require
databricks secrets put-secret foco_fit supabase_usuario
databricks secrets put-secret foco_fit supabase_senha
```

Sem `hash_sal`, o pipeline usa um sal de desenvolvimento e avisa. Serve para os dados
sintéticos, mas não para dados reais. Trocar o sal muda todos os hashes.

## Dados reais do Supabase (opcional)

Parâmetro `fonte_supabase` (no job ou no widget do notebook):

* `nenhuma` (padrão): só dados sintéticos.
* `arquivos`: exporte do **SQL Editor do Supabase** com **Download CSV** e envie para a pasta
  `supabase` do volume, com os nomes `eventos.csv`, `profiles.csv`, `vinculos.csv`:
  ```sql
  select id, aluno_id, tipo, dia, ocorrido_em, recebido_em, plataforma, app_versao, sessao_id, props from public.eventos;
  select id, papel, nascimento, criado_em from public.profiles;   -- sem nome
  select id, aluno_id, profissional_id, tipo, status, criado_em from public.vinculos;
  ```
* `jdbc`: lê direto do Postgres com os segredos acima, já selecionando só as colunas permitidas.
  O Free Edition limita a saída para a internet; se a conexão não abrir, use `arquivos`.

A tabela `public.eventos` passa a existir quando a migração de eventos do app for aplicada.

## Testes locais

```bash
cd analytics/databricks
pip install -r tests/requirements.txt     # precisa de Java 17+
pytest tests -q
```

O teste monta um conjunto pequeno com casos conhecidos (refeição desfeita, treino desfeito,
reenvio da fila offline, eventos inválidos, aluno que só abriu o app, aluno real com personal) e
confere bronze, prata, fatos, KPIs, retenção e qualidade. `tests/executar_local.py` roda os
mesmos notebooks fora do Databricks, simulando `dbutils` e gravando parquet, com o modo ANSI
ligado como no serverless.

Também rodei o pipeline inteiro com a saída do gerador sintético (150 alunos, ~170 mil eventos,
uns 50 s em local): todas as checagens de qualidade passaram, e minutos de cardio e copos de água
bateram exatamente com o gabarito. Os treinos dão 47 a mais que o gabarito porque o gabarito só
conta treino em dia previsto no plano, e o lakehouse conta todo `treino_feito` registrado.

## Ligação com a pós

* **04 Modelagem Dimensional:** o modelo estrela da ouro e seus grãos.
* **06 Integração e Fluxos de Dados:** ingestão de duas origens, medalhão, job agendado (bundle).
* **03 Gestão de Indicadores:** `kpi_*` seguindo o catálogo de KPIs.
* **12 Governança de Dados:** pseudonimização, segredos fora do código, `eventos_rejeitados` e
  `qualidade_execucoes` como evidência.
* **10 Machine Learning:** `fato_aluno_dia` e `kpi_aluno_semana` são a base de features para
  prever desistência e preencher `prob_desistencia`.
