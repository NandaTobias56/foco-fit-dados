# Dados sintéticos de alunos

Gerador Python que simula 500 alunos e 20 profissionais usando o Foco Fit de maio a outubro de 2026. Ele produz as mesmas tabelas do contrato de eventos v1 e v2 (`analytics/docs/contrato-eventos.md`; os eventos v2 começam em 2026-09-01), com padrões de comportamento escondidos de propósito, para que o lakehouse, os painéis e as disciplinas de estatística e machine learning tenham o que descobrir antes de existirem dados reais suficientes.

Tudo é reprodutível: a mesma seed gera exatamente os mesmos arquivos, linha por linha.

## Como rodar

```bash
cd analytics/synthetic
pip install -r requirements.txt
python gerar.py                 # padrão: 500 alunos, 20 profissionais, 2026-05-01 a 2026-10-31, seed 20261102
python -m pytest -q tests       # 19 testes, cerca de 20 s
```

Opções: `--alunos`, `--profissionais`, `--inicio`, `--fim`, `--seed`, `--saida` (padrão `saida/`) e `--formatos csv,parquet`. Com os padrões leva uns 45 s.

A pasta `saida/` não vai para o Git (o `eventos.csv` passa de 120 MB e o `eventos.parquet` tem uns 35 MB). Para levar ao Databricks, rode o gerador e envie os `.parquet` para um Volume, ou rode `gerar.py` direto num notebook.

## O que sai

| arquivo | linhas (padrão) | colunas |
| --- | --- | --- |
| `eventos` | ~520 mil | id, aluno_id, tipo, dia, ocorrido_em, recebido_em, plataforma, app_versao, sessao_id, props |
| `profiles` | 520 | id, papel, nascimento, criado_em |
| `vinculos` | ~490 | id, aluno_id, profissional_id, tipo, status, criado_em |
| `profissionais` | 20 | id, conselho, uf, status, criado_em |
| `gabarito/gabarito_alunos` | 500 | aluno_id, perfil, objetivo, plataforma, peso_inicial, tem_personal_ativo, tem_nutri_ativo, divisao_treino, abandonou_em |
| `gabarito/gabarito_dias` | ~34 mil | aluno_id, dia, semana_desde_cadastro, ativo, refeicoes_boas, refeicoes_principais, dia_de_treino, treinou, cardio_min, copos_agua, completo, escudo_usado, sequencia, peso_real |

Cada tabela sai em CSV e em Parquet. Detalhes de formato:

- `ocorrido_em`, `recebido_em` e `criado_em` estão em UTC. No CSV vêm como `2026-05-03T09:41:12.000000Z`. `dia` é o dia local do aparelho (fuso de Brasília, UTC−3), igual à chave de dia do app.
- `props` é um texto JSON e sempre traz `"sintetico": true`, como pede o contrato. Os ids são UUID v4 gerados pela seed.
- `vinculos` segue as colunas reais do Supabase (`tipo` personal ou nutri, `status` pendente, ativo, recusado ou revogado). O contrato cita `estado`, mas no banco a coluna se chama `status`.
- `profissionais` não está no contrato. Vai junto porque a dimensão profissional do modelo estrela precisa de conselho (CREF ou CRN) e UF. Não tem nome, número de registro nem foto.
- `recebido_em` fica segundos depois de `ocorrido_em`, exceto em 7% dos eventos do Android, que chegam horas depois (fila offline).
- `app_versao` é o número do build disponível no dia. No Android o aluno demora alguns dias para atualizar; na web cerca de 5% vem nulo.
- Uma sessão começa com `app_aberto` e termina depois de 30 minutos parado.

### O gabarito

`gabarito/` guarda o que a simulação sabe e o app nunca saberia: o perfil oculto de cada aluno, quando ele desistiu, o peso real do dia (a pesagem registrada tem ruído de balança) e se o dia foi cumprido. Ele serve para conferir se a pipeline calcula os KPIs certos e como rótulo para os modelos de ML. **Não carregue o gabarito na camada bronze** junto com os dados de uso, porque seria vazamento de informação.

## Como os alunos foram simulados

Cada aluno recebe um perfil oculto, um objetivo (emagrecer 55%, massa 25%, saúde 20%), uma plataforma (Android 68%, web 32%), uma divisão de treino, horário de acordar e de treinar. 60% têm personal e 35% têm nutricionista, e poucos profissionais concentram muitos alunos. Os cadastros crescem ao longo dos meses e param três semanas antes do fim, para todo aluno ter pelo menos três semanas de histórico.

O dia a dia segue as regras do app (`src/logic.js`): o dia está cumprido quando pelo menos 75% das refeições principais foram boas e o treino do dia foi feito; a sequência conta os dias cumpridos seguidos; um escudo por semana segura a sequência num dia ruim; o baú abre no dia cumprido.

| perfil | alunos (padrão) | abre o app | adesão à dieta nos dias ativos | abandonam até o fim |
| --- | --- | --- | --- | --- |
| disciplinado | 108 | 93% dos dias | 88% | 6% |
| fim de semana fraco | 142 | 65% (some no sábado e domingo) | 72% | 27% |
| irregular | 152 | 38% (fases boas e ruins) | 64% | 38% |
| desistente precoce | 98 | 62% enquanto fica | 65% | 91%, quase todos no 1º mês |

## Hipóteses embutidas

De H1 a H9, e H11 e H12, cada uma tem um teste em `tests/test_hipoteses.py` que garante que o padrão aparece. Nas disciplinas, o exercício é descobrir e medir isso a partir dos eventos, sem olhar o gabarito.

| | hipótese | onde usar |
| --- | --- | --- |
| H1 | A atividade cai com as semanas desde o cadastro (abandono e queda de engajamento) | Gestão de Indicadores (retenção D7/D30), Estatística |
| H2 | No fim de semana a atividade e a dieta pioram, muito mais no perfil "fim de semana fraco" | Visualização, Estatística (teste de proporções) |
| H3 | Quem está numa sequência de 7 dias ou mais tem bem menos risco de abandonar; perder uma sequência longa derruba o engajamento por uns dias | Advanced Analytics (sobrevivência), ML (previsão de abandono) |
| H4 | O cardio é bem mais fraco que o treino (cerca de 27% dos dias ativos contra 78% dos dias de treino) | Indicadores |
| H5 | Quem quer emagrecer e adere à dieta perde peso devagar, cerca de 0,3 kg por semana; quem não adere fica quase estável | Estatística (regressão) |
| H6 | Os registros de refeição se concentram nos horários das refeições, com um pico às 21 h de quem registra tudo à noite | Data Discovery, Visualização |
| H7 | Alunos com personal ativo treinam mais, e alguns personais têm alunos mais aderentes que outros | Estatística (ANOVA), Indicadores por profissional |
| H8 | Só o Android tem notificação; tocar na notificação da refeição aumenta a chance de registrar | Data Discovery |
| H9 | Na web os alunos registram menos refeições por dia | Estatística |
| H10 | Os perfis de comportamento podem ser reencontrados por agrupamento | ML (k-means sobre frequência, fim de semana, sequência), validando com `gabarito_alunos.perfil` |
| H11 | (contrato v2, a partir de 2026-09-01) No teste A/B `botao_fechar_dia`, quem está no grupo B e toca em "Fechar o dia" no aviso das 21 h completa o que faltava bem mais do que quem só abre o app. O efeito nos dias completos por aluno é pequeno (poucos dias têm o toque), bom para discutir poder do teste | Estatística (teste A/B, Welch), notebook 06 `experimento_resultado`; o grupo está em `gabarito_alunos.grupo_fechar_dia` |
| H12 | (contrato v2) Parte de quem confirma o código desiste antes de terminar o primeiro acesso, a maioria no personagem ou nas metas | Funil (notebook 06 `funil_cadastro`) |

## Limites

- É simulação. Os números servem para exercitar a pipeline e os métodos, não para tirar conclusões sobre pessoas reais.
- O fuso é fixo em UTC−3 e não há viagens nem troca de aparelho.
- A dieta é sempre um dos três modelos do app; dietas montadas por nutricionista não são simuladas.
- Professor e admin não geram eventos (o contrato só registra eventos do aluno).
