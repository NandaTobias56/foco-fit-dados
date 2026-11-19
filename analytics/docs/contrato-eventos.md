# Contrato de eventos do Foco Fit (v1 2026-11-02, v2 2026-11-08, v3 2026-11-19)

Base comum para todas as frentes da pós (PUC Minas, Analytics e BI). Quem mudar algo aqui avisa as outras frentes.

## Tabela `public.eventos` (Supabase / Postgres)

| coluna | tipo | regra |
| --- | --- | --- |
| id | uuid | pk, default gen_random_uuid() |
| aluno_id | uuid | not null, references profiles(id) on delete cascade, default auth.uid() |
| tipo | text | not null, um dos tipos abaixo |
| dia | date | not null, dia local do registro (a chave de dia do app, YYYY-MM-DD) |
| ocorrido_em | timestamptz | not null, hora do aparelho |
| recebido_em | timestamptz | not null, default now() |
| plataforma | text | 'android' ou 'web' |
| app_versao | text | v1: número do build (FF_BUILD). v2: número de commits da main (FF_VERSAO), igual no APK e na web. Null se faltar |
| sessao_id | uuid | gerado quando o app abre |
| props | jsonb | not null, default '{}' |

RLS: o aluno só insere e lê os próprios eventos (aluno_id = auth.uid()); ninguém atualiza nem apaga pelo app. Profissional e admin não leem eventos crus. Índice em (aluno_id, ocorrido_em).

## Tipos e props

| tipo | props |
| --- | --- |
| app_aberto | {} |
| refeicao_registrada | {refeicao_id: text, status: 'ok' \| 'troca' \| 'pulei'} |
| refeicao_desfeita | {refeicao_id} |
| treino_feito | {grupo: text (ex. 'gluteos', 'costas')} |
| treino_desfeito | {} |
| cardio_registrado | {minutos: int} |
| agua_copo | {delta: 1 \| -1, total_dia: int} |
| peso_registrado | {kg: number} |
| doce_sos_vencido | {} |
| bau_aberto | {premio: text} |
| promessa_feita | {texto: text} |
| notificacao_tocada | {tipo: 'refeicao' \| 'treino' \| 'agua' \| 'lembrete', acao: 'ok' \| 'abrir'} |

## v2 (2026-11-08): navegação, cadastro, avisos e teste A/B

O v2 só acrescenta; nada do v1 muda de significado. Motivo: medir jornada (por onde o aluno navega),
funil de cadastro, quantos avisos ele recebe e o primeiro teste A/B. Migração no Supabase:
`supabase/migrations/20261108000002_eventos_v2.sql` (troca o `check` de tipos).

| tipo | quando dispara | props |
| --- | --- | --- |
| tela_vista | quando a tela muda: aba (hoje, treino, trocas, doces, evolucao…), primeiro acesso (`cadastro_avatar`, `cadastro_metas`, `cadastro_dieta`, `cadastro_lembretes`), escolha de papel (`papel`), login (`conta`) e área do professor (`pro`) | {tela: text, origem: 'aba' \| 'notificacao' \| 'botao' \| 'abertura'} |
| cadastro_etapa | a cada etapa do cadastro e do primeiro acesso | {etapa: 'codigo_pedido' \| 'login_ok' \| 'avatar_ok' \| 'metas_ok' \| 'dieta_ok' \| 'lembretes_ok' \| 'plano_do_professor', papel: text, novo?: bool} |
| notificacao_agendada | quando o app agenda os avisos do dia (uma vez por dia por tipo; só Android) | {tipo: 'refeicao' \| 'treino' \| 'agua' \| 'lembrete', quantidade: int} |
| experimento_exposto | uma vez por dia, quando o aviso das 21 h é agendado para o aluno | {experimento: text, grupo: 'A' \| 'B'} |

`notificacao_tocada.acao` ganha `'fechar_dia'`: o botão "Fechar o dia" do aviso das 21 h, que só o grupo B
tem. Ele abre uma folha com as refeições principais que faltam (Segui, Troquei, Pulei) e o treino do dia.

Regras:

- `tela_vista` só dispara quando o nome da tela muda, então re-render da mesma tela não duplica. Uma
  sessão nova (app aberto, ou volta depois de 30 min fora) sempre registra a tela em que o aluno está.
- `cadastro_etapa` `codigo_pedido` acontece antes do login: fica na fila do aparelho sem dono e ganha o
  id do aluno quando o código é confirmado (`login_ok`, com `novo: true` se a conta é nova). Quem desiste
  antes de confirmar não sobe nada (o banco só aceita evento de aluno logado), então o funil de cadastro
  medido começa em `login_ok`; `codigo_pedido` serve para medir o tempo entre pedir e confirmar.
- Grupo do experimento: hash FNV-1a de 32 bits de `"<experimento>:<aluno_id>"`, ímpar → B, par → A. É
  fixo por aluno, calculado no aparelho, e vai no próprio evento `experimento_exposto` (o lakehouse não
  precisa recalcular e nunca vê o id cru).
- Experimento 1, `botao_fechar_dia`. Métrica principal: % de dias completos desde a primeira exposição,
  por aluno, comparada com Welch e com CUPED (taxa dos 28 dias anteriores como covariável). Secundárias:
  chegou a 7 dias seguidos, ativo nos últimos 7 dias. Guarda-corpo: toques no botão por dia e taxa de
  desligar notificações (quando houver o evento).

## v3 (2026-11-19): o que a nutricionista acompanha

O v3 também só acrescenta. Motivo: o painel da nutri já mostra adesão por refeição, escapes e
consumido × plano a partir dos dias do aluno, mas porção, nota da troca, vontades de doce e as metas
de macros não chegavam à análise de dados. Migração no Supabase:
`supabase/migrations/20261119000001_eventos_v3.sql` (troca o `check` de tipos).

`refeicao_registrada` ganha duas props (opcionais, porque eventos antigos não têm):

| prop | quando vem | valores |
| --- | --- | --- |
| porcao | status `ok` ou `troca` | `0.5` (menos), `1` (o do plano), `1.5` (mais) |
| nota | status `troca` | `'ok'` (troca boa), `'mod'` (com moderação), `'no'` (troca ruim) |

Mudar só a porção de uma refeição já marcada gera um novo `refeicao_registrada`; como antes, o último
evento do dia para aquela refeição vale. O texto do que o aluno comeu na troca não sai do aparelho.

| tipo | quando dispara | props |
| --- | --- | --- |
| sos_doce_iniciado | o aluno toca em SOS Doce (vontade de doce) | {} |
| sos_doce_cedeu | dentro do SOS, toca em "Ainda quero, me mostre opções leves" | {} |
| dieta_recebida | o aluno recebe uma dieta da nutricionista (uma vez por dieta) | {dieta_id: text, kcal: int \| null, proteina_g, carbo_g, gordura_g: int \| null, refeicoes: [{refeicao_id, tipo, principal: bool, fatia: number}]} |

`doce_sos_vencido` ganha `repetido: true` quando o aluno vence o doce de novo no mesmo dia (antes só a
primeira vitória do dia virava evento).

Regras:

- Vontade de doce = `sos_doce_iniciado`. Desfecho: venceu (`doce_sos_vencido`), cedeu
  (`sos_doce_cedeu`) ou nenhum dos dois (fechou o SOS). Escape = troca com nota `'no'` ou SOS cedido.
- `fatia` é a parte da meta de calorias que cabe à refeição, a mesma divisão do app (café 20%, lanche
  10%, almoço 30%, pré-treino 15%, jantar 25%, ceia 7%), normalizada para somar 1 no plano do aluno.
- Consumido estimado de um dia = soma, nas refeições marcadas e não puladas, de
  `kcal × fatia × porcao × fator da troca` (troca boa 1, com moderação 1,25, ruim 1,6). Só entra o dia
  em que todas as refeições principais do plano foram marcadas. Os macros seguem a mesma proporção.
  É a mesma conta do painel da nutri (`src/painel.js`).
- O plano vale do dia em que chegou até o dia anterior ao próximo `dieta_recebida` do mesmo aluno.

## Dados sintéticos e Databricks

- O gerador sintético produz exatamente estas colunas (CSV ou Parquet, um arquivo por tabela), mais `profiles` (id, papel, nascimento, criado_em) e `vinculos` (aluno_id, profissional_id, estado, criado_em) com as mesmas colunas do Supabase.
- Linhas sintéticas levam `props.sintetico = true` e ids gerados, para nunca se misturarem com dados reais sem querer.
- Bronze no Databricks = cópia fiel destas tabelas, com colunas extras `_ingerido_em` e `_origem` ('supabase' ou 'sintetico').
- Dados de saúde são sensíveis (LGPD): no lakehouse o aluno é identificado só por hash do id; nome e e-mail nunca saem do Supabase.
