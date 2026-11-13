# Foco Fit Analytics

Projeto de dados construído em cima do app Foco Fit, como trabalho da pós-graduação em **Analytics e Business Intelligence (PUC Minas, EAD 2026)**.

O app gera eventos reais de rotina (refeições, treinos, cardio, água, peso). Um pipeline leva esses dados para um lakehouse no Databricks, onde viram indicadores, painéis e um modelo de risco de desistência. O alerta de risco volta para a tela do personal no app, fechando o ciclo.

**Pergunta de negócio:** o que faz um aluno manter a rotina de dieta e treino, e como o personal identifica cedo quem vai desistir?

## Estrutura

| Pasta | O que tem |
| --- | --- |
| `docs/` | Arquitetura, contrato de eventos, catálogo de KPIs, mapa de disciplinas e backlog |
| `synthetic/` | Gerador de dados sintéticos de alunos (Python) |
| `databricks/` | Notebooks bronze, prata e ouro e o Databricks Asset Bundle |
| `../supabase/migrations/` | Tabela `eventos` e regras de acesso (RLS) no Postgres do Supabase |
| App | O código do app fica em um repositório privado |

## Documentos

- [Arquitetura](docs/arquitetura.md)
- [Contrato de eventos](docs/contrato-eventos.md): a base comum entre app, gerador e lakehouse
- [Catálogo de KPIs](docs/kpis.md)
- [Disciplinas e entregas](docs/disciplinas.md)
- [Backlog e sprints](docs/backlog.md)

## Princípios

- **Um contrato só.** App, dados sintéticos e lakehouse seguem o mesmo formato de evento. Mudou o contrato, muda nos três.
- **Sintético separado do real.** Linhas sintéticas levam `props.sintetico = true` e `_origem = 'sintetico'` no bronze.
- **LGPD desde o início.** Dados de saúde são sensíveis. No lakehouse o aluno é só um hash; nome e e-mail nunca saem do Supabase.
- **Reprodutível.** Seed fixa no gerador, notebooks versionados, job agendado pelo bundle.
