# Catálogo de KPIs

Indicadores calculados na camada ouro. Todos por aluno e por semana (segunda a domingo), agregáveis por personal e pela base inteira. "Semana ativa" = semana com pelo menos um evento além de `app_aberto`.

| KPI | Fórmula | Meta inicial | Para quê |
| --- | --- | --- | --- |
| Adesão à dieta | refeições com status `ok` ou `troca` ÷ refeições previstas no plano | ≥ 80% | Mede se o aluno segue a dieta |
| Taxa de troca | refeições `troca` ÷ refeições registradas | acompanhar | Mostra se a dieta está difícil de seguir |
| Treinos na semana | dias com `treino_feito` (líquido de `treino_desfeito`) | ≥ dias de treino do plano | Frequência na academia |
| Minutos de cardio | soma de `cardio_registrado.minutos` | ≥ meta semanal do perfil | O hábito mais fraco da base |
| Hidratação | média de copos por dia (último `agua_copo.total_dia`) | ≥ 8 | Hábito simples, bom sinal de engajamento |
| Dias registrados | dias com algum registro ÷ 7 | ≥ 5 | Engajamento geral |
| Sequência | dias seguidos com registro até o fim da semana | crescer | Base da gamificação |
| Retenção D7 / D30 | alunos com evento no dia 7 (ou 30) após o primeiro ÷ alunos que começaram | acompanhar | Saúde do produto |
| Variação de peso | último `peso_registrado` − primeiro, em kg, por 4 semanas | perda de 0,5 a 1 kg/semana | Resultado para o aluno |
| Alunos em risco | alunos com probabilidade de desistência ≥ limiar do modelo | reduzir | Alerta para o personal |

## KPIs da nutricionista (contrato v3, notebook `07_nutricao.py`)

Só para alunos com dieta da nutri (`dieta_recebida`). Por aluno e semana, agregáveis por nutricionista.

| KPI | Fórmula | Meta inicial | Para quê |
| --- | --- | --- | --- |
| Adesão por refeição | refeições `ok` ou troca boa/moderada ÷ dias ativos com aquela refeição no plano | ≥ 80% | Mostra qual refeição mais falha |
| Refeição mais difícil | a refeição principal com a menor adesão nas últimas 4 semanas | acompanhar | Onde a nutri ajusta o cardápio |
| Escapes por semana | trocas com nota `no` + SOS cedidos | ≤ 2 | Frequência de fuga do plano |
| Taxa de vitória no SOS | vontades vencidas ÷ vontades de doce | ≥ 60% | Se a estratégia do SOS funciona |
| Consumido × plano | média do consumido estimado nos dias completos ÷ meta de kcal | entre 90% e 110% | Se o aluno come o que o plano manda |
| Porção acima do plano | refeições com porção 1,5 ÷ refeições marcadas | acompanhar | Onde o aluno exagera |

## Regras

- Refeições previstas vêm do plano de dieta do aluno; sem plano, usa 5 por dia.
- Eventos com `props.sintetico = true` ficam em tabelas separadas ou filtráveis por `_origem`, nunca misturados num painel sem aviso.
- A ficha completa de cada indicador (dono, fonte, periodicidade, limites de alerta) é entrega da disciplina 03.
