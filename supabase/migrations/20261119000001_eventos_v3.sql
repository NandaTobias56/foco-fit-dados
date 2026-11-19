-- Contrato de eventos v3 (2026-11-19): o que a nutricionista acompanha.
-- Só amplia a lista de tipos aceitos; nada do v1 e do v2 muda. As props novas de refeicao_registrada
-- (porcao e nota) não precisam de mudança no banco: props já é jsonb.

alter table public.eventos drop constraint if exists eventos_tipo_check;
alter table public.eventos add constraint eventos_tipo_check check (tipo in (
  'app_aberto','refeicao_registrada','refeicao_desfeita','treino_feito','treino_desfeito',
  'cardio_registrado','agua_copo','peso_registrado','doce_sos_vencido','bau_aberto',
  'promessa_feita','notificacao_tocada',
  'tela_vista','cadastro_etapa','notificacao_agendada','experimento_exposto',
  'sos_doce_iniciado','sos_doce_cedeu','dieta_recebida'));
