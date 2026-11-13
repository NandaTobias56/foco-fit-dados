-- Contrato de eventos v2 (2026-11-08): telas, etapas do cadastro, avisos agendados e experimentos.
-- Só amplia a lista de tipos aceitos; nada do v1 muda.

alter table public.eventos drop constraint if exists eventos_tipo_check;
alter table public.eventos add constraint eventos_tipo_check check (tipo in (
  'app_aberto','refeicao_registrada','refeicao_desfeita','treino_feito','treino_desfeito',
  'cardio_registrado','agua_copo','peso_registrado','doce_sos_vencido','bau_aberto',
  'promessa_feita','notificacao_tocada',
  'tela_vista','cadastro_etapa','notificacao_agendada','experimento_exposto'));
