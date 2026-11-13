-- Eventos de uso do app (contrato de eventos v1, 2026-11-02).
-- Cada toque que importa para análise vira uma linha: refeição marcada, treino feito, copo d'água...
-- O aluno só grava e lê os próprios eventos; ninguém altera nem apaga pelo app.
-- Profissional e admin não leem eventos crus. Ao excluir a conta, os eventos somem junto (cascade).

create table if not exists public.eventos (
  id uuid primary key default gen_random_uuid(),
  aluno_id uuid not null default auth.uid() references public.profiles(id) on delete cascade,
  tipo text not null check (tipo in (
    'app_aberto','refeicao_registrada','refeicao_desfeita','treino_feito','treino_desfeito',
    'cardio_registrado','agua_copo','peso_registrado','doce_sos_vencido','bau_aberto',
    'promessa_feita','notificacao_tocada')),
  dia date not null,
  ocorrido_em timestamptz not null,
  recebido_em timestamptz not null default now(),
  plataforma text check (plataforma in ('android','web')),
  app_versao text check (char_length(app_versao) <= 20),
  sessao_id uuid,
  props jsonb not null default '{}'::jsonb
    check (jsonb_typeof(props) = 'object' and octet_length(props::text) <= 2000)
);
create index if not exists eventos_aluno_ocorrido on public.eventos (aluno_id, ocorrido_em);

alter table public.eventos enable row level security;

drop policy if exists eventos_ver on public.eventos;
create policy eventos_ver on public.eventos for select to authenticated
  using (aluno_id = auth.uid());

drop policy if exists eventos_gravar on public.eventos;
create policy eventos_gravar on public.eventos for insert to authenticated
  with check (aluno_id = auth.uid()
    and exists (select 1 from public.profiles p where p.id = auth.uid() and p.papel = 'aluno'));

-- sem update/delete; recebido_em é sempre a hora do servidor (o app não consegue mandar)
revoke all on public.eventos from anon, authenticated;
grant select on public.eventos to authenticated;
grant insert (id, aluno_id, tipo, dia, ocorrido_em, plataforma, app_versao, sessao_id, props)
  on public.eventos to authenticated;
