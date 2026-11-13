-- Foco Fit: reforço de segurança do banco (2026-11-11)
-- Nada aqui muda o que o app faz no dia a dia. Fecha portas que ninguém usa e põe limites.
--   1. Funções do banco: quem não fez login (anon) não chama nenhuma.
--   2. Código do aluno (FF-XXXXXXXX): profissional que erra muitos códigos seguidos é travado por 1 hora,
--      para ninguém "chutar" códigos até achar um aluno.
--   3. Profissional só altera ou apaga o que ele mesmo escreveu (alergia, treino, dieta).
--      Antes, um nutri vinculado podia apagar uma alergia grave que o próprio aluno cadastrou.
--   4. Limite de tamanho nos campos livres (JSON), para ninguém encher o banco de lixo.

-- ---------- 1. funções: só para quem está logado ----------
-- O Postgres libera toda função nova para "public" (inclusive anon). Tiramos de todas e devolvemos só ao app logado.
revoke execute on all functions in schema public from public, anon;
alter default privileges in schema public revoke execute on functions from public, anon;

-- usadas pelas regras de acesso (RLS) e pelo Storage, e as chamadas pelo app.
-- (a lista pula as funções que ainda não existem, para este arquivo funcionar mesmo se a fase 2 não foi colada)
do $$
declare f text;
begin
  foreach f in array array[
    'public.is_admin()', 'public.vinculo_ativo(uuid, text)', 'public.tem_vinculo_com(uuid)', 'public.pasta_do_aluno(text)',
    'public.solicitar_vinculo(text)', 'public.responder_vinculo(uuid, boolean)', 'public.revogar_vinculo(uuid)',
    'public.trocar_codigo()', 'public.revisar_profissional(uuid, text, text)', 'public.excluir_minha_conta()',
    'public.exportar_meus_dados()', 'public.dias_resumo(date)', 'public.painel_alunos()', 'public.painel_semanas(uuid, integer)']
  loop
    if to_regprocedure(f) is not null then execute 'grant execute on function ' || f || ' to authenticated'; end if;
  end loop;
end $$;
-- novo_codigo só é usado por dentro (cadastro e trocar_codigo); ninguém de fora precisa chamar
revoke execute on function public.novo_codigo() from authenticated;

-- ---------- 2. limite para chutar códigos de aluno ----------
create table if not exists public.tentativas_codigo (
  id bigint generated always as identity primary key,
  profissional_id uuid not null references public.profiles(id) on delete cascade,
  em timestamptz not null default now()
);
create index if not exists tentativas_codigo_prof_em on public.tentativas_codigo (profissional_id, em);
alter table public.tentativas_codigo enable row level security;
revoke all on public.tentativas_codigo from anon, authenticated;

-- Código errado devolve uma mensagem em vez de dar erro: o erro desfaria o registro da tentativa.
-- Depois de 10 códigos errados em 1 hora, o profissional espera.
create or replace function public.solicitar_vinculo(codigo text) returns text
language plpgsql security definer set search_path = public as $$
declare me profissionais; al uuid; t text;
begin
  select * into me from profissionais where id = auth.uid();
  if me.id is null or me.status <> 'aprovado' then raise exception 'cadastro profissional não aprovado'; end if;
  if (select count(*) from tentativas_codigo where profissional_id = me.id and em > now() - interval '1 hour') >= 10 then
    raise exception 'muitos códigos errados, tente de novo em 1 hora'; end if;
  if (select count(*) from vinculos where profissional_id = me.id and criado_em > now() - interval '1 day') >= 30 then
    raise exception 'limite de pedidos por dia'; end if;
  select id into al from profiles where codigo_vinculo = upper(trim(left(codigo, 20))) and papel = 'aluno';
  if al is null then
    insert into tentativas_codigo(profissional_id) values (me.id);
    delete from tentativas_codigo where em < now() - interval '1 day';
    return 'código não encontrado';
  end if;
  t := case me.conselho when 'CREF' then 'personal' else 'nutri' end;
  if exists (select 1 from vinculos where aluno_id = al and profissional_id = me.id and status in ('pendente','ativo')) then
    return 'já existe'; end if;
  insert into vinculos(aluno_id, profissional_id, tipo) values (al, me.id, t);
  return 'pedido enviado';
end $$;
revoke execute on function public.solicitar_vinculo(text) from public, anon;
grant execute on function public.solicitar_vinculo(text) to authenticated;

-- ---------- 3. profissional só mexe no que ele escreveu ----------
drop policy if exists restr_escrever on public.restricoes;
drop policy if exists restr_criar on public.restricoes;
drop policy if exists restr_editar on public.restricoes;
drop policy if exists restr_apagar on public.restricoes;
create policy restr_criar on public.restricoes for insert to authenticated
  with check ((aluno_id = auth.uid() or public.vinculo_ativo(aluno_id, 'nutri')) and autor_id = auth.uid());
create policy restr_editar on public.restricoes for update to authenticated
  using (aluno_id = auth.uid() or (autor_id = auth.uid() and public.vinculo_ativo(aluno_id, 'nutri')))
  with check ((aluno_id = auth.uid() or public.vinculo_ativo(aluno_id, 'nutri')) and autor_id = auth.uid());
create policy restr_apagar on public.restricoes for delete to authenticated
  using (aluno_id = auth.uid() or (autor_id = auth.uid() and public.vinculo_ativo(aluno_id, 'nutri')));

drop policy if exists treino_escrever on public.planos_treino;
drop policy if exists treino_criar on public.planos_treino;
drop policy if exists treino_editar on public.planos_treino;
drop policy if exists treino_apagar on public.planos_treino;
create policy treino_criar on public.planos_treino for insert to authenticated
  with check (public.vinculo_ativo(aluno_id, 'personal') and autor_id = auth.uid());
create policy treino_editar on public.planos_treino for update to authenticated
  using (autor_id = auth.uid() and public.vinculo_ativo(aluno_id, 'personal'))
  with check (autor_id = auth.uid() and public.vinculo_ativo(aluno_id, 'personal'));
create policy treino_apagar on public.planos_treino for delete to authenticated
  using (autor_id = auth.uid() and public.vinculo_ativo(aluno_id, 'personal'));

drop policy if exists dieta_escrever on public.planos_dieta;
drop policy if exists dieta_criar on public.planos_dieta;
drop policy if exists dieta_editar on public.planos_dieta;
drop policy if exists dieta_apagar on public.planos_dieta;
create policy dieta_criar on public.planos_dieta for insert to authenticated
  with check (public.vinculo_ativo(aluno_id, 'nutri') and autor_id = auth.uid());
create policy dieta_editar on public.planos_dieta for update to authenticated
  using (autor_id = auth.uid() and public.vinculo_ativo(aluno_id, 'nutri'))
  with check (autor_id = auth.uid() and public.vinculo_ativo(aluno_id, 'nutri'));
create policy dieta_apagar on public.planos_dieta for delete to authenticated
  using (autor_id = auth.uid() and public.vinculo_ativo(aluno_id, 'nutri'));

-- ---------- 4. limites de tamanho ----------
-- "not valid": vale para o que entrar daqui em diante, sem travar por algum dado antigo
alter table public.registros_dia drop constraint if exists dia_tamanho;
alter table public.registros_dia add constraint dia_tamanho
  check (jsonb_typeof(dados) = 'object' and octet_length(dados::text) <= 60000) not valid;
alter table public.planos_treino drop constraint if exists treino_formato;
alter table public.planos_treino add constraint treino_formato
  check (jsonb_typeof(dados) = 'object' and octet_length(dados::text) <= 60000) not valid;
alter table public.avaliacoes drop constraint if exists aval_medidas_tamanho;
alter table public.avaliacoes add constraint aval_medidas_tamanho
  check (medidas is null or octet_length(medidas::text) <= 2000) not valid;
alter table public.avaliacoes drop constraint if exists aval_metodo_tamanho;
alter table public.avaliacoes add constraint aval_metodo_tamanho
  check (metodo is null or char_length(metodo) <= 40) not valid;
alter table public.profissionais drop constraint if exists prof_motivo_tamanho;
alter table public.profissionais add constraint prof_motivo_tamanho
  check (motivo is null or char_length(motivo) <= 300) not valid;
alter table public.profissionais drop constraint if exists prof_doc_tamanho;
alter table public.profissionais add constraint prof_doc_tamanho
  check (documento_path is null or char_length(documento_path) <= 200) not valid;
