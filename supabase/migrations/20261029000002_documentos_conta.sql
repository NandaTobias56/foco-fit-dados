-- Foco Fit fase 1, parte 2: documento do CREF/CRN no Storage e exclusão de conta (LGPD e Google Play)

-- ---------- documentos do registro profissional ----------
-- bucket privado: só o dono envia/vê o próprio arquivo; o admin vê para conferir
insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values ('documentos', 'documentos', false, 5242880, array['image/jpeg','image/png','image/webp','application/pdf'])
on conflict (id) do nothing;

create policy doc_enviar on storage.objects for insert to authenticated
  with check (bucket_id = 'documentos' and (storage.foldername(name))[1] = auth.uid()::text);
create policy doc_ver on storage.objects for select to authenticated
  using (bucket_id = 'documentos' and ((storage.foldername(name))[1] = auth.uid()::text or public.is_admin()));
create policy doc_trocar on storage.objects for update to authenticated
  using (bucket_id = 'documentos' and (storage.foldername(name))[1] = auth.uid()::text)
  with check (bucket_id = 'documentos' and (storage.foldername(name))[1] = auth.uid()::text);
create policy doc_apagar on storage.objects for delete to authenticated
  using (bucket_id = 'documentos' and (storage.foldername(name))[1] = auth.uid()::text);

-- o cadastro só aponta para um documento da própria pasta
create or replace function public.protege_profissional() returns trigger
language plpgsql as $$
declare p text;
begin
  if current_user not in ('authenticated','anon') then return new; end if;
  select papel into p from public.profiles where id = new.id;
  if (p = 'personal' and new.conselho <> 'CREF') or (p = 'nutri' and new.conselho <> 'CRN') or p not in ('personal','nutri') then
    raise exception 'conselho não combina com o papel';
  end if;
  if new.documento_path is not null and split_part(new.documento_path, '/', 1) <> new.id::text then
    raise exception 'documento de outra pessoa';
  end if;
  if tg_op = 'UPDATE' and old.status = 'bloqueado' then raise exception 'cadastro bloqueado'; end if;
  new.status := 'pendente'; new.motivo := null; new.revisado_por := null; new.revisado_em := null;
  return new;
end $$;

-- ---------- excluir a própria conta ----------
-- o que um profissional escreveu para o aluno (alergia, plano, avaliação) fica com o aluno, sem autor
alter table public.restricoes alter column autor_id drop not null;
alter table public.planos_treino alter column autor_id drop not null;
alter table public.planos_dieta alter column autor_id drop not null;
alter table public.avaliacoes alter column autor_id drop not null;
alter table public.restricoes drop constraint restricoes_autor_id_fkey,
  add foreign key (autor_id) references public.profiles(id) on delete set null;
alter table public.planos_treino drop constraint planos_treino_autor_id_fkey,
  add foreign key (autor_id) references public.profiles(id) on delete set null;
alter table public.planos_dieta drop constraint planos_dieta_autor_id_fkey,
  add foreign key (autor_id) references public.profiles(id) on delete set null;
alter table public.avaliacoes drop constraint avaliacoes_autor_id_fkey,
  add foreign key (autor_id) references public.profiles(id) on delete set null;
alter table public.profissionais drop constraint profissionais_revisado_por_fkey,
  add foreign key (revisado_por) references public.profiles(id) on delete set null;

create or replace function public.excluir_minha_conta() returns void
language plpgsql security definer set search_path = public as $$
declare me uuid := auth.uid();
begin
  if me is null then raise exception 'sem login'; end if;
  -- o app apaga os arquivos pela API do Storage antes; aqui é só a garantia (o Storage pode recusar delete direto)
  begin
    delete from storage.objects where bucket_id = 'documentos' and (storage.foldername(name))[1] = me::text;
  exception when others then null;
  end;
  delete from auth.users where id = me;
end $$;
revoke all on function public.excluir_minha_conta() from public;
grant execute on function public.excluir_minha_conta() to authenticated;
