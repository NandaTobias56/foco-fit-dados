-- Foco Fit: base da plataforma (fase 1)
-- Papéis: aluno, personal, nutri, admin. O aluno é dono dos dados de saúde.
-- Profissionais só veem alunos com vínculo ATIVO, aceito pelo aluno. Admin aprova CREF/CRN e não lê dados de saúde.


-- ---------- perfis ----------
create table public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  papel text not null check (papel in ('aluno','personal','nutri','admin')),
  nome text not null default '' check (char_length(nome) <= 80),
  nascimento date,
  codigo_vinculo text unique,
  aceite_termos_em timestamptz,
  criado_em timestamptz not null default now()
);

create or replace function public.novo_codigo() returns text
language sql volatile as $$
  select 'FF-' || upper(substr(replace(gen_random_uuid()::text,'-',''),1,8))
$$;

-- cria o perfil quando a conta é criada; ninguém se cadastra como admin
create or replace function public.on_auth_user_created() returns trigger
language plpgsql security definer set search_path = public as $$
declare p text := coalesce(new.raw_user_meta_data->>'papel','aluno');
begin
  if p not in ('aluno','personal','nutri') then p := 'aluno'; end if;
  insert into public.profiles(id, papel, nome, codigo_vinculo)
  values (new.id, p, coalesce(left(new.raw_user_meta_data->>'nome',80),''),
          case when p = 'aluno' then public.novo_codigo() end);
  return new;
end $$;
create trigger on_auth_user_created after insert on auth.users
  for each row execute function public.on_auth_user_created();

-- o usuário edita nome, nascimento e aceite; papel e código só mudam por funções do sistema
create or replace function public.protege_profile() returns trigger
language plpgsql as $$
begin
  -- funções do sistema rodam como dono do banco; o app roda como "authenticated"
  if current_user not in ('authenticated','anon') then return new; end if;
  if new.papel is distinct from old.papel or new.codigo_vinculo is distinct from old.codigo_vinculo or new.id is distinct from old.id then
    raise exception 'campo protegido';
  end if;
  return new;
end $$;
create trigger protege_profile before update on public.profiles
  for each row execute function public.protege_profile();

-- ---------- profissionais (CREF / CRN) ----------
create table public.profissionais (
  id uuid primary key references public.profiles(id) on delete cascade,
  conselho text not null check (conselho in ('CREF','CRN')),
  registro text not null,
  uf char(2) not null check (uf ~ '^[A-Z]{2}$'),
  documento_path text,
  status text not null default 'pendente' check (status in ('pendente','aprovado','recusado','bloqueado')),
  motivo text,
  revisado_por uuid references public.profiles(id),
  revisado_em timestamptz,
  criado_em timestamptz not null default now(),
  -- CREF: 012345-G/SP ; CRN: CRN-3 12345 (região + número)
  constraint registro_formato check (
    (conselho = 'CREF' and registro ~ '^[0-9]{6}-[GP]/[A-Z]{2}$') or
    (conselho = 'CRN' and registro ~ '^CRN-[0-9]{1,2} ?[0-9]{3,7}(/[A-Z])?$'))
);
create unique index profissionais_registro_unico on public.profissionais (conselho, registro);

-- o profissional só cadastra o próprio registro, do conselho do seu papel; editar volta para "pendente"
create or replace function public.protege_profissional() returns trigger
language plpgsql as $$
declare p text;
begin
  if current_user not in ('authenticated','anon') then return new; end if;
  select papel into p from public.profiles where id = new.id;
  if (p = 'personal' and new.conselho <> 'CREF') or (p = 'nutri' and new.conselho <> 'CRN') or p not in ('personal','nutri') then
    raise exception 'conselho não combina com o papel';
  end if;
  if tg_op = 'UPDATE' and old.status = 'bloqueado' then raise exception 'cadastro bloqueado'; end if;
  new.status := 'pendente'; new.motivo := null; new.revisado_por := null; new.revisado_em := null;
  return new;
end $$;
create trigger protege_profissional before insert or update on public.profissionais
  for each row execute function public.protege_profissional();

-- ---------- vínculos ----------
create table public.vinculos (
  id uuid primary key default gen_random_uuid(),
  aluno_id uuid not null references public.profiles(id) on delete cascade,
  profissional_id uuid not null references public.profissionais(id) on delete cascade,
  tipo text not null check (tipo in ('personal','nutri')),
  status text not null default 'pendente' check (status in ('pendente','ativo','recusado','revogado')),
  criado_em timestamptz not null default now(),
  respondido_em timestamptz
);
create unique index vinculo_aberto_unico on public.vinculos (aluno_id, profissional_id) where status in ('pendente','ativo');

-- funções de apoio às regras (security definer para não cair em recursão de RLS)
create or replace function public.is_admin() returns boolean
language sql stable security definer set search_path = public as $$
  select exists (select 1 from profiles where id = auth.uid() and papel = 'admin')
$$;
create or replace function public.vinculo_ativo(aluno uuid, t text default null) returns boolean
language sql stable security definer set search_path = public as $$
  select exists (
    select 1 from vinculos v join profissionais p on p.id = v.profissional_id
    where v.aluno_id = aluno and v.profissional_id = auth.uid() and v.status = 'ativo'
      and p.status = 'aprovado' and (t is null or v.tipo = t))
$$;
create or replace function public.tem_vinculo_com(outro uuid) returns boolean
language sql stable security definer set search_path = public as $$
  select exists (select 1 from vinculos v where
    (v.aluno_id = auth.uid() and v.profissional_id = outro) or
    (v.profissional_id = auth.uid() and v.aluno_id = outro and v.status in ('pendente','ativo')))
$$;

-- ---------- dados de saúde ----------
create table public.restricoes (
  id uuid primary key default gen_random_uuid(),
  aluno_id uuid not null references public.profiles(id) on delete cascade,
  tipo text not null check (tipo in ('alergia','intolerancia','preferencia')),
  item text not null check (char_length(item) between 1 and 80),
  gravidade text check (gravidade in ('leve','moderada','grave')),
  obs text check (char_length(obs) <= 300),
  autor_id uuid not null default auth.uid() references public.profiles(id),
  atualizado_em timestamptz not null default now()
);
create table public.planos_treino (
  id uuid primary key default gen_random_uuid(),
  aluno_id uuid not null references public.profiles(id) on delete cascade,
  autor_id uuid not null default auth.uid() references public.profiles(id),
  dados jsonb not null,
  ativo boolean not null default true,
  criado_em timestamptz not null default now()
);
create table public.planos_dieta (like public.planos_treino including all);
alter table public.planos_dieta add foreign key (aluno_id) references public.profiles(id) on delete cascade;
alter table public.planos_dieta add foreign key (autor_id) references public.profiles(id);
create table public.avaliacoes (
  id uuid primary key default gen_random_uuid(),
  aluno_id uuid not null references public.profiles(id) on delete cascade,
  autor_id uuid not null default auth.uid() references public.profiles(id),
  feita_em date not null default current_date,
  peso_kg numeric(5,1), gordura_pct numeric(4,1), massa_magra_kg numeric(5,1), massa_muscular_kg numeric(5,1),
  agua_pct numeric(4,1), gordura_visceral numeric(4,1), tmb_kcal integer, metodo text,
  medidas jsonb, laudo_path text, obs text,
  criado_em timestamptz not null default now()
);
create table public.registros_dia (
  aluno_id uuid not null references public.profiles(id) on delete cascade,
  dia date not null,
  dados jsonb not null default '{}'::jsonb,
  atualizado_em timestamptz not null default now(),
  primary key (aluno_id, dia)
);

-- ---------- registro de alterações (auditoria) ----------
create table public.auditoria (
  id bigint generated always as identity primary key,
  ator_id uuid, aluno_id uuid, tabela text not null, acao text not null, linha_id text,
  em timestamptz not null default now()
);
create or replace function public.audita() returns trigger
language plpgsql security definer set search_path = public as $$
declare r record := coalesce(new, old);
begin
  insert into auditoria(ator_id, aluno_id, tabela, acao, linha_id)
  values (auth.uid(), r.aluno_id, tg_table_name, tg_op, coalesce(to_jsonb(r)->>'id', r.aluno_id::text));
  return coalesce(new, old);
end $$;
create trigger audita after insert or update or delete on public.restricoes for each row execute function public.audita();
create trigger audita after insert or update or delete on public.planos_treino for each row execute function public.audita();
create trigger audita after insert or update or delete on public.planos_dieta for each row execute function public.audita();
create trigger audita after insert or update or delete on public.avaliacoes for each row execute function public.audita();
create trigger audita after insert or update or delete on public.vinculos for each row execute function public.audita();

-- ---------- regras de acesso (RLS) ----------
alter table public.profiles enable row level security;
alter table public.profissionais enable row level security;
alter table public.vinculos enable row level security;
alter table public.restricoes enable row level security;
alter table public.planos_treino enable row level security;
alter table public.planos_dieta enable row level security;
alter table public.avaliacoes enable row level security;
alter table public.registros_dia enable row level security;
alter table public.auditoria enable row level security;

create policy perfil_ver on public.profiles for select to authenticated
  using (id = auth.uid() or public.is_admin() or public.tem_vinculo_com(id));
create policy perfil_editar on public.profiles for update to authenticated
  using (id = auth.uid()) with check (id = auth.uid());

create policy prof_ver on public.profissionais for select to authenticated
  using (id = auth.uid() or public.is_admin() or public.tem_vinculo_com(id));
create policy prof_criar on public.profissionais for insert to authenticated with check (id = auth.uid());
create policy prof_editar on public.profissionais for update to authenticated using (id = auth.uid()) with check (id = auth.uid());

create policy vinc_ver on public.vinculos for select to authenticated
  using (aluno_id = auth.uid() or profissional_id = auth.uid());
-- vínculos só mudam pelas funções abaixo

create policy restr_ver on public.restricoes for select to authenticated
  using (aluno_id = auth.uid() or public.vinculo_ativo(aluno_id));
create policy restr_escrever on public.restricoes for all to authenticated
  using (aluno_id = auth.uid() or public.vinculo_ativo(aluno_id, 'nutri'))
  with check ((aluno_id = auth.uid() or public.vinculo_ativo(aluno_id, 'nutri')) and autor_id = auth.uid());

create policy treino_ver on public.planos_treino for select to authenticated
  using (aluno_id = auth.uid() or public.vinculo_ativo(aluno_id));
create policy treino_escrever on public.planos_treino for all to authenticated
  using (public.vinculo_ativo(aluno_id, 'personal'))
  with check (public.vinculo_ativo(aluno_id, 'personal') and autor_id = auth.uid());

create policy dieta_ver on public.planos_dieta for select to authenticated
  using (aluno_id = auth.uid() or public.vinculo_ativo(aluno_id));
create policy dieta_escrever on public.planos_dieta for all to authenticated
  using (public.vinculo_ativo(aluno_id, 'nutri'))
  with check (public.vinculo_ativo(aluno_id, 'nutri') and autor_id = auth.uid());

create policy aval_ver on public.avaliacoes for select to authenticated
  using (aluno_id = auth.uid() or public.vinculo_ativo(aluno_id));
create policy aval_criar on public.avaliacoes for insert to authenticated
  with check (public.vinculo_ativo(aluno_id) and autor_id = auth.uid());
create policy aval_editar on public.avaliacoes for update to authenticated
  using (autor_id = auth.uid() and public.vinculo_ativo(aluno_id))
  with check (autor_id = auth.uid() and public.vinculo_ativo(aluno_id));
create policy aval_apagar on public.avaliacoes for delete to authenticated
  using (autor_id = auth.uid() and public.vinculo_ativo(aluno_id));

create policy dia_ver on public.registros_dia for select to authenticated
  using (aluno_id = auth.uid() or public.vinculo_ativo(aluno_id));
create policy dia_escrever on public.registros_dia for all to authenticated
  using (aluno_id = auth.uid()) with check (aluno_id = auth.uid());

-- o aluno vê quem mexeu nos dados dele
create policy audit_ver on public.auditoria for select to authenticated using (aluno_id = auth.uid());

-- ---------- funções chamadas pelo app ----------
-- profissional aprovado pede vínculo pelo código do aluno (até 30 pedidos por dia)
create or replace function public.solicitar_vinculo(codigo text) returns text
language plpgsql security definer set search_path = public as $$
declare me profissionais; al uuid; t text;
begin
  select * into me from profissionais where id = auth.uid();
  if me.id is null or me.status <> 'aprovado' then raise exception 'cadastro profissional não aprovado'; end if;
  if (select count(*) from vinculos where profissional_id = me.id and criado_em > now() - interval '1 day') >= 30 then
    raise exception 'limite de pedidos por dia'; end if;
  select id into al from profiles where codigo_vinculo = upper(trim(codigo)) and papel = 'aluno';
  if al is null then raise exception 'código não encontrado'; end if;
  t := case me.conselho when 'CREF' then 'personal' else 'nutri' end;
  if exists (select 1 from vinculos where aluno_id = al and profissional_id = me.id and status in ('pendente','ativo')) then
    return 'já existe'; end if;
  insert into vinculos(aluno_id, profissional_id, tipo) values (al, me.id, t);
  return 'pedido enviado';
end $$;

-- o aluno aceita ou recusa
create or replace function public.responder_vinculo(vinculo uuid, aceitar boolean) returns void
language plpgsql security definer set search_path = public as $$
begin
  update vinculos set status = case when aceitar then 'ativo' else 'recusado' end, respondido_em = now()
   where id = vinculo and aluno_id = auth.uid() and status = 'pendente';
  if not found then raise exception 'pedido não encontrado'; end if;
end $$;

-- aluno ou profissional encerram o vínculo
create or replace function public.revogar_vinculo(vinculo uuid) returns void
language plpgsql security definer set search_path = public as $$
begin
  update vinculos set status = 'revogado', respondido_em = now()
   where id = vinculo and (aluno_id = auth.uid() or profissional_id = auth.uid()) and status in ('pendente','ativo');
  if not found then raise exception 'vínculo não encontrado'; end if;
end $$;

-- aluno troca o código (se vazou)
create or replace function public.trocar_codigo() returns text
language plpgsql security definer set search_path = public as $$
declare c text := novo_codigo();
begin
  update profiles set codigo_vinculo = c where id = auth.uid() and papel = 'aluno';
  if not found then raise exception 'só aluno tem código'; end if;
  return c;
end $$;

-- admin aprova, recusa ou bloqueia um profissional
create or replace function public.revisar_profissional(prof uuid, novo_status text, motivo_txt text default null) returns void
language plpgsql security definer set search_path = public as $$
begin
  if not is_admin() then raise exception 'só administrador'; end if;
  if novo_status not in ('aprovado','recusado','bloqueado') then raise exception 'status inválido'; end if;
  update profissionais set status = novo_status, motivo = motivo_txt, revisado_por = auth.uid(), revisado_em = now() where id = prof;
  if not found then raise exception 'profissional não encontrado'; end if;
end $$;

revoke all on function public.revisar_profissional(uuid,text,text) from public;
grant execute on function public.solicitar_vinculo(text), public.responder_vinculo(uuid,boolean), public.revogar_vinculo(uuid),
  public.trocar_codigo(), public.revisar_profissional(uuid,text,text) to authenticated;
grant select, insert, update, delete on all tables in schema public to authenticated;
revoke insert, update, delete on public.vinculos, public.auditoria from authenticated;
