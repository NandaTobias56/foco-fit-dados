-- Foco Fit fase 2 (2026-11-08): painel do profissional, avaliação física com laudo, dieta do nutricionista e "baixar meus dados".
-- As funções do painel rodam com as permissões de quem chama (security invoker), então as regras de acesso (RLS)
-- de registros_dia continuam valendo: o profissional só enxerga aluno com vínculo ativo.
--
-- O app manda em cada dia (registros_dia.dados) um resumo calculado no celular:
--   _r = {"g": refeições principais seguidas, "m": refeições principais previstas, "td": 1 se era dia de treino}
--   peso = peso registrado naquele dia (kg)
-- As mesmas definições dos KPIs do lakehouse (analytics/databricks, notebook 04): adesão à dieta = g/m, meta = 80%.

-- ---------- dias do aluno já "achatados" (base das duas funções do painel) ----------
create or replace function public.dias_resumo(desde date)
returns table(aluno_id uuid, dia date, g int, m int, td int, treinou int, cardio_min int, agua int, peso numeric)
language sql stable set search_path = public as $$
  select r.aluno_id, r.dia,
    least(nullif(r.dados->'_r'->>'g','')::int, nullif(r.dados->'_r'->>'m','')::int),
    nullif(r.dados->'_r'->>'m','')::int,
    nullif(r.dados->'_r'->>'td','')::int,
    case when r.dados->>'treino' in ('true','1') then 1 else 0 end,
    coalesce(nullif(r.dados->>'cardio','')::numeric, 0)::int,
    coalesce(nullif(r.dados->>'agua','')::numeric, 0)::int,
    case when jsonb_typeof(r.dados->'peso') = 'number' and (r.dados->>'peso')::numeric between 30 and 300
         then (r.dados->>'peso')::numeric end
  from registros_dia r
  where r.dia >= desde and r.dia <= current_date + 1 and r.dados <> '{}'::jsonb
$$;

-- ---------- lista de alunos do profissional: semáforo de cada um ----------
-- dias_sem_registro alimenta o alerta de "aluno sumido" (4+ dias atenção, 7+ dias vermelho)
create or replace function public.painel_alunos()
returns table(aluno_id uuid, ultimo_dia date, dias_sem_registro int, dias_registrados_7d int,
  refeicoes_seguidas_7d int, refeicoes_previstas_7d int, adesao_dieta_7d numeric,
  treinos_7d int, treinos_meta_7d int, peso_atual numeric, peso_30d numeric, ultima_avaliacao date)
language sql stable set search_path = public as $$
  with meus as (
    select distinct v.aluno_id from vinculos v
    where v.profissional_id = auth.uid() and v.status = 'ativo'
  ), d as (
    select x.* from dias_resumo(current_date - 120) x join meus using (aluno_id)
  ), pesos as (
    -- último peso e o peso de ~30 dias atrás, com window function
    select aluno_id, dia, peso,
      row_number() over (partition by aluno_id order by dia desc) as rn_atual,
      row_number() over (partition by aluno_id order by (dia <= current_date - 30) desc, dia desc) as rn_30d,
      (dia <= current_date - 30) as antigo
    from d where peso is not null
  ), semana as (
    select aluno_id,
      count(*) filter (where dia > current_date - 7) as dias7,
      sum(g) filter (where dia > current_date - 7) as g7,
      sum(m) filter (where dia > current_date - 7) as m7,
      sum(treinou * coalesce(td, 1)) filter (where dia > current_date - 7) as t7,
      sum(td) filter (where dia > current_date - 7) as tm7,
      max(dia) as ultimo
    from d group by aluno_id
  )
  select m.aluno_id, s.ultimo,
    case when s.ultimo is null then null else greatest(0, current_date - s.ultimo) end,
    coalesce(s.dias7, 0)::int, coalesce(s.g7, 0)::int, coalesce(s.m7, 0)::int,
    case when coalesce(s.m7, 0) > 0 then round(s.g7::numeric / s.m7, 4) end,
    coalesce(s.t7, 0)::int, coalesce(s.tm7, 0)::int,
    (select p.peso from pesos p where p.aluno_id = m.aluno_id and p.rn_atual = 1),
    (select p.peso from pesos p where p.aluno_id = m.aluno_id and p.rn_30d = 1 and p.antigo),
    (select max(a.feita_em) from avaliacoes a where a.aluno_id = m.aluno_id)
  from meus m left join semana s using (aluno_id)
$$;

-- ---------- série semanal de um aluno (gráficos do painel) ----------
-- semanas começam na segunda-feira; var_* comparam com a semana anterior (lag)
create or replace function public.painel_semanas(aluno uuid, semanas int default 12)
returns table(semana date, dias_registrados int, refeicoes_seguidas int, refeicoes_previstas int,
  adesao_dieta numeric, treinos int, treinos_meta int, adesao_treino numeric, cardio_min int,
  agua_media numeric, peso numeric, var_adesao_dieta numeric, var_peso numeric)
language sql stable set search_path = public as $$
  with s as (
    select generate_series(
      date_trunc('week', current_date)::date - 7 * (least(greatest(semanas, 1), 52) - 1),
      date_trunc('week', current_date)::date, interval '7 days')::date as semana
  ), d as (
    select date_trunc('week', x.dia)::date as semana, x.*
    from dias_resumo((select min(semana) from s)) x where x.aluno_id = aluno
  ), agg as (
    select s.semana, count(d.dia)::int as dias,
      coalesce(sum(d.g), 0)::int as g, coalesce(sum(d.m), 0)::int as m,
      coalesce(sum(d.treinou * coalesce(d.td, 1)), 0)::int as t, coalesce(sum(d.td), 0)::int as tm,
      coalesce(sum(d.cardio_min), 0)::int as cardio,
      round(coalesce(sum(d.agua), 0)::numeric / 7, 1) as agua,
      (array_agg(d.peso order by d.dia desc) filter (where d.peso is not null))[1] as peso
    from s left join d using (semana) group by s.semana
  ), k as (
    select *, case when m > 0 then round(g::numeric / m, 4) end as ad,
      case when tm > 0 then round(least(t, tm)::numeric / tm, 4) end as at
    from agg
  )
  select semana, dias, g, m, ad, t, tm, at, cardio, agua, peso,
    ad - lag(ad) over (order by semana),
    peso - lag(peso) over (order by semana)
  from k order by semana
$$;

grant execute on function public.dias_resumo(date), public.painel_alunos(), public.painel_semanas(uuid, int) to authenticated;

-- ---------- avaliação física: laudo (foto ou PDF) no Storage ----------
-- pasta = id do aluno. Profissional com vínculo ativo envia; o aluno e os profissionais vinculados veem.
insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values ('laudos', 'laudos', false, 5242880, array['image/jpeg','image/png','image/webp','application/pdf'])
on conflict (id) do nothing;

create or replace function public.pasta_do_aluno(nome text) returns uuid
language sql immutable as $$
  select case when split_part(nome, '/', 1) ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    then split_part(nome, '/', 1)::uuid end
$$;
grant execute on function public.pasta_do_aluno(text) to authenticated;

drop policy if exists laudo_enviar on storage.objects;
create policy laudo_enviar on storage.objects for insert to authenticated
  with check (bucket_id = 'laudos' and public.vinculo_ativo(public.pasta_do_aluno(name)));
drop policy if exists laudo_ver on storage.objects;
create policy laudo_ver on storage.objects for select to authenticated
  using (bucket_id = 'laudos' and (public.pasta_do_aluno(name) = auth.uid() or public.vinculo_ativo(public.pasta_do_aluno(name))));
drop policy if exists laudo_apagar on storage.objects;
create policy laudo_apagar on storage.objects for delete to authenticated
  using (bucket_id = 'laudos' and (public.pasta_do_aluno(name) = auth.uid()
    or (owner = auth.uid() and public.vinculo_ativo(public.pasta_do_aluno(name)))));

-- a avaliação só aponta para laudo da pasta do próprio aluno; valores fora do possível são recusados
create or replace function public.protege_avaliacao() returns trigger
language plpgsql as $$
begin
  if new.laudo_path is not null and public.pasta_do_aluno(new.laudo_path) is distinct from new.aluno_id then
    raise exception 'laudo de outro aluno';
  end if;
  if new.medidas is not null and jsonb_typeof(new.medidas) <> 'object' then raise exception 'medidas inválidas'; end if;
  return new;
end $$;
drop trigger if exists protege_avaliacao on public.avaliacoes;
create trigger protege_avaliacao before insert or update on public.avaliacoes
  for each row execute function public.protege_avaliacao();
-- "not valid": vale para o que entrar daqui em diante, sem travar por algum dado antigo
alter table public.avaliacoes
  add constraint aval_peso check (peso_kg is null or peso_kg between 25 and 350) not valid,
  add constraint aval_gordura check (gordura_pct is null or gordura_pct between 2 and 75) not valid,
  add constraint aval_agua check (agua_pct is null or agua_pct between 20 and 85) not valid,
  add constraint aval_tmb check (tmb_kcal is null or tmb_kcal between 600 and 5000) not valid,
  add constraint aval_obs check (obs is null or char_length(obs) <= 500) not valid;

-- ---------- dieta do nutricionista: só uma ativa por aluno ----------
-- ao salvar uma dieta nova, as anteriores do aluno ficam como histórico (ativo = false)
create or replace function public.uma_dieta_ativa() returns trigger
language plpgsql security definer set search_path = public as $$
begin
  if new.ativo then
    update planos_dieta set ativo = false where aluno_id = new.aluno_id and id <> new.id and ativo;
  end if;
  return new;
end $$;
drop trigger if exists uma_dieta_ativa on public.planos_dieta;
create trigger uma_dieta_ativa after insert on public.planos_dieta
  for each row execute function public.uma_dieta_ativa();
alter table public.planos_dieta
  add constraint dieta_formato check (jsonb_typeof(dados) = 'object' and octet_length(dados::text) <= 20000) not valid;

-- ---------- LGPD: baixar meus dados ----------
-- tudo o que é sobre a própria pessoa, num JSON só. Dados de outras pessoas aparecem só pelo nome.
create or replace function public.exportar_meus_dados() returns jsonb
language plpgsql stable security definer set search_path = public as $$
declare me uuid := auth.uid();
begin
  if me is null then raise exception 'sem login'; end if;
  return jsonb_build_object(
    'gerado_em', now(),
    'formato', 'foco-fit-export-v1',
    'perfil', (select to_jsonb(p) from profiles p where p.id = me),
    'registro_profissional', (select to_jsonb(x) - 'revisado_por' from profissionais x where x.id = me),
    'vinculos', coalesce((select jsonb_agg(jsonb_build_object('tipo', v.tipo, 'status', v.status, 'criado_em', v.criado_em,
        'respondido_em', v.respondido_em, 'com', coalesce(o.nome, '')) order by v.criado_em)
      from vinculos v left join profiles o on o.id = case when v.aluno_id = me then v.profissional_id else v.aluno_id end
      where v.aluno_id = me or v.profissional_id = me), '[]'::jsonb),
    'restricoes', coalesce((select jsonb_agg(to_jsonb(r) - 'autor_id' order by r.atualizado_em) from restricoes r where r.aluno_id = me), '[]'::jsonb),
    'registros_dia', coalesce((select jsonb_agg(jsonb_build_object('dia', r.dia, 'dados', r.dados) order by r.dia) from registros_dia r where r.aluno_id = me), '[]'::jsonb),
    'avaliacoes', coalesce((select jsonb_agg(to_jsonb(a) - 'autor_id' order by a.feita_em) from avaliacoes a where a.aluno_id = me), '[]'::jsonb),
    'planos_treino', coalesce((select jsonb_agg(to_jsonb(t) - 'autor_id' order by t.criado_em) from planos_treino t where t.aluno_id = me), '[]'::jsonb),
    'planos_dieta', coalesce((select jsonb_agg(to_jsonb(t) - 'autor_id' order by t.criado_em) from planos_dieta t where t.aluno_id = me), '[]'::jsonb),
    'eventos_de_uso', coalesce((select jsonb_agg(to_jsonb(e) order by e.ocorrido_em) from eventos e where e.aluno_id = me), '[]'::jsonb),
    'quem_mexeu_nos_meus_dados', coalesce((select jsonb_agg(jsonb_build_object('quando', a.em, 'tabela', a.tabela, 'acao', a.acao,
        'quem', case when a.ator_id = me then 'você' else coalesce(o.nome, 'sistema') end) order by a.em)
      from auditoria a left join profiles o on o.id = a.ator_id where a.aluno_id = me), '[]'::jsonb)
  );
end $$;
revoke all on function public.exportar_meus_dados() from public;
grant execute on function public.exportar_meus_dados() to authenticated;

-- ---------- excluir conta: agora também apaga os laudos do aluno ----------
create or replace function public.excluir_minha_conta() returns void
language plpgsql security definer set search_path = public as $$
declare me uuid := auth.uid();
begin
  if me is null then raise exception 'sem login'; end if;
  begin
    delete from storage.objects where bucket_id = 'documentos' and (storage.foldername(name))[1] = me::text;
    delete from storage.objects where bucket_id = 'laudos' and split_part(name, '/', 1) = me::text;
  exception when others then null;
  end;
  delete from auth.users where id = me;
end $$;
revoke all on function public.excluir_minha_conta() from public;
grant execute on function public.excluir_minha_conta() to authenticated;
