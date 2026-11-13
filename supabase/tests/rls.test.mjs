// Testa as regras de acesso do banco num Postgres local (PGlite), sem servidor.
// Rodar: npm run test:db
import {PGlite} from "@electric-sql/pglite";
import {readFileSync, readdirSync} from "node:fs";
const here = new URL(".", import.meta.url), mig = new URL("../migrations/", import.meta.url);
const db = new PGlite();
await db.exec(readFileSync(new URL("auth_stub.sql", here), "utf8"));
for (const f of readdirSync(mig).sort()) await db.exec(readFileSync(new URL(f, mig), "utf8"));

let ok = 0, fail = 0;
const check = (cond, msg) => { if (cond) ok++; else { fail++; console.log("FALHOU:", msg); } };
const id = n => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
// roda SQL como um usuário logado (papel authenticated + auth.uid())
async function as(uid, sql, params = []) {
  await db.exec(`set role authenticated; select set_config('request.jwt.claim.sub', '${uid}', false);`);
  try { return await db.query(sql, params); } finally { await db.exec(`reset role; select set_config('request.jwt.claim.sub', '', false);`); }
}
async function fails(uid, sql, msg, params = []) {
  try { await as(uid, sql, params); check(false, msg + " (deveria ter sido bloqueado)"); } catch (e) { check(true, msg); }
}
const rows = async (uid, sql, params) => (await as(uid, sql, params)).rows;

const A = id(1), B = id(2), P = id(3), N = id(4), Q = id(5), X = id(6);
const signup = (u, meta) => db.query("insert into auth.users(id, raw_user_meta_data) values ($1, $2)", [u, meta]);
await signup(A, {papel: "aluno", nome: "Ana"});
await signup(B, {papel: "aluno", nome: "Bia"});
await signup(P, {papel: "personal", nome: "Paulo"});
await signup(N, {papel: "nutri", nome: "Nina"});
await signup(Q, {papel: "personal", nome: "Quim"});
await signup(X, {papel: "admin", nome: "Esperto"});
check((await db.query("select papel from profiles where id=$1", [X])).rows[0].papel === "aluno", "cadastro como admin vira aluno");
await db.query("update profiles set papel='admin' where id=$1", [X]); // admin real é definido direto no banco
const codA = (await db.query("select codigo_vinculo c from profiles where id=$1", [A])).rows[0].c;
check(/^FF-[0-9A-F]{8}$/.test(codA), "aluno recebe código FF-XXXXXXXX");
check((await db.query("select codigo_vinculo c from profiles where id=$1", [P])).rows[0].c === null, "profissional não tem código");

// cadastro profissional
await fails(P, "insert into profissionais(id, conselho, registro, uf) values ($1,'CREF','12345','SP')", "CREF com formato errado", [P]);
await fails(P, "insert into profissionais(id, conselho, registro, uf) values ($1,'CRN','CRN-3 12345','SP')", "personal não cadastra CRN", [P]);
await fails(A, "insert into profissionais(id, conselho, registro, uf) values ($1,'CREF','012345-G/SP','SP')", "aluno não vira profissional", [A]);
await fails(P, "insert into profissionais(id, conselho, registro, uf) values ($1,'CREF','012345-G/SP','SP')", "ninguém cadastra registro em nome de outro", [Q]);
await as(P, "insert into profissionais(id, conselho, registro, uf, status) values ($1,'CREF','012345-G/SP','SP','aprovado')", [P]);
check((await db.query("select status from profissionais where id=$1", [P])).rows[0].status === "pendente", "profissional não se autoaprova");
await as(N, "insert into profissionais(id, conselho, registro, uf) values ($1,'CRN','CRN-3 45678','SP')", [N]);
await as(Q, "insert into profissionais(id, conselho, registro, uf) values ($1,'CREF','098765-G/RJ','RJ')", [Q]);
await fails(Q, "insert into profissionais(id, conselho, registro, uf) values ($1,'CREF','012345-G/SP','SP')", "registro duplicado", [Q]);

// aprovação pelo admin
await fails(P, "select revisar_profissional($1,'aprovado')", "profissional não aprova a si mesmo", [P]);
await as(X, "select revisar_profissional($1,'aprovado')", [P]);
await as(X, "select revisar_profissional($1,'aprovado')", [N]);
check((await rows(X, "select count(*)::int n from profissionais")).at(0).n === 3, "admin vê os cadastros profissionais");

// vínculo
await fails(Q, "select solicitar_vinculo($1)", "profissional pendente não pede vínculo", [codA]);
check((await rows(P, "select solicitar_vinculo('FF-00000000') r"))[0].r === "código não encontrado", "código inexistente");
await fails(P, "insert into vinculos(aluno_id, profissional_id, tipo, status) values ($1,$2,'personal','ativo')", "ninguém cria vínculo ativo direto", [A, P]);
await as(P, "select solicitar_vinculo($1)", [codA.toLowerCase()]);
await as(A, "insert into registros_dia(aluno_id, dia, dados) values ($1, '2026-10-29', '{\"agua\":5}')", [A]);
await as(A, "insert into restricoes(aluno_id, tipo, item, gravidade) values ($1,'alergia','amendoim','grave')", [A]);
check((await rows(P, "select * from registros_dia")).length === 0, "pedido pendente não dá acesso aos dados");
check((await rows(P, "select nome from profiles where id=$1", [A])).length === 1, "profissional vê o nome do aluno do pedido");
check((await rows(B, "select * from vinculos")).length === 0, "outro aluno não vê o pedido");
const vP = (await rows(A, "select id from vinculos where profissional_id=$1", [P]))[0].id;
check(!!vP, "aluno vê o pedido");
await fails(P, "select responder_vinculo($1, true)", "profissional não aceita o próprio pedido", [vP]);
await as(A, "select responder_vinculo($1, true)", [vP]);

// personal com vínculo ativo
check((await rows(P, "select * from registros_dia")).length === 1, "personal vê os registros do aluno vinculado");
check((await rows(P, "select * from restricoes")).length === 1, "personal vê as alergias do aluno");
await as(P, "insert into planos_treino(aluno_id, dados) values ($1, '{\"dias\":{}}')", [A]);
await fails(P, "insert into planos_dieta(aluno_id, dados) values ($1, '{}')", "personal não escreve dieta", [A]);
await fails(P, "insert into restricoes(aluno_id, tipo, item) values ($1,'alergia','leite')", "personal não edita alergias", [A]);
await fails(P, "insert into registros_dia(aluno_id, dia) values ($1, '2026-10-30')", "personal não preenche o dia do aluno", [A]);
await fails(P, "insert into planos_treino(aluno_id, dados) values ($1, '{}')", "personal não escreve treino de aluno sem vínculo", [B]);
await as(P, "insert into avaliacoes(aluno_id, peso_kg, gordura_pct, tmb_kcal, metodo) values ($1, 72.4, 31.2, 1430, 'bioimpedância')", [A]);
check((await rows(A, "select * from avaliacoes")).length === 1, "aluno vê a avaliação feita pelo personal");
check((await rows(P, "select * from registros_dia where aluno_id=$1", [B])).length === 0, "personal não vê aluno de outro");

// nutricionista
await as(N, "select solicitar_vinculo($1)", [codA]);
const vN = (await rows(A, "select id from vinculos where profissional_id=$1", [N]))[0].id;
await as(A, "select responder_vinculo($1, true)", [vN]);
await as(N, "insert into planos_dieta(aluno_id, dados) values ($1, '{\"refeicoes\":[]}')", [A]);
await as(N, "insert into restricoes(aluno_id, tipo, item, gravidade) values ($1,'intolerancia','lactose','moderada')", [A]);
await fails(N, "insert into planos_treino(aluno_id, dados) values ($1, '{}')", "nutri não escreve treino", [A]);
check((await rows(P, "select * from planos_dieta")).length === 1, "personal lê a dieta do nutri");
await as(P, "update planos_dieta set dados='{}'");
check((await db.query("select dados->'refeicoes' r from planos_dieta")).rows[0].r !== null, "personal não altera a dieta do nutri");

// perfil protegido
await fails(A, "update profiles set papel='admin' where id=$1", "aluno não vira admin", [A]);
await fails(A, "update profiles set codigo_vinculo='FF-AAAAAAAA' where id=$1", "aluno não escolhe o próprio código", [A]);
await as(A, "update profiles set nome='Ana Paula' where id=$1", [A]);
const novo = (await rows(A, "select trocar_codigo() c"))[0].c;
check(novo !== codA, "aluno troca o código");
check((await rows(P, "select solicitar_vinculo($1) r", [codA]))[0].r === "código não encontrado", "código antigo para de funcionar");

// admin não lê saúde
check((await rows(X, "select * from registros_dia")).length === 0, "admin não lê registros do dia");
check((await rows(X, "select * from restricoes")).length === 0, "admin não lê alergias");
check((await rows(X, "select * from avaliacoes")).length === 0, "admin não lê avaliações");

// auditoria
check((await rows(A, "select * from auditoria")).length >= 4, "aluno vê quem mexeu nos dados dele");
check((await rows(B, "select * from auditoria")).length === 0, "outro aluno não vê a auditoria");
await fails(P, "delete from auditoria", "ninguém apaga a auditoria");

// eventos de uso (contrato v1)
const EV = "insert into eventos(tipo, dia, ocorrido_em, plataforma, app_versao, sessao_id, props) values ";
await as(A, EV + "('refeicao_registrada','2026-11-02', now(), 'android', '12', gen_random_uuid(), '{\"refeicao_id\":\"cafe\",\"status\":\"ok\"}')");
check((await rows(A, "select aluno_id from eventos"))[0]?.aluno_id === A, "aluno grava evento e aluno_id vem do login");
await fails(A, "insert into eventos(aluno_id, tipo, dia, ocorrido_em) values ($1,'app_aberto','2026-11-02', now())", "aluno não grava evento em nome de outro", [B]);
await fails(A, "insert into eventos(tipo, dia, ocorrido_em, recebido_em) values ('app_aberto','2026-11-02', now(), '2020-01-01')", "app não escolhe a hora de recebimento");
await fails(A, EV + "('tipo_inventado','2026-11-02', now(), 'web', null, null, '{}')", "tipo fora do contrato é recusado");
await as(A, EV + "('tela_vista','2026-11-02', now(), 'web', '28', gen_random_uuid(), '{\"tela\":\"hoje\",\"origem\":\"aba\"}')");
check((await rows(A, "select * from eventos where tipo='tela_vista'")).length === 1, "tipos do contrato v2 são aceitos");
await fails(A, EV + "('app_aberto','2026-11-02', now(), 'ios', null, null, '{}')", "plataforma fora do contrato é recusada");
await fails(A, EV + "('app_aberto','2026-11-02', now(), 'web', null, null, '[1,2]')", "props precisa ser objeto");
const evId = "11111111-1111-4111-8111-111111111111";
const dup = () => as(A, "insert into eventos(id, tipo, dia, ocorrido_em) values ($1,'agua_copo','2026-11-02', now()) on conflict (id) do nothing", [evId]);
await dup(); await dup();
check((await rows(A, "select count(*)::int n from eventos where id=$1", [evId]))[0].n === 1, "reenvio do mesmo lote não duplica");
check((await rows(A, "select * from eventos")).length === 3, "aluno lê os próprios eventos");
await fails(A, "update eventos set tipo='app_aberto'", "ninguém altera evento");
await fails(A, "delete from eventos", "ninguém apaga evento pelo app");
check((await rows(B, "select * from eventos")).length === 0, "outro aluno não vê eventos");
check((await rows(P, "select * from eventos")).length === 0, "personal vinculado não lê eventos crus");
check((await rows(X, "select * from eventos")).length === 0, "admin não lê eventos crus");
await fails(P, "insert into eventos(tipo, dia, ocorrido_em) values ('app_aberto','2026-11-02', now())", "profissional não grava eventos");
await db.exec("set role anon");
let anonEv = true; try { await db.query("insert into eventos(aluno_id, tipo, dia, ocorrido_em) values ($1,'app_aberto','2026-11-02', now())", [A]); anonEv = false } catch (e) {}
await db.exec("reset role");
check(anonEv, "sem login não grava evento");

// fase 2: painel do profissional (resumo do dia mandado pelo app em dados._r)
const dia = n => `current_date - ${n}`;
await as(A, `insert into registros_dia(aluno_id, dia, dados) values
  ($1, ${dia(1)}, '{"_r":{"g":4,"m":5,"td":1},"treino":true,"agua":8,"peso":72.0}'),
  ($1, ${dia(2)}, '{"_r":{"g":5,"m":5,"td":0},"cardio":20}'),
  ($1, ${dia(3)}, '{"_r":{"g":2,"m":5,"td":1}}'),
  ($1, ${dia(35)}, '{"_r":{"g":5,"m":5,"td":0},"peso":74.5}')`, [A]);
const pa = await rows(P, "select * from painel_alunos()");
check(pa.length === 1 && pa[0].aluno_id === A, "painel lista só os alunos do profissional");
check(pa[0].dias_sem_registro === 1, "painel conta dias sem registro (alerta de aluno sumido)");
check(Number(pa[0].adesao_dieta_7d) === 0.7333, "adesão à dieta dos 7 dias = seguidas / previstas");
check(pa[0].treinos_7d === 1 && pa[0].treinos_meta_7d === 2, "treinos feitos e previstos na semana");
check(Number(pa[0].peso_atual) === 72 && Number(pa[0].peso_30d) === 74.5, "peso atual e de 30 dias atrás (window function)");
check((await rows(Q, "select * from painel_alunos()")).length === 0, "painel de outro personal vem vazio");
check((await rows(Q, "select * from painel_semanas($1, 8)", [A])).every(r => r.dias_registrados === 0), "série semanal respeita o vínculo (RLS)");
const ps = await rows(P, "select * from painel_semanas($1, 8)", [A]);
check(ps.length === 8, "série semanal tem uma linha por semana, mesmo sem registro");
check(ps.reduce((s, r) => s + r.dias_registrados, 0) === (await rows(P, "select count(*)::int n from registros_dia where aluno_id=$1 and dia >= date_trunc('week', current_date)::date - 49", [A]))[0].n, "série semanal soma todos os dias registrados");
check(ps.some(r => r.peso !== null && Number(r.peso) === 72), "série semanal traz o peso da semana");
check((await rows(X, "select * from painel_alunos()")).length === 0, "admin não vê painel de alunos");

// fase 2: avaliação física com laudo
await as(P, "insert into storage.objects(bucket_id, name) values ('laudos', $1)", [A + "/laudo-1.jpg"]);
await fails(P, "insert into storage.objects(bucket_id, name) values ('laudos', $1)", "personal não envia laudo de aluno sem vínculo", [B + "/laudo.jpg"]);
await fails(P, "insert into storage.objects(bucket_id, name) values ('laudos', 'qualquer/laudo.jpg')", "laudo fora da pasta de um aluno é recusado");
check((await rows(A, "select * from storage.objects where bucket_id='laudos'")).length === 1, "aluno vê o próprio laudo");
check((await rows(B, "select * from storage.objects where bucket_id='laudos'")).length === 0, "outro aluno não vê o laudo");
check((await rows(X, "select * from storage.objects where bucket_id='laudos'")).length === 0, "admin não vê laudos");
await as(P, "insert into avaliacoes(aluno_id, metodo, peso_kg, gordura_pct, massa_magra_kg, tmb_kcal, medidas, laudo_path) values ($1,'bioimpedancia',72,30.5,50,1410,'{\"cintura\":78}',$2)", [A, A + "/laudo-1.jpg"]);
await fails(P, "insert into avaliacoes(aluno_id, laudo_path) values ($1,$2)", "avaliação não aponta para laudo de outro aluno", [A, B + "/laudo.jpg"]);
await fails(P, "insert into avaliacoes(aluno_id, gordura_pct) values ($1, 120)", "% de gordura impossível é recusado", [A]);
await fails(A, "insert into avaliacoes(aluno_id, peso_kg) values ($1, 70)", "aluno não cria avaliação para si (é do profissional)", [A]);
check((await rows(P, "select ultima_avaliacao from painel_alunos()"))[0].ultima_avaliacao !== null, "painel mostra a data da última avaliação");

// fase 2: dieta do nutricionista (uma ativa por aluno)
await as(N, "insert into planos_dieta(aluno_id, dados) values ($1, '{\"refeicoes\":[{\"nome\":\"Café\"}],\"kcal\":1500}')", [A]);
check((await rows(A, "select count(*)::int n from planos_dieta where ativo"))[0].n === 1, "dieta nova do nutri desativa a anterior");
check((await rows(A, "select count(*)::int n from planos_dieta"))[0].n === 2, "dieta antiga fica no histórico");
await fails(N, "insert into planos_dieta(aluno_id, dados) values ($1, '[1]')", "dieta precisa ser um objeto", [A]);
await fails(P, "insert into planos_dieta(aluno_id, dados) values ($1, '{}')", "personal continua sem escrever dieta", [A]);

// fase 2: LGPD, baixar meus dados
const ex = (await rows(A, "select exportar_meus_dados() j"))[0].j;
check(ex.perfil.id === A && ex.registros_dia.length >= 4 && ex.avaliacoes.length >= 2 && ex.planos_dieta.length === 2, "aluno baixa todos os próprios dados");
check(ex.vinculos.length === 2 && ex.vinculos.every(v => v.com && !v.aluno_id), "export mostra os profissionais só pelo nome");
check(ex.eventos_de_uso.length >= 1 && ex.quem_mexeu_nos_meus_dados.length >= 4, "export traz eventos de uso e a auditoria");
check(ex.avaliacoes.every(a => !("autor_id" in a)), "export não expõe o id de outras pessoas");
const exP = (await rows(P, "select exportar_meus_dados() j"))[0].j;
check(exP.registro_profissional.registro === "012345-G/SP" && exP.registros_dia.length === 0, "profissional baixa os próprios dados, não os do aluno");

// revogar
await as(A, "select revogar_vinculo($1)", [vP]);
check((await rows(P, "select * from registros_dia")).length === 0, "acesso some quando o aluno revoga");
check((await rows(P, "select * from avaliacoes")).length === 0, "personal revogado não vê mais avaliações");
await as(X, "select revisar_profissional($1,'bloqueado','teste')", [N]);
check((await rows(N, "select * from planos_dieta")).length === 0, "profissional bloqueado perde o acesso");
await fails(N, "update profissionais set registro='CRN-3 99999' where id=$1", "bloqueado não reabre o próprio cadastro", [N]);
check((await rows(B, "select * from profiles where id=$1", [A])).length === 0, "aluno não vê perfil de outro aluno");

// documentos do registro (Storage)
await as(Q, "insert into storage.objects(bucket_id, name) values ('documentos', $1)", [Q + "/cref.jpg"]);
await fails(Q, "insert into storage.objects(bucket_id, name) values ('documentos', $1)", "ninguém envia arquivo na pasta de outro", [P + "/cref.jpg"]);
check((await rows(P, "select * from storage.objects")).length === 0, "profissional não vê documento de outro");
check((await rows(X, "select * from storage.objects")).length === 1, "admin vê o documento para conferir");
await as(Q, "update profissionais set documento_path=$2 where id=$1", [Q, Q + "/cref.jpg"]);
await fails(Q, "update profissionais set documento_path=$2 where id=$1", "cadastro não aponta para documento de outro", [Q, P + "/cref.jpg"]);

// excluir conta: some tudo do aluno; o que o nutri escreveu fica com o aluno
await as(X, "select revisar_profissional($1,'aprovado')", [Q]);
const codB = (await db.query("select codigo_vinculo c from profiles where id=$1", [B])).rows[0].c;
await as(Q, "select solicitar_vinculo($1)", [codB]);
await as(B, "select responder_vinculo((select id from vinculos where aluno_id=$1), true)", [B]);
await as(B, "insert into registros_dia(aluno_id, dia) values ($1, '2026-10-29')", [B]);
await as(B, "insert into eventos(tipo, dia, ocorrido_em) values ('app_aberto','2026-10-29', now())");
await as(N, "select excluir_minha_conta()");
check((await db.query("select count(*)::int n from profiles where id=$1", [N])).rows[0].n === 0, "profissional exclui a própria conta");
check((await db.query("select count(*)::int n from restricoes where item='lactose'")).rows[0].n === 1, "alergia anotada pelo nutri fica com o aluno");
await as(B, "select excluir_minha_conta()");
check((await db.query("select count(*)::int n from registros_dia where aluno_id=$1", [B])).rows[0].n === 0, "dados do aluno somem ao excluir a conta");
check((await db.query("select count(*)::int n from vinculos where aluno_id=$1", [B])).rows[0].n === 0, "vínculos somem ao excluir a conta");
check((await db.query("select count(*)::int n from eventos where aluno_id=$1", [B])).rows[0].n === 0, "eventos somem ao excluir a conta");
await as(Q, "select excluir_minha_conta()");
check((await db.query("select count(*)::int n from storage.objects where name like $1", [Q + "/%"])).rows[0].n === 0, "documento some ao excluir a conta");

// reforço de segurança (2026-11-11)
{
  const C = id(20), PP = id(21), NN = id(22);
  await signup(C, {papel: "aluno", nome: "Cris"});
  await signup(PP, {papel: "personal", nome: "Pedro"});
  await signup(NN, {papel: "nutri", nome: "Nara"});
  await as(PP, "insert into profissionais(id, conselho, registro, uf) values ($1,'CREF','111111-G/MG','MG')", [PP]);
  await as(NN, "insert into profissionais(id, conselho, registro, uf) values ($1,'CRN','CRN-9 11111','MG')", [NN]);
  await as(X, "select revisar_profissional($1,'aprovado')", [PP]);
  await as(X, "select revisar_profissional($1,'aprovado')", [NN]);
  const codC = (await db.query("select codigo_vinculo c from profiles where id=$1", [C])).rows[0].c;
  // chutar códigos: 10 erros em 1 hora travam o profissional, inclusive para o código certo
  for (let i = 0; i < 10; i++) await as(PP, "select solicitar_vinculo($1)", ["FF-" + String(i).padStart(8, "0")]);
  await fails(PP, "select solicitar_vinculo($1)", "profissional que erra 10 códigos em 1 hora é travado", [codC]);
  check((await db.query("select count(*)::int n from vinculos where profissional_id=$1", [PP])).rows[0].n === 0, "travado não cria pedido");
  await fails(PP, "select * from tentativas_codigo", "ninguém lê as tentativas pelo app");
  await db.query("delete from tentativas_codigo where profissional_id=$1", [PP]); // passou 1 hora
  check((await rows(PP, "select solicitar_vinculo($1) r", [codC]))[0].r === "pedido enviado", "depois da espera o código certo funciona");
  await as(NN, "select solicitar_vinculo($1)", [codC]);
  for (const v of (await rows(C, "select id from vinculos where aluno_id=$1", [C]))) await as(C, "select responder_vinculo($1, true)", [v.id]);
  // alergia que o aluno cadastrou: o nutri não apaga nem altera
  await as(C, "insert into restricoes(aluno_id, tipo, item, gravidade) values ($1,'alergia','camarão','grave')", [C]);
  await as(NN, "delete from restricoes where aluno_id=$1", [C]);
  await as(NN, "update restricoes set gravidade='leve' where aluno_id=$1", [C]);
  const r0 = (await db.query("select gravidade from restricoes where aluno_id=$1", [C])).rows;
  check(r0.length === 1 && r0[0].gravidade === "grave", "nutri não apaga nem altera alergia cadastrada pelo aluno");
  await as(NN, "insert into restricoes(aluno_id, tipo, item) values ($1,'intolerancia','glúten')", [C]);
  await as(NN, "delete from restricoes where aluno_id=$1 and item='glúten'", [C]);
  check((await db.query("select count(*)::int n from restricoes where aluno_id=$1", [C])).rows[0].n === 1, "nutri apaga o que ele mesmo anotou");
  await as(C, "delete from restricoes where aluno_id=$1", [C]);
  check((await db.query("select count(*)::int n from restricoes where aluno_id=$1", [C])).rows[0].n === 0, "aluno apaga as próprias alergias");
  // treino e dieta: um profissional não mexe no plano de outro
  await as(PP, "insert into planos_treino(aluno_id, dados) values ($1, '{\"a\":1}')", [C]);
  await as(P, "update planos_treino set dados='{}' where aluno_id=$1", [C]);
  check((await db.query("select dados->>'a' a from planos_treino where aluno_id=$1", [C])).rows[0].a === "1", "outro personal não altera treino que não é dele");
  await as(PP, "update planos_treino set dados='{\"a\":2}' where aluno_id=$1", [C]);
  check((await db.query("select dados->>'a' a from planos_treino where aluno_id=$1", [C])).rows[0].a === "2", "autor altera o próprio treino");
  await as(NN, "insert into planos_dieta(aluno_id, dados) values ($1, '{\"refeicoes\":[]}')", [C]);
  check((await rows(C, "select count(*)::int n from planos_dieta where ativo"))[0].n === 1, "dieta do nutri continua chegando ao aluno");
  // tamanhos
  await fails(C, "insert into registros_dia(aluno_id, dia, dados) values ($1, '2026-11-11', $2)", "registro do dia gigante é recusado", [C, JSON.stringify({x: "a".repeat(70000)})]);
  await fails(C, "insert into registros_dia(aluno_id, dia, dados) values ($1, '2026-11-11', '[1]')", "registro do dia precisa ser objeto", [C]);
  await as(C, "insert into registros_dia(aluno_id, dia, dados) values ($1, '2026-11-11', '{\"agua\":3}')", [C]);
  check((await rows(C, "select count(*)::int n from registros_dia where aluno_id=$1", [C]))[0].n === 1, "registro normal continua entrando");
  // funções: sem login ninguém chama; logado não chama as internas
  await fails(C, "select novo_codigo()", "app não chama função interna novo_codigo");
  await db.exec("set role anon");
  let anonFn = true;
  for (const f of ["is_admin()", "painel_alunos()", "exportar_meus_dados()", "solicitar_vinculo('FF-00000000')", "excluir_minha_conta()"]) {
    try { await db.query("select " + f); anonFn = false } catch (e) {}
  }
  await db.exec("reset role");
  check(anonFn, "sem login não chama nenhuma função do banco");
  check((await rows(C, "select is_admin() a"))[0].a === false, "funções das regras continuam funcionando para quem está logado");
}

// anônimo
await db.exec("set role anon");
let anonOk = true; try { await db.query("select * from profiles"); anonOk = false } catch (e) {}
await db.exec("reset role");
check(anonOk, "sem login não lê nada");

console.log(`${ok} regras ok, ${fail} falharam`);
process.exit(fail ? 1 : 0);
