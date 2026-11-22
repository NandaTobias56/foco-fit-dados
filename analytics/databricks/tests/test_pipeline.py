"""Teste ponta a ponta do lakehouse com um conjunto pequeno de dados feito à mão.

Rodar a partir de analytics/databricks:  pip install -r tests/requirements.txt && pytest tests
Precisa de Java 17+ (PySpark 4).
"""
import json
import uuid
from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")
pd = pytest.importorskip("pandas")

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from executar_local import executar_pipeline  # noqa: E402

A = "aaaaaaaa-0000-0000-0000-000000000001"   # aluno sintético, caso principal
B = "bbbbbbbb-0000-0000-0000-000000000002"   # aluno sintético, só abriu o app
C = "cccccccc-0000-0000-0000-000000000003"   # aluno real (exportação do Supabase)
P1 = "dddddddd-0000-0000-0000-000000000004"  # personal sintético
P2 = "eeeeeeee-0000-0000-0000-000000000005"  # personal real
D = "ffffffff-0000-0000-0000-000000000006"   # aluno sintético com dieta da nutri (contrato v3)
N1 = "abababab-0000-0000-0000-000000000007"  # nutricionista sintética
SEG = date(2026, 6, 1)  # segunda-feira


def _ev(aluno, tipo, dia, hora=12, minuto=0, props=None, sintetico=True, id_=None, sessao="s1"):
    d = SEG + timedelta(days=dia)
    p = dict(props or {})
    if sintetico:
        p["sintetico"] = True
    oc = datetime(d.year, d.month, d.day, hora, minuto)
    return {
        "id": id_ or str(uuid.uuid4()), "aluno_id": aluno, "tipo": tipo, "dia": d.isoformat(),
        "ocorrido_em": oc.isoformat() + "-03:00", "recebido_em": (oc + timedelta(seconds=5)).isoformat() + "-03:00",
        "plataforma": "android", "app_versao": "13", "sessao_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, aluno + sessao)),
        "props": json.dumps(p),
    }


def _plano(dieta_id, kcal):
    return {"dieta_id": dieta_id, "kcal": kcal, "proteina_g": 120, "carbo_g": 160, "gordura_g": 50,
            "refeicoes": [{"refeicao_id": "cafe", "tipo": "cafe", "principal": True, "fatia": .2},
                          {"refeicao_id": "lanche", "tipo": "lanche", "principal": False, "fatia": .15},
                          {"refeicao_id": "almoco", "tipo": "almoco", "principal": True, "fatia": .35},
                          {"refeicao_id": "jantar", "tipo": "jantar", "principal": True, "fatia": .3}]}


def _dados():
    ev = [
        # Semana 1 do aluno A
        _ev(A, "app_aberto", 0, 7),
        _ev(A, "refeicao_registrada", 0, 8, props={"refeicao_id": "cafe", "status": "ok"}),
        _ev(A, "refeicao_registrada", 0, 12, props={"refeicao_id": "almoco", "status": "troca"}),
        _ev(A, "refeicao_registrada", 0, 16, props={"refeicao_id": "lanche", "status": "ok"}),
        _ev(A, "refeicao_desfeita", 0, 16, 5, props={"refeicao_id": "lanche"}),
        _ev(A, "refeicao_registrada", 0, 20, props={"refeicao_id": "jantar", "status": "pulei"}),
        _ev(A, "treino_feito", 0, 18, props={"grupo": "Glúteos"}),
        _ev(A, "agua_copo", 0, 9, props={"delta": 1, "total_dia": 1}),
        _ev(A, "agua_copo", 0, 10, props={"delta": 1, "total_dia": 2}),
        _ev(A, "agua_copo", 0, 11, props={"delta": 1, "total_dia": 3}),
        _ev(A, "peso_registrado", 0, 7, 30, props={"kg": 70.0}),
        _ev(A, "treino_feito", 1, 18, props={"grupo": "costas"}),
        _ev(A, "treino_desfeito", 1, 18, 10),
        _ev(A, "cardio_registrado", 1, 19, props={"minutos": 15}),
        _ev(A, "agua_copo", 1, 15, props={"delta": 1, "total_dia": 5}),
        _ev(A, "refeicao_registrada", 2, 8, props={"refeicao_id": "cafe", "status": "ok"}),
        _ev(A, "refeicao_registrada", 2, 12, props={"refeicao_id": "almoco", "status": "ok"}),
        _ev(A, "treino_feito", 2, 18, props={"grupo": "quadriceps"}),
        _ev(A, "agua_copo", 4, 9, props={"delta": 1, "total_dia": 1}),
        _ev(A, "agua_copo", 5, 9, props={"delta": 1, "total_dia": 1}),
        _ev(A, "agua_copo", 6, 9, props={"delta": 1, "total_dia": 1}),
        # D7 do aluno A (06-08) e peso 3 semanas depois
        _ev(A, "app_aberto", 7, 7),
        _ev(A, "peso_registrado", 21, 7, props={"kg": 69.0}),
        # Aluno B: só abriu o app
        _ev(B, "app_aberto", 0, 7),
        # Aluno D (contrato v3): dieta da nutri, porção, nota da troca e SOS doce, semana de 06-15
        _ev(D, "app_aberto", 14, 7, sessao="d14"),
        _ev(D, "dieta_recebida", 14, 7, 5, sessao="d14", props=_plano("dieta-1", 1600)),
        _ev(D, "refeicao_registrada", 14, 8, sessao="d14", props={"refeicao_id": "cafe", "status": "ok", "porcao": 1}),
        _ev(D, "refeicao_registrada", 14, 12, sessao="d14", props={"refeicao_id": "almoco", "status": "troca", "nota": "mod", "porcao": 1.5}),
        _ev(D, "refeicao_registrada", 14, 20, sessao="d14", props={"refeicao_id": "jantar", "status": "ok", "porcao": 0.5}),
        _ev(D, "sos_doce_iniciado", 14, 16, sessao="d14"),
        _ev(D, "doce_sos_vencido", 14, 16, 10, sessao="d14"),
        _ev(D, "app_aberto", 15, 8, sessao="d15"),
        _ev(D, "refeicao_registrada", 15, 8, 1, sessao="d15", props={"refeicao_id": "cafe", "status": "ok", "porcao": 1}),
        _ev(D, "refeicao_registrada", 15, 12, sessao="d15", props={"refeicao_id": "almoco", "status": "troca", "nota": "no", "porcao": 1}),
        _ev(D, "sos_doce_iniciado", 15, 21, sessao="d15"),
        _ev(D, "sos_doce_cedeu", 15, 21, 5, sessao="d15"),
        _ev(D, "sos_doce_iniciado", 15, 22, sessao="d15"),
        _ev(D, "app_aberto", 19, 9, sessao="d19"),
        _ev(D, "dieta_recebida", 19, 9, 1, sessao="d19", props=_plano("dieta-2", 1500)),
        _ev(D, "refeicao_registrada", 19, 9, 5, sessao="d19", props={"refeicao_id": "cafe", "status": "pulei"}),
        _ev(D, "refeicao_registrada", 19, 13, sessao="d19", props={"refeicao_id": "almoco", "status": "troca", "nota": "no", "porcao": 1}),
        _ev(D, "refeicao_registrada", 19, 20, sessao="d19", props={"refeicao_id": "jantar", "status": "ok", "porcao": 1.5}),
        _ev(D, "refeicao_registrada", 19, 16, sessao="d19", props={"refeicao_id": "lanche", "status": "ok", "porcao": 2}),  # inválido
        # Inválidos
        _ev(A, "tipo_inventado", 3),
        _ev(A, "peso_registrado", 3, props={"kg": 900}),
    ]
    cardio = _ev(A, "cardio_registrado", 0, 19, props={"minutos": 20})
    reenvio = dict(cardio, id=str(uuid.uuid4()))       # fila offline reenviou: mesmo conteúdo, outro id
    ev += [cardio, reenvio, dict(ev[1])]                # ev[1] repetido com o mesmo id
    sint_profiles = [
        {"id": A, "papel": "aluno", "nascimento": "1995-03-10", "criado_em": "2026-05-31T10:00:00-03:00"},
        {"id": B, "papel": "aluno", "nascimento": "", "criado_em": "2026-05-31T10:00:00-03:00"},
        {"id": P1, "papel": "personal", "nascimento": "1988-01-01", "criado_em": "2026-01-01T10:00:00-03:00"},
        {"id": D, "papel": "aluno", "nascimento": "1992-05-05", "criado_em": "2026-06-14T10:00:00-03:00"},
        {"id": N1, "papel": "nutri", "nascimento": "1985-02-02", "criado_em": "2026-01-01T10:00:00-03:00"},
    ]
    sint_vinculos = [{"aluno_id": A, "profissional_id": P1, "estado": "ativo", "criado_em": "2026-05-31T11:00:00-03:00"},
                     {"aluno_id": D, "profissional_id": N1, "tipo": "nutri", "estado": "ativo",
                      "criado_em": "2026-06-14T11:00:00-03:00"}]
    supa_ev = [_ev(C, "treino_feito", 9, sintetico=False, props={"grupo": "costas"}),
               _ev(C, "app_aberto", 9, sintetico=False)]
    supa_profiles = [
        {"id": C, "papel": "aluno", "nascimento": "2000-07-07", "criado_em": "2026-06-09T10:00:00-03:00"},
        {"id": P2, "papel": "personal", "nascimento": "1990-01-01", "criado_em": "2026-01-01T10:00:00-03:00"},
    ]
    supa_vinculos = [{"id": str(uuid.uuid4()), "aluno_id": C, "profissional_id": P2, "tipo": "personal",
                      "status": "ativo", "criado_em": "2026-06-09T11:00:00-03:00"}]
    return ev, sint_profiles, sint_vinculos, supa_ev, supa_profiles, supa_vinculos


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    raiz = tmp_path_factory.mktemp("lake")
    volume = raiz / "volume"
    (volume / "sintetico").mkdir(parents=True)
    (volume / "supabase").mkdir(parents=True)
    ev, sp, sv, qe, qp, qv = _dados()
    pd.DataFrame(ev).to_parquet(volume / "sintetico" / "eventos.parquet", index=False)
    pd.DataFrame(sp).to_csv(volume / "sintetico" / "profiles.csv", index=False)
    pd.DataFrame(sv).to_csv(volume / "sintetico" / "vinculos.csv", index=False)
    pd.DataFrame(qe).to_csv(volume / "supabase" / "eventos.csv", index=False)
    pd.DataFrame(qp).to_csv(volume / "supabase" / "profiles.csv", index=False)
    pd.DataFrame(qv).to_csv(volume / "supabase" / "vinculos.csv", index=False)

    spark = (SparkSession.builder.master("local[2]").appName("foco-fit-teste")
             .config("spark.sql.warehouse.dir", str(raiz / "warehouse"))
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.ansi.enabled", "true")  # como no Databricks serverless
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    executar_pipeline(spark, volume, widgets={"fonte_supabase": "arquivos"},
                      segredos={("foco_fit", "hash_sal"): "sal-de-teste"})
    yield spark
    spark.stop()


def _hash(spark, id_):
    return spark.sql(f"select sha2(concat('sal-de-teste', '{id_}'), 256) h").first().h


def test_bronze_guarda_origem(lake):
    por_origem = {r["_origem"]: r["count"] for r in lake.table("foco_fit_bronze.eventos").groupBy("_origem").count().collect()}
    assert por_origem == {"sintetico": 48, "supabase": 2}


def test_prata_rejeita_e_deduplica(lake):
    motivos = {r.motivo_rejeicao for r in lake.table("foco_fit_prata.eventos_rejeitados").collect()}
    assert motivos == {"tipo fora do contrato", "peso fora de 25..350 kg", "porção fora de 0,5/1/1,5"}
    # 50 na bronze - 3 rejeitados - 1 id repetido - 1 reenvio com outro id
    assert lake.table("foco_fit_prata.eventos").count() == 45


def test_prata_pseudonimiza(lake):
    cols = set(lake.table("foco_fit_prata.eventos").columns)
    assert "aluno_id" not in cols
    hashes = {r.aluno_hash for r in lake.table("foco_fit_prata.eventos").select("aluno_hash").distinct().collect()}
    assert hashes == {_hash(lake, A), _hash(lake, B), _hash(lake, C), _hash(lake, D)}


def test_fatos_do_dia(lake):
    ha = _hash(lake, A)
    ref = lake.table("foco_fit_ouro.fato_refeicao").filter(f"aluno_hash = '{ha}' and dia = '2026-06-01'")
    assert {(r.refeicao_id, r.status) for r in ref.collect()} == {("cafe", "ok"), ("almoco", "troca"), ("jantar", "pulei")}
    treinos = lake.table("foco_fit_ouro.fato_treino_dia").filter(f"aluno_hash = '{ha}'")
    assert {(str(r.dia), r.grupo_id) for r in treinos.collect()} == {("2026-06-01", "gluteo"), ("2026-06-03", "quadriceps")}
    cardio = {str(r.dia): r.minutos for r in lake.table("foco_fit_ouro.fato_cardio_dia").filter(f"aluno_hash = '{ha}'").collect()}
    assert cardio == {"2026-06-01": 20, "2026-06-02": 15}
    agua = lake.table("foco_fit_ouro.fato_agua_dia").filter(f"aluno_hash = '{ha}' and dia = '2026-06-01'").first()
    assert agua.copos == 3 and agua.toques == 3


def test_kpis_da_primeira_semana(lake):
    ha = _hash(lake, A)
    k = lake.table("foco_fit_ouro.kpi_aluno_semana").filter(f"aluno_hash = '{ha}' and semana_inicio = '2026-06-01'").first()
    assert k.refeicoes_previstas == 35
    assert (k.refeicoes_ok, k.refeicoes_troca, k.refeicoes_puladas) == (3, 1, 1)
    assert k.adesao_dieta == pytest.approx(4 / 35, abs=1e-4)
    assert k.taxa_troca == pytest.approx(0.2)
    assert k.treinos_semana == 2
    assert k.cardio_minutos == 35
    assert k.hidratacao_media_copos == pytest.approx(2.2)
    assert k.dias_registrados == 6
    assert k.sequencia_fim_semana == 3
    assert k.personal_hash == _hash(lake, P1)


def test_semanas_sem_registro_entram(lake):
    hb = _hash(lake, B)
    semanas = lake.table("foco_fit_ouro.kpi_aluno_semana").filter(f"aluno_hash = '{hb}'")
    assert semanas.count() == 4
    assert semanas.filter("semana_ativa").count() == 0


def test_variacao_de_peso(lake):
    ha = _hash(lake, A)
    k = lake.table("foco_fit_ouro.kpi_aluno_semana").filter(f"aluno_hash = '{ha}' and semana_inicio = '2026-06-22'").first()
    assert k.variacao_peso_4sem_kg == pytest.approx(-1.0)
    assert k.dias_no_periodo == 1 and not k.semana_completa


def test_retencao(lake):
    r = lake.table("foco_fit_ouro.kpi_retencao_coorte").filter("coorte_semana = '2026-06-01' and _origem = 'sintetico'").first()
    assert (r.alunos, r.elegiveis_d7, r.retidos_d7) == (2, 2, 1)
    assert r.retencao_d7 == pytest.approx(0.5)
    assert r.elegiveis_d30 == 0 and r.retencao_d30 is None


def test_origens_separadas(lake):
    dim = {r.aluno_hash: r._origem for r in lake.table("foco_fit_ouro.dim_aluno").collect()}
    assert dim[_hash(lake, C)] == "supabase"
    assert dim[_hash(lake, A)] == "sintetico"
    c = lake.table("foco_fit_ouro.dim_aluno").filter(f"aluno_hash = '{_hash(lake, C)}'").first()
    assert c.personal_hash == _hash(lake, P2)


def test_qualidade_passou(lake):
    q = lake.table("foco_fit_ouro.qualidade_execucoes")
    assert q.filter("critica and not ok").count() == 0


def test_jornada_sessao(lake):
    s = lake.table("foco_fit_ouro.jornada_sessao")
    b = s.filter(f"aluno_hash = '{_hash(lake, B)}'").collect()
    assert len(b) == 1 and not b[0].converteu and b[0].caminho == "(abriu e saiu)"
    a = s.filter(f"aluno_hash = '{_hash(lake, A)}'").first()
    assert a.converteu and a.primeira_acao == "peso_registrado" and a.entrada == "direta"
    assert a.caminho.startswith("peso_registrado > refeicao_registrada")


def test_transicoes_somam_um(lake):
    t = lake.table("foco_fit_ouro.jornada_transicoes")
    for r in t.groupBy("_origem", "plataforma", "evento").agg(F.sum("probabilidade").alias("p")).collect():
        assert r.p == pytest.approx(1.0)


def test_tagueamento_aponta_rejeitados_e_duplicados(lake):
    c = {r.regra: r for r in lake.table("foco_fit_ouro.tagueamento_checagens")
         .filter("_origem = 'sintetico' and plataforma = 'todas'").collect()}
    assert c["rejeitados"].valor == pytest.approx(3 / 48) and c["rejeitados"].status == "alerta"
    assert c["duplicados"].valor == pytest.approx(2 / 48) and c["duplicados"].status == "alerta"
    assert c["props_incompletas"].status == "ok" and c["sem_sessao"].status == "ok"


# ---------------------------------------------------------------- contrato v3 (07_nutricao)
def test_prata_v3_tipada(lake):
    hd = _hash(lake, D)
    e = lake.table("foco_fit_prata.eventos").filter(f"aluno_hash = '{hd}' and tipo = 'refeicao_registrada' and dia = '2026-06-15'")
    assert {(r.refeicao_id, r.refeicao_porcao, r.troca_nota) for r in e.collect()} == {
        ("cafe", 1.0, None), ("almoco", 1.5, "mod"), ("jantar", 0.5, None)}
    d = lake.table("foco_fit_prata.eventos").filter(f"aluno_hash = '{hd}' and tipo = 'dieta_recebida'").orderBy("dia").first()
    assert d.dieta_kcal == 1600 and d.dieta_proteina_g == 120 and len(d.dieta_refeicoes) == 4


def test_plano_com_vigencia(lake):
    hd = _hash(lake, D)
    p = {r.dieta_id: r for r in lake.table("foco_fit_ouro.fato_plano_dieta").filter(f"aluno_hash = '{hd}'").collect()}
    assert (str(p["dieta-1"].vigente_de), str(p["dieta-1"].vigente_ate)) == ("2026-06-15", "2026-06-19")
    assert str(p["dieta-2"].vigente_de) == "2026-06-20" and p["dieta-2"].vigente_ate is None and p["dieta-2"].vigente_hoje
    assert p["dieta-1"].refeicoes_principais == 3
    r = lake.table("foco_fit_ouro.fato_refeicao_plano").filter(f"aluno_hash = '{hd}'")
    assert r.count() == 12  # 3 dias registrados x 4 refeições do plano


def test_consumido_x_plano(lake):
    hd = _hash(lake, D)
    c = {str(r.dia): r for r in lake.table("foco_fit_ouro.fato_consumo_dia").filter(f"aluno_hash = '{hd}'").collect()}
    # 1600 x (0,2 + 0,35 x 1,5 x 1,25 + 0,3 x 0,5)
    assert c["2026-06-15"].dia_completo and c["2026-06-15"].kcal_estimado == pytest.approx(1610.0)
    assert c["2026-06-15"].proteina_g_estimada == pytest.approx(120 * 1.00625, abs=.1)
    assert not c["2026-06-16"].dia_completo and c["2026-06-16"].kcal_estimado is None  # jantar não marcado
    # 1500 x (0 + 0,35 x 1,6 + 0,3 x 1,5): café pulado, troca ruim no almoço
    assert c["2026-06-20"].kcal_estimado == pytest.approx(1515.0) and c["2026-06-20"].kcal_meta == 1500


def test_sos_e_escapes(lake):
    hd = _hash(lake, D)
    s = {str(r.dia): r for r in lake.table("foco_fit_ouro.fato_sos_dia").filter(f"aluno_hash = '{hd}'").collect()}
    assert (s["2026-06-15"].vontades, s["2026-06-15"].vencidas, s["2026-06-15"].escapes) == (1, 1, 0)
    assert (s["2026-06-16"].vontades, s["2026-06-16"].cedidas, s["2026-06-16"].vontades_sem_desfecho,
            s["2026-06-16"].trocas_ruins, s["2026-06-16"].escapes) == (2, 1, 1, 1, 2)
    assert s["2026-06-20"].escapes == 1 and s["2026-06-20"].vontades == 0


def test_kpi_nutri_aluno_semana(lake):
    hd = _hash(lake, D)
    k = lake.table("foco_fit_ouro.kpi_nutri_aluno_semana").filter(f"aluno_hash = '{hd}'").collect()
    assert len(k) == 1
    k = k[0]
    assert str(k.semana_inicio) == "2026-06-15" and k.nutri_hash == _hash(lake, N1)
    assert k.dias_com_plano == 3 and k.adesao_principais == pytest.approx(5 / 9, abs=1e-4)
    assert k.refeicao_mais_dificil == "almoco" and k.adesao_refeicao_mais_dificil == pytest.approx(1 / 3, abs=1e-4)
    assert (k.escapes, k.trocas_ruins, k.sos_vontades, k.sos_vencidas, k.sos_cedidas) == (3, 2, 3, 1, 1)
    assert k.taxa_vitoria_sos == pytest.approx(1 / 3, abs=1e-4)
    assert k.dias_completos == 2 and k.razao_consumo == pytest.approx((1.00625 + 1.01) / 2, abs=1e-4)
    assert k.consumo_na_faixa and k.porcao_mais_pct == pytest.approx(2 / 7, abs=1e-4)


def test_kpi_refeicao_e_nutricionista(lake):
    hd = _hash(lake, D)
    r = {x.refeicao_id: x for x in lake.table("foco_fit_ouro.kpi_refeicao_semana").filter(f"aluno_hash = '{hd}'").collect()}
    assert (r["almoco"].troca_moderada, r["almoco"].troca_ruim, r["almoco"].dias) == (1, 2, 3)
    assert r["jantar"].nao_marcou == 1 and r["cafe"].pulou == 1 and r["lanche"].nao_marcou == 3
    n = lake.table("foco_fit_ouro.kpi_nutricionista_semana").filter(f"nutri_hash = '{_hash(lake, N1)}'").first()
    assert n.alunos_com_plano == 1 and n.refeicao_mais_dificil == "almoco" and n.alunos_fora_da_faixa == 0
    b = lake.table("foco_fit_ouro.kpi_refeicao_base").filter("_origem = 'sintetico'").collect()
    assert {x.refeicao_id for x in b} == {"cafe", "lanche", "almoco", "jantar"}
