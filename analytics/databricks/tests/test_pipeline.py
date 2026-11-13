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
    ]
    sint_vinculos = [{"aluno_id": A, "profissional_id": P1, "estado": "ativo", "criado_em": "2026-05-31T11:00:00-03:00"}]
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
    assert por_origem == {"sintetico": 29, "supabase": 2}


def test_prata_rejeita_e_deduplica(lake):
    motivos = {r.motivo_rejeicao for r in lake.table("foco_fit_prata.eventos_rejeitados").collect()}
    assert motivos == {"tipo fora do contrato", "peso fora de 25..350 kg"}
    # 31 na bronze - 2 rejeitados - 1 id repetido - 1 reenvio com outro id
    assert lake.table("foco_fit_prata.eventos").count() == 27


def test_prata_pseudonimiza(lake):
    cols = set(lake.table("foco_fit_prata.eventos").columns)
    assert "aluno_id" not in cols
    hashes = {r.aluno_hash for r in lake.table("foco_fit_prata.eventos").select("aluno_hash").distinct().collect()}
    assert hashes == {_hash(lake, A), _hash(lake, B), _hash(lake, C)}


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
    assert c["rejeitados"].valor == pytest.approx(2 / 29) and c["rejeitados"].status == "alerta"
    assert c["duplicados"].valor == pytest.approx(2 / 29) and c["duplicados"].status == "alerta"
    assert c["props_incompletas"].status == "ok" and c["sem_sessao"].status == "ok"
