"""O gerador respeita o contrato de eventos v1 e v2 (analytics/docs/contrato-eventos.md)."""
import json
import uuid
from datetime import timedelta

import pandas as pd
import pytest

from gerador import (COLUNAS_EVENTOS, COLUNAS_PROFILES, COLUNAS_VINCULOS, Config,
                     TIPOS_EVENTO, gerar, salvar)

PROPS = {
    "app_aberto": set(), "refeicao_registrada": {"refeicao_id", "status"},
    "refeicao_desfeita": {"refeicao_id"}, "treino_feito": {"grupo"}, "treino_desfeito": set(),
    "cardio_registrado": {"minutos"}, "agua_copo": {"delta", "total_dia"}, "peso_registrado": {"kg"},
    "doce_sos_vencido": set(), "bau_aberto": {"premio"}, "promessa_feita": {"texto"},
    "notificacao_tocada": {"tipo", "acao"},
    # v2
    "tela_vista": {"tela", "origem"}, "notificacao_agendada": {"tipo", "quantidade"},
    "experimento_exposto": {"experimento", "grupo"},
}
ETAPAS = {"codigo_pedido", "login_ok", "avatar_ok", "metas_ok", "dieta_ok", "lembretes_ok", "plano_do_professor"}


def test_colunas(tabelas):
    assert list(tabelas["eventos"].columns) == COLUNAS_EVENTOS
    assert list(tabelas["profiles"].columns) == COLUNAS_PROFILES
    assert list(tabelas["vinculos"].columns) == COLUNAS_VINCULOS


def test_tipos_e_props(ev):
    assert set(ev["tipo"]) == set(TIPOS_EVENTO)
    for tipo, props in zip(ev["tipo"], ev["props"]):
        p = json.loads(props)
        assert p.pop("sintetico") is True
        if tipo == "cadastro_etapa":
            assert set(p) in ({"etapa", "papel"}, {"etapa", "papel", "novo"}) and p["etapa"] in ETAPAS
            continue
        assert set(p) == PROPS[tipo], tipo
        if tipo == "refeicao_registrada":
            assert p["status"] in {"ok", "troca", "pulei"}
        elif tipo == "agua_copo":
            assert p["delta"] in (1, -1) and p["total_dia"] >= 0
        elif tipo == "cardio_registrado":
            assert isinstance(p["minutos"], int) and p["minutos"] > 0
        elif tipo == "peso_registrado":
            assert 35 < p["kg"] < 160
        elif tipo == "notificacao_tocada":
            assert p["tipo"] in {"refeicao", "treino", "agua", "lembrete"} and p["acao"] in {"ok", "abrir", "fechar_dia"}
        elif tipo == "tela_vista":
            assert p["origem"] in {"aba", "notificacao", "botao", "abertura"} and p["tela"]
        elif tipo == "notificacao_agendada":
            assert p["tipo"] in {"refeicao", "treino", "agua", "lembrete"} and isinstance(p["quantidade"], int)
        elif tipo == "experimento_exposto":
            assert p["grupo"] in {"A", "B"}


def test_ids_unicos_uuid(tabelas):
    for nome in ("eventos", "profiles", "vinculos"):
        ids = tabelas[nome]["id"]
        assert ids.is_unique
        assert all(uuid.UUID(i).version == 4 for i in ids.head(500))


def test_chaves_estrangeiras(tabelas):
    prof = tabelas["profiles"]
    alunos = set(prof.loc[prof["papel"] == "aluno", "id"])
    assert set(tabelas["eventos"]["aluno_id"]) <= alunos
    v = tabelas["vinculos"]
    assert set(v["aluno_id"]) <= alunos
    assert set(v["profissional_id"]) <= set(tabelas["profissionais"]["id"])
    assert set(v["status"]) <= {"pendente", "ativo", "recusado", "revogado"}
    assert not v.duplicated(["aluno_id", "profissional_id"]).any()


def test_tempos(ev, tabelas):
    assert ev["plataforma"].isin(["android", "web"]).all()
    assert (ev["recebido_em"] >= ev["ocorrido_em"]).all()
    assert (ev["hora_local"].dt.date == ev["dia"]).all()  # dia = dia local do aparelho
    cad = tabelas["profiles"].set_index("id")["criado_em"]
    assert (ev["ocorrido_em"] >= ev["aluno_id"].map(cad)).all()
    assert ev["dia"].max() <= Config().fim


def test_sessoes_comecam_com_app_aberto(ev):
    primeiro = ev.sort_values(["ocorrido_em", "tipo"], key=lambda s: s if s.name == "ocorrido_em" else s != "app_aberto") \
                 .groupby("sessao_id").head(1)
    assert (primeiro["tipo"] == "app_aberto").all()


def test_agua_total_do_dia_bate(ev):
    a = ev[ev["tipo"] == "agua_copo"].copy()
    p = a["props"].map(json.loads)
    a["delta"], a["total"] = p.str["delta"], p.str["total_dia"]
    a = a.sort_values("ocorrido_em")
    assert (a.groupby(["aluno_id", "dia"])["delta"].cumsum() == a["total"]).all()


def test_reprodutivel():
    a, b = gerar(Config(alunos=15, seed=3)), gerar(Config(alunos=15, seed=3))
    for nome in a:
        pd.testing.assert_frame_equal(a[nome], b[nome])
    c = gerar(Config(alunos=15, seed=4))
    assert not a["eventos"]["id"].equals(c["eventos"]["id"])


def test_salvar_csv_e_parquet(tmp_path):
    t = gerar(Config(alunos=8, seed=1))
    arquivos = salvar(t, tmp_path)
    assert (tmp_path / "eventos.parquet").exists() and (tmp_path / "gabarito" / "gabarito_dias.csv").exists()
    assert len(arquivos) == 2 * len(t)
    back = pd.read_parquet(tmp_path / "eventos.parquet")
    pd.testing.assert_frame_equal(back, t["eventos"], check_dtype=False)
    csv = pd.read_csv(tmp_path / "eventos.csv")
    assert csv["ocorrido_em"].str.match(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$").all()
