"""Os padrões que o README promete existem nos dados (com folga)."""
import json

import numpy as np
import pandas as pd


def _ativos_por_semana(dias, max_sem):
    # Quem abandonou conta como inativo até o fim (curva de adesão da coorte).
    g = dias[dias["semana_desde_cadastro"] < max_sem]
    tot = g.groupby("aluno_id")["semana_desde_cadastro"].max()
    return g.groupby("semana_desde_cadastro")["ativo"].sum() / dias["aluno_id"].nunique(), tot


def test_h1_adesao_cai_com_as_semanas(dias):
    taxa, _ = _ativos_por_semana(dias, 12)
    assert taxa[0] > taxa[3] > taxa[8]


def test_h2_fim_de_semana_mais_fraco(dias):
    a = dias.groupby(["perfil", "fds"])["ativo"].mean().unstack()
    assert a.loc["fim_de_semana_fraco", True] < .6 * a.loc["fim_de_semana_fraco", False]
    assert a.loc["disciplinado", True] > .85 * a.loc["disciplinado", False]


def test_h3_desistente_precoce_some_no_primeiro_mes(tabelas):
    g = tabelas["gabarito_alunos"].merge(tabelas["profiles"][["id", "criado_em"]], left_on="aluno_id", right_on="id")
    fim = pd.to_datetime(g["abandonou_em"])
    ini = pd.to_datetime(g["criado_em"].dt.tz_convert("America/Sao_Paulo").dt.date)
    g["abandonou_30d"] = (fim - ini).dt.days <= 30  # NaT (não abandonou) vira False
    taxa = g.groupby("perfil")["abandonou_30d"].mean()
    assert taxa["desistente_precoce"] > .6 and taxa["disciplinado"] < .1


def test_h3_sequencia_longa_protege(dias):
    d = dias.sort_values(["aluno_id", "dia"]).copy()
    d["ultimo_dia"] = ~d["aluno_id"].duplicated(keep="last")
    d["abandona_amanha"] = d["ultimo_dia"] & d["abandonou_em"].notna()
    long, curta = d[d["sequencia"] >= 7], d[d["sequencia"] < 7]
    assert long["abandona_amanha"].mean() < .6 * curta["abandona_amanha"].mean()


def test_h4_cardio_mais_fraco_que_treino(dias):
    a = dias[dias["ativo"]]
    assert (a["cardio_min"] > 0).mean() < .6 * a.loc[a["dia_de_treino"], "treinou"].mean()


def test_h5_peso_cai_para_quem_adere(dias):
    e = dias[dias["objetivo"] == "emagrecer"].sort_values("dia")
    g = e.groupby("aluno_id").agg(p0=("peso_real", "first"), p1=("peso_real", "last"), n=("dia", "size"),
                                  bons=("refeicoes_boas", "sum"), tot=("refeicoes_principais", "sum"))
    g = g[g["n"] >= 28]
    g["kg_sem"] = (g["p1"] - g["p0"]) / (g["n"] / 7)
    g["adesao"] = g["bons"] / g["tot"]
    alta, baixa = g[g["adesao"] >= .7]["kg_sem"], g[g["adesao"] < .4]["kg_sem"]
    assert -.6 < alta.mean() < -.15  # devagar: entre 150 e 600 g por semana
    assert alta.mean() < baixa.mean() - .1
    assert np.corrcoef(g["adesao"], g["kg_sem"])[0, 1] < -.4


def test_h6_refeicoes_nos_horarios_das_refeicoes(ev):
    h = ev.loc[ev["tipo"] == "refeicao_registrada", "hora_local"].dt.hour
    assert h.between(0, 4).mean() < .01
    janelas = h.between(6, 9) | h.between(11, 13) | h.between(15, 17) | h.between(20, 22)
    assert janelas.mean() > .7


def test_h7_personal_ativo_treina_mais(dias):
    t = dias[dias["ativo"] & dias["dia_de_treino"]].groupby("tem_personal_ativo")["treinou"].mean()
    assert t[True] > t[False] + .03


def test_h8_notificacao_puxa_registro(ev):
    n = ev[ev["tipo"] == "notificacao_tocada"]
    assert set(n["plataforma"]) == {"android"}  # web não tem notificação
    p = n["props"].map(json.loads)
    assert (p.str["tipo"] == "refeicao").mean() > .5


def test_h9_web_registra_menos_refeicoes_por_dia(ev):
    r = ev[ev["tipo"] == "refeicao_registrada"].groupby(["plataforma", "aluno_id", "dia"]).size()
    m = r.groupby("plataforma").mean()
    assert m["web"] < m["android"]


def test_h11_botao_fechar_dia_puxa_registro(ev):
    """Quem toca em "Fechar o dia" no aviso das 21 h registra mais nos 15 min seguintes do que quem só abre."""
    e = ev[ev["hora_local"].dt.hour >= 21]
    n = e[(e["tipo"] == "notificacao_tocada") & e["props"].str.contains('"lembrete"')]
    n = n.assign(acao=n["props"].map(json.loads).str["acao"])
    reg = e[e["tipo"] == "refeicao_registrada"][["aluno_id", "ocorrido_em"]]
    m = n.merge(reg, on="aluno_id", suffixes=("", "_reg"))
    dt = (m["ocorrido_em_reg"] - m["ocorrido_em"]).dt.total_seconds()
    puxou = set(m.loc[dt.between(0, 15 * 60), "id"])
    taxa = n.assign(puxou=n["id"].isin(puxou)).groupby("acao")["puxou"].mean()
    assert taxa["fechar_dia"] > taxa["abrir"] + .2


def test_h12_cadastro_perde_gente_antes_das_metas(ev):
    c = ev[ev["tipo"] == "cadastro_etapa"]
    etapa = c["props"].map(json.loads).str["etapa"]
    alunos = c.assign(etapa=etapa).groupby("etapa")["aluno_id"].nunique()
    assert alunos["login_ok"] > alunos["avatar_ok"] > alunos["lembretes_ok"]
    assert .6 < alunos["lembretes_ok"] / alunos["login_ok"] < .95
