"""Gerador de dados sintéticos do Foco Fit.

Produz as tabelas do contrato de eventos v1 (eventos, profiles, vinculos) mais
`profissionais` e um gabarito com o que foi simulado (perfil oculto de cada
aluno, dia a dia), para testar hipóteses nas disciplinas de estatística e ML.

Tudo é reprodutível: a mesma seed e os mesmos parâmetros geram exatamente os
mesmos arquivos. Uso:

    python gerar.py                      # 500 alunos, 20 profissionais, mai-out/2026
    python gerar.py --alunos 100 --seed 7 --formatos csv
"""
from __future__ import annotations

import json
import math
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

# ---------------------------------------------------------------- constantes
FUSO = timezone(timedelta(hours=-3))  # America/Sao_Paulo (sem horário de verão desde 2019)

TIPOS_EVENTO = [
    "app_aberto", "refeicao_registrada", "refeicao_desfeita", "treino_feito",
    "treino_desfeito", "cardio_registrado", "agua_copo", "peso_registrado",
    "doce_sos_vencido", "bau_aberto", "promessa_feita", "notificacao_tocada",
    # contrato v2 (2026-11-08): navegação, cadastro, avisos agendados e experimentos
    "tela_vista", "cadastro_etapa", "notificacao_agendada", "experimento_exposto",
]
EXP_FECHAR_DIA = "botao_fechar_dia"
COLUNAS_EVENTOS = [
    "id", "aluno_id", "tipo", "dia", "ocorrido_em", "recebido_em",
    "plataforma", "app_versao", "sessao_id", "props",
]
COLUNAS_PROFILES = ["id", "papel", "nascimento", "criado_em"]
COLUNAS_VINCULOS = ["id", "aluno_id", "profissional_id", "tipo", "status", "criado_em"]
COLUNAS_PROFISSIONAIS = ["id", "conselho", "uf", "status", "criado_em"]

# Refeições como nos modelos de dieta do app (src/consts.js): id = tipo,
# hora = minutos depois de acordar, principal ou opcional.
DIETAS = {
    "emagrecer": [("cafe", 20, True), ("lanche", 180, False), ("almoco", 330, True),
                  ("pre", 600, True), ("jantar", 870, True)],
    "massa": [("cafe", 20, True), ("lanche", 180, True), ("almoco", 330, True),
              ("pre", 600, True), ("jantar", 870, True), ("ceia", 960, False)],
    "saude": [("cafe", 20, True), ("lanche", 180, False), ("almoco", 330, True),
              ("pre", 600, True), ("jantar", 840, True)],
}
# Divisões de treino (dia da semana Python: 0 = segunda).
DIVISOES = [
    {0: "gluteos", 1: "costas", 2: "quadriceps", 3: "ombro_triceps", 4: "posterior"},
    {0: "superiores", 2: "inferiores", 4: "corpo_todo"},
    {0: "peito_triceps", 1: "costas_biceps", 3: "pernas", 4: "ombro_abdomen"},
    {0: "gluteos", 1: "superiores", 3: "quadriceps", 5: "corpo_todo"},
]
PROMESSAS = ["Seguir todas as refeições principais", "Fazer meu cardio",
             "Beber toda a água", "Treinar com tudo"]
PREMIOS = ["xp", "bone", "laco", "fone", "faixa", "coroa", "aureola", "academia", "praia", "por"]
UFS = ["MG"] * 8 + ["SP"] * 5 + ["RJ"] * 3 + ["BA", "PR", "RS", "DF"]

# Builds publicados (número do FF_BUILD) e a data em que ficaram disponíveis.
BUILDS = [(3, date(2026, 4, 20)), (4, date(2026, 5, 18)), (5, date(2026, 6, 22)),
          (6, date(2026, 7, 27)), (7, date(2026, 8, 31)), (8, date(2026, 9, 28)),
          (9, date(2026, 10, 19)), (10, date(2026, 10, 27))]


@dataclass(frozen=True)
class Perfil:
    nome: str
    peso: float           # fração dos alunos
    engaj: float          # chance de abrir o app num dia, na 1ª semana
    queda: float          # queda semanal da adesão (exponencial)
    fds: float            # multiplicador de engajamento no sábado e domingo
    fds_dieta: float      # multiplicador da adesão à dieta no fim de semana
    risco: float          # risco diário de abandonar de vez
    dieta: float          # adesão base à dieta
    treino: float         # chance de treinar num dia de treino
    cardio: float         # chance de fazer cardio num dia ativo


PERFIS = [
    Perfil("disciplinado", .20, .95, .010, .90, .92, .0015, .86, .86, .42),
    Perfil("fim_de_semana_fraco", .30, .88, .020, .40, .60, .0035, .76, .76, .28),
    Perfil("irregular", .30, .70, .025, .75, .85, .0050, .62, .58, .20),
    Perfil("desistente_precoce", .20, .85, .120, .70, .80, .0100, .66, .62, .18),
]
PERFIL = {p.nome: p for p in PERFIS}


@dataclass
class Config:
    alunos: int = 500
    profissionais: int = 20
    inicio: date = date(2026, 5, 1)
    fim: date = date(2026, 10, 31)
    seed: int = 20261102
    ultimo_cadastro_dias_antes_do_fim: int = 21  # todo aluno tem ao menos 3 semanas de histórico
    # Contrato v2 (telas, cadastro, avisos agendados, teste A/B) entra num build a partir desta data.
    inicio_v2: date = date(2026, 9, 1)
    # Parte de quem cria conta depois do v2 e desiste no meio do cadastro (vira só eventos de cadastro).
    desistem_no_cadastro: float = .18
    # Grupo B do teste: chance de quem toca em "Fechar o dia" completar o que faltava (H11).
    efeito_fechar_dia: float = .7


# ------------------------------------------------------------------ utilidades
def _uuid(rng: np.random.Generator) -> str:
    return str(uuid.UUID(bytes=rng.bytes(16), version=4))


def _props(**kw) -> str:
    kw["sintetico"] = True
    return json.dumps(kw, ensure_ascii=False, separators=(",", ":"))


def _local(d: date, minutos: float) -> datetime:
    """Data local + minutos depois da meia-noite, limitado ao mesmo dia."""
    minutos = min(max(minutos, 5.0), 24 * 60 - 1.0)
    return datetime(d.year, d.month, d.day, tzinfo=FUSO) + timedelta(minutes=float(minutos))


def _build(d: date, atraso_dias: int) -> int:
    ref = d - timedelta(days=atraso_dias)
    num = BUILDS[0][0]
    for n, quando in BUILDS:
        if quando <= ref:
            num = n
    return num


def _segunda(d: date) -> date:
    return d - timedelta(days=d.weekday())


# ---------------------------------------------------------------- simulação
@dataclass
class Aluno:
    id: str
    perfil: Perfil
    objetivo: str
    plataforma: str
    nascimento: date
    criado_em: datetime
    acorda_min: float
    treino_min: float
    divisao: dict
    peso_inicial: float
    atraso_update: int
    personal: str | None = None
    personal_fator: float = 1.0
    personal_status: str | None = None
    nutri: str | None = None
    nutri_status: str | None = None
    abandono: date | None = None
    grupo: str = "A"     # teste A/B botao_fechar_dia (só tem efeito no Android, que tem notificação)
    dias: list = field(default_factory=list)


def _sorteia_perfil(rng):
    return PERFIS[rng.choice(len(PERFIS), p=[p.peso for p in PERFIS])]


def gerar(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    """Gera todas as tabelas. Devolve {nome: DataFrame}."""
    cfg = cfg or Config()
    raiz = np.random.SeedSequence(cfg.seed)
    s_prof, s_alunos = raiz.spawn(2)
    rng = np.random.default_rng(s_prof)

    # ---- profissionais: ~70% personal (CREF), ~30% nutri (CRN)
    n_nutri = max(1, round(cfg.profissionais * .3))
    profs = []
    for i in range(cfg.profissionais):
        nutri = i < n_nutri
        criado = datetime(2026, 3, 1, tzinfo=FUSO) + timedelta(days=float(rng.uniform(0, 60)))
        profs.append({
            "id": _uuid(rng), "conselho": "CRN" if nutri else "CREF",
            "uf": UFS[rng.integers(len(UFS))], "status": "aprovado",
            "criado_em": criado, "nascimento": date(1975, 1, 1) + timedelta(days=int(rng.integers(0, 365 * 25))),
            "fator": float(np.clip(rng.normal(1.0, .10), .8, 1.2)),  # qualidade do acompanhamento
        })
    personais = [p for p in profs if p["conselho"] == "CREF"]
    nutris = [p for p in profs if p["conselho"] == "CRN"]

    def popular(lista):  # poucos profissionais concentram muitos alunos (cauda longa)
        w = 1 / np.arange(1, len(lista) + 1) ** .8
        return lista[rng.choice(len(lista), p=w / w.sum())]

    # ---- alunos
    dias_janela = (cfg.fim - cfg.inicio).days
    ultimo = dias_janela - cfg.ultimo_cadastro_dias_antes_do_fim
    w_cad = np.linspace(1, 2.2, ultimo + 1)  # app crescendo: mais cadastros com o tempo
    w_cad /= w_cad.sum()

    eventos: list[tuple] = []
    gabarito_dias: list[dict] = []
    alunos: list[Aluno] = []
    vinculos: list[dict] = []

    for seq in s_alunos.spawn(cfg.alunos):
        r = np.random.default_rng(seq)
        perfil = _sorteia_perfil(r)
        objetivo = r.choice(["emagrecer", "massa", "saude"], p=[.55, .25, .20]).item()
        dia_cad = int(r.choice(ultimo + 1, p=w_cad))
        hora_cad = float(r.uniform(7 * 60, 23 * 60))
        idade = float(np.clip(r.normal(31, 8), 18, 62))
        al = Aluno(
            id=_uuid(r), perfil=perfil, objetivo=objetivo,
            plataforma="android" if r.random() < .68 else "web",
            nascimento=cfg.fim - timedelta(days=int(idade * 365.25)),
            criado_em=_local(cfg.inicio + timedelta(days=dia_cad), hora_cad),
            acorda_min=float(np.clip(r.normal(6.4 * 60, 45), 4.5 * 60, 9 * 60)),
            treino_min=float(r.choice([6.5 * 60, 12.2 * 60, 18.5 * 60, 19.5 * 60], p=[.3, .1, .35, .25])),
            divisao=DIVISOES[r.integers(len(DIVISOES))],
            peso_inicial=round(float(np.clip(r.normal({"emagrecer": 82, "massa": 64, "saude": 70}[objetivo], 11), 45, 140)), 1),
            atraso_update=int(r.exponential(5)),
        )
        if r.random() < .60:
            p = popular(personais)
            al.personal, al.personal_fator = p["id"], p["fator"]
            al.personal_status = r.choice(["ativo", "pendente", "recusado"], p=[.93, .04, .03]).item()
        if r.random() < .35:
            al.nutri = popular(nutris)["id"]
            al.nutri_status = r.choice(["ativo", "pendente", "recusado"], p=[.93, .04, .03]).item()
        # sorteio próprio para o v2: não mexe na sequência aleatória do v1 (os números do v1 não mudam)
        r2 = np.random.default_rng(seq.spawn(1)[0])
        al.grupo = "B" if r2.random() < .5 else "A"
        _simula(al, cfg, r, eventos, gabarito_dias, r2)
        for prof_id, tipo, status in ((al.personal, "personal", al.personal_status), (al.nutri, "nutri", al.nutri_status)):
            if not prof_id:
                continue
            if status == "ativo" and al.abandono and r.random() < .5:
                status = "revogado"
            vinculos.append({"id": _uuid(r), "aluno_id": al.id, "profissional_id": prof_id, "tipo": tipo,
                             "status": status, "criado_em": al.criado_em + timedelta(days=float(r.exponential(4)))})
        alunos.append(al)

    # ---- quem desiste no cadastro (contrato v2): conta criada, só eventos de cadastro
    novos = sum(1 for a in alunos if a.criado_em.date() >= cfg.inicio_v2)
    n_desist = round(novos * cfg.desistem_no_cadastro / (1 - cfg.desistem_no_cadastro))
    ini_v2 = (cfg.inicio_v2 - cfg.inicio).days
    for seq in s_alunos.spawn(n_desist):
        r = np.random.default_rng(seq)
        dia_cad = int(r.integers(ini_v2, ultimo + 1))
        al = Aluno(id=_uuid(r), perfil=DESISTIU_NO_CADASTRO, objetivo="emagrecer",
                   plataforma="android" if r.random() < .68 else "web",
                   nascimento=cfg.fim - timedelta(days=int(float(np.clip(r.normal(31, 8), 18, 62)) * 365.25)),
                   criado_em=_local(cfg.inicio + timedelta(days=dia_cad), float(r.uniform(7 * 60, 23 * 60))),
                   acorda_min=0, treino_min=0, divisao={}, peso_inicial=0, atraso_update=int(r.exponential(5)))
        ate = int(r.choice(3, p=[.45, .35, .20]))  # para depois do login, do personagem ou das metas
        evs = _cadastro(r, al, ate_etapa=ate)
        _emite_dia(al, al.criado_em.astimezone(FUSO).date(), evs, r, eventos,
                   piso=al.criado_em.hour * 60 + al.criado_em.minute + 1, r2=r)
        al.abandono = al.criado_em.astimezone(FUSO).date()
        alunos.append(al)

    return _monta_tabelas(alunos, profs, vinculos, eventos, gabarito_dias)


DESISTIU_NO_CADASTRO = Perfil("desistiu_no_cadastro", 0, 0, 0, 0, 0, 0, 0, 0, 0)
ETAPAS_CADASTRO = ["codigo_pedido", "login_ok", "avatar_ok", "metas_ok", "dieta_ok", "lembretes_ok"]


def _cadastro(r: np.random.Generator, al: Aluno, ate_etapa: int | None = None, plano: bool = False) -> list:
    """Eventos do cadastro no primeiro dia (minutos pequenos: vêm antes de tudo e são ajustados ao horário
    do cadastro). `ate_etapa` = índice da última etapa em ETAPAS_CADASTRO depois do login (desistência)."""
    evs, m = [], .1
    def add(tipo, props):
        nonlocal m
        evs.append((m, tipo, props, True))
        m += float(r.exponential(1.2)) + .2
    add("cadastro_etapa", {"etapa": "codigo_pedido", "papel": "aluno", "novo": True})
    m += float(r.exponential(2))  # abrir o e-mail e copiar o código
    add("cadastro_etapa", {"etapa": "login_ok", "papel": "aluno", "novo": True})
    telas = ["cadastro_avatar", "cadastro_metas", "cadastro_dieta", "cadastro_lembretes"]
    seguintes = ["avatar_ok", "metas_ok", "dieta_ok", "lembretes_ok"]
    if plano:  # plano pronto do professor: pula metas e dieta
        telas, seguintes = ["cadastro_avatar", "cadastro_lembretes"], ["avatar_ok", "lembretes_ok"]
    for i, (tela, etapa) in enumerate(zip(telas, seguintes)):
        add("tela_vista", {"tela": tela, "origem": "abertura" if i == 0 else "botao"})
        if ate_etapa is not None and i > ate_etapa - 1:
            break
        add("cadastro_etapa", {"etapa": etapa, "papel": "aluno"})
        if plano and etapa == "avatar_ok":
            add("cadastro_etapa", {"etapa": "plano_do_professor", "papel": "aluno"})
    if ate_etapa is None:
        add("tela_vista", {"tela": "hoje", "origem": "botao"})
    return evs


def _simula(al: Aluno, cfg: Config, r: np.random.Generator, eventos: list, gab: list,
            r2: np.random.Generator | None = None) -> None:
    """Simula o aluno dia a dia, do cadastro até o fim da janela ou o abandono."""
    p = al.perfil
    refeicoes = DIETAS[al.objetivo]
    principais = [m for m in refeicoes if m[2]]
    precisa = max(1, math.ceil(len(principais) * .75))  # regra do dia cumprido (src/logic.js dayInfo)
    personal_ativo = al.personal_status == "ativo"
    fator_treino = al.personal_fator * 1.12 if personal_ativo else 1.0

    peso = al.peso_inicial
    alvo = {"emagrecer": -.060, "massa": .025, "saude": 0.0}[al.objetivo]
    adesao_recente = [p.dieta] * 7
    seq = 0              # sequência de dias cumpridos (com escudo)
    escudo_semana = None  # segunda-feira da semana em que o escudo foi usado
    desanimo = 0         # dias de desânimo depois de perder uma sequência longa
    ligado = True        # estado do perfil irregular (fases boas e ruins)

    d0 = al.criado_em.astimezone(FUSO).date()
    d = d0
    while d <= cfg.fim:
        t = (d - d0).days
        semanas = t / 7
        fds = d.weekday() >= 5

        # ---- abandono definitivo
        risco = p.risco
        if p.nome == "desistente_precoce":
            risco = .07 if 5 <= t <= 30 else (.004 if t < 5 else .02)
        if seq >= 7:
            risco *= .4          # H3: sequência longa protege contra o abandono
        if personal_ativo:
            risco *= .75
        if t > 2 and r.random() < risco:
            al.abandono = d
            break

        # ---- abriu o app hoje?
        if p.nome == "irregular":
            ligado = (r.random() > .14) if ligado else (r.random() < .22)
        engaj = p.engaj * math.exp(-p.queda * semanas) * (p.fds if fds else 1.0)
        engaj *= 1 + .035 * min(seq, 8)
        if desanimo:
            engaj *= .65
            desanimo -= 1
        if not ligado:
            engaj *= .12
        if t == 0:
            engaj = 1.0
        ativo = r.random() < min(engaj, .985)

        adesao = p.dieta * math.exp(-p.queda * .5 * semanas) * (p.fds_dieta if fds else 1.0)
        adesao = float(np.clip(adesao * (1 + .02 * min(seq, 8)), .05, .97))
        bons, treinou, cardio_min, copos = 0, False, 0, 0
        v2 = r2 is not None and d >= cfg.inicio_v2
        registradas = set()
        dia_treino = al.divisao.get(d.weekday())
        evs: list[tuple] = []  # (minuto_local, tipo, props)

        if ativo:
            acorda = al.acorda_min + r.normal(0, 25) + (70 if fds else 0)
            lote_noite = r.random() < .15  # registra tudo de uma vez à noite
            noite = float(np.clip(r.normal(21.7 * 60, 40), 20 * 60, 23.8 * 60))
            for ref_id, off, principal in refeicoes:
                hora = acorda + off
                p_reg = (.62 + .34 * adesao) if principal else .30 * adesao
                tocou = False
                if al.plataforma == "android" and principal and r.random() < .55:
                    if r.random() < .35:  # tocou na notificação
                        tocou = True
                        acao = "ok" if r.random() < .55 else "abrir"
                        hora_n = hora + r.uniform(0, 12)
                        evs.append((hora_n, "notificacao_tocada", {"tipo": "refeicao", "acao": acao}))
                        p_reg = min(.98, p_reg + .18)  # H8
                if r.random() >= p_reg:
                    continue
                u = r.random()
                status = "ok" if u < adesao else ("troca" if u < adesao + (1 - adesao) * .6 else "pulei")
                if tocou and acao == "ok":
                    status, quando = "ok", hora_n + r.uniform(.1, 1)
                elif lote_noite and r.random() < .8:
                    quando = noite + r.uniform(0, 6)
                else:
                    quando = hora + abs(r.normal(0, 22)) + (r.exponential(60) if r.random() < .12 else 0)
                evs.append((quando, "refeicao_registrada", {"refeicao_id": ref_id, "status": status}))
                registradas.add(ref_id)
                if r.random() < .025:  # errou e desfez, depois corrigiu
                    evs.append((quando + r.uniform(.2, 3), "refeicao_desfeita", {"refeicao_id": ref_id}))
                    status = r.choice(["ok", "troca", "pulei"]).item()
                    evs.append((quando + r.uniform(3, 6), "refeicao_registrada", {"refeicao_id": ref_id, "status": status}))
                if principal and status in ("ok", "troca") and (status == "ok" or r.random() < .7):
                    bons += 1

            # ---- treino (H4: mais forte que o cardio; H7: personal ajuda)
            if dia_treino:
                p_t = p.treino * math.exp(-p.queda * .7 * semanas) * fator_treino * (1 + .02 * min(seq, 8))
                if r.random() < min(p_t, .97):
                    treinou = True
                    quando = al.treino_min + r.normal(0, 30) + 55
                    evs.append((quando, "treino_feito", {"grupo": dia_treino}))
                    if v2 and r2.random() < .6:
                        evs.append((quando - .3, "tela_vista", {"tela": "treino", "origem": "aba"}, True))
                    if r.random() < .02:
                        evs.append((quando + r.uniform(.2, 2), "treino_desfeito", {}))
                        if r.random() < .5:
                            evs.append((quando + r.uniform(2, 5), "treino_feito", {"grupo": dia_treino}))
                        else:
                            treinou = False
            elif fds and r.random() < .04:
                evs.append((10 * 60 + r.normal(0, 60), "treino_feito", {"grupo": "extra"}))

            # ---- cardio
            p_c = p.cardio * math.exp(-p.queda * .9 * semanas) * (.7 if fds else 1)
            if r.random() < p_c:
                cardio_min = int(np.clip(round(r.normal(24, 9)), 5, 90))
                hora_c = (al.treino_min + 70) if treinou and r.random() < .6 else float(r.choice([6.8 * 60, 18 * 60, 20 * 60]))
                evs.append((hora_c + r.normal(0, 20), "cardio_registrado", {"minutos": cardio_min}))

            # ---- água: copos espalhados no dia, total acumulado
            n_copos = int(r.binomial(11, .45 + .35 * adesao))
            horas_a = np.sort(r.uniform(acorda + 15, 22.5 * 60, n_copos)).tolist()
            for i, hora_a in enumerate(horas_a):
                copos += 1
                evs.append((hora_a, "agua_copo", {"delta": 1, "total_dia": copos}))
                if r.random() < .04:  # tocou sem querer e tirou o copo
                    prox = horas_a[i + 1] if i + 1 < len(horas_a) else hora_a + 1
                    copos -= 1
                    evs.append((hora_a + min(.3, (prox - hora_a) / 2), "agua_copo", {"delta": -1, "total_dia": copos}))

            # ---- extras
            if r.random() < .12 + .05 * (p.nome == "disciplinado"):
                evs.append((r.uniform(15 * 60, 22 * 60), "doce_sos_vencido", {}))
            if r.random() < .18 * min(1, engaj):
                evs.append((acorda + r.uniform(5, 40), "promessa_feita", {"texto": PROMESSAS[r.integers(4)]}))
            if al.plataforma == "android" and dia_treino and r.random() < .3:
                evs.append((al.treino_min - r.uniform(0, 15), "notificacao_tocada",
                            {"tipo": "treino", "acao": "abrir"}))
            if al.plataforma == "android" and r.random() < .15:
                evs.append((r.uniform(10 * 60, 17 * 60), "notificacao_tocada",
                            {"tipo": r.choice(["agua", "lembrete"]).item(), "acao": "abrir"}))

        # ---- peso real (sempre evolui) e pesagem registrada
        adesao_hoje = (bons / len(principais)) if ativo else .35
        adesao_recente = adesao_recente[1:] + [adesao_hoje]
        a7 = sum(adesao_recente) / 7
        tendencia = alvo * a7 + (.02 * (1 - a7) if al.objetivo != "massa" else -.01 * (1 - a7))
        peso = peso + tendencia + r.normal(0, .03)
        if ativo:
            p_pesa = .55 if d.weekday() == 0 else .06
            if p.nome == "disciplinado":
                p_pesa += .1
            if r.random() < p_pesa:
                kg = round(peso + r.normal(0, .35), 1)
                evs.append((acorda + r.uniform(3, 25), "peso_registrado", {"kg": kg}))

        # ---- contrato v2: avisos agendados, exposição ao teste e o aviso das 21 h (H11)
        if v2 and ativo:
            if t == 0 and al.criado_em.date() >= cfg.inicio_v2:
                evs += _cadastro(r2, al, plano=personal_ativo and r2.random() < .3)
            if r2.random() < .1:
                evs.append((float(r2.uniform(12 * 60, 22 * 60)), "tela_vista", {"tela": "evolucao", "origem": "aba"}, True))
            if al.plataforma == "android":
                primeiro = min((e[0] for e in evs), default=acorda) - .2
                agend = [("refeicao", len(refeicoes)), ("agua", 6), ("lembrete", 1)] + ([("treino", 1)] if dia_treino else [])
                for tipo_n, qtd in agend:
                    evs.append((primeiro, "notificacao_agendada", {"tipo": tipo_n, "quantidade": qtd}, True))
                evs.append((primeiro, "experimento_exposto", {"experimento": EXP_FECHAR_DIA, "grupo": al.grupo}, True))
                completo_21h = bons >= precisa and (not dia_treino or treinou)
                if not completo_21h and r2.random() < .3:
                    acao = "fechar_dia" if al.grupo == "B" and r2.random() < .7 else "abrir"
                    hora = 21 * 60 + float(r2.uniform(0, 20))
                    evs.append((hora, "notificacao_tocada", {"tipo": "lembrete", "acao": acao}, True))
                    if r2.random() < (cfg.efeito_fechar_dia if acao == "fechar_dia" else .2):
                        for i, (ref_id, _, principal) in enumerate(m for m in refeicoes if m[2] and m[0] not in registradas):
                            evs.append((hora + 1 + i * .4, "refeicao_registrada", {"refeicao_id": ref_id, "status": "ok"}, True))
                            registradas.add(ref_id)
                            bons += 1
                        if acao == "fechar_dia" and dia_treino and not treinou and r2.random() < .3:
                            evs.append((hora + 3, "treino_feito", {"grupo": dia_treino}, True))  # "Marcar o treino"
                            treinou = True

        # ---- dia cumprido, sequência e escudo (como computeAll em src/logic.js)
        completo = ativo and bons >= precisa and (not dia_treino or treinou)
        escudo = False
        if completo:
            seq += 1
            if r.random() < .85:
                premio = "xp" if r.random() < .5 else PREMIOS[1 + r.integers(len(PREMIOS) - 1)]
                ultimo_reg = max((e[0] for e in evs), default=20 * 60)
                evs.append((ultimo_reg + r.uniform(.5, 4), "bau_aberto", {"premio": premio}))
        elif seq > 0 and escudo_semana != _segunda(d):
            escudo_semana, escudo = _segunda(d), True  # escudo segura a sequência
        else:
            if seq >= 5:
                desanimo = 3
            seq = 0

        if ativo:
            piso = (al.criado_em.hour * 60 + al.criado_em.minute + 1) if t == 0 else 0
            _emite_dia(al, d, evs, r, eventos, piso, r2 if v2 else None)
        gab.append({
            "aluno_id": al.id, "dia": d, "semana_desde_cadastro": t // 7, "ativo": ativo,
            "refeicoes_boas": bons, "refeicoes_principais": len(principais), "dia_de_treino": bool(dia_treino),
            "treinou": treinou, "cardio_min": cardio_min, "copos_agua": copos, "completo": completo,
            "escudo_usado": escudo, "sequencia": seq, "peso_real": round(peso, 2),
        })
        d += timedelta(days=1)


def _emite_dia(al: Aluno, d: date, evs: list, r: np.random.Generator, eventos: list, piso: float = 0,
               r2: np.random.Generator | None = None) -> None:
    """Ordena os eventos do dia, cria sessões (abre o app depois de 30 min parado) e grava.

    `piso` é o minuto do cadastro no primeiro dia: nada acontece antes de a conta existir.
    `r2` liga os eventos do contrato v2: eventos marcados como v2 (4º item da tupla) e a tela "hoje"
    de cada sessão usam esse sorteio próprio, para o v1 continuar igual.
    """
    if not evs:
        evs = [(r.uniform(8 * 60, 22 * 60), None, None)]  # só abriu e olhou
    evs = [e if len(e) == 4 else (*e, False) for e in evs]
    evs.sort(key=lambda e: e[0])
    if piso:
        janela = max(24 * 60 - 1 - (piso + 3), 1)
        ini, fim = evs[0][0], max(evs[-1][0], evs[0][0] + 1)
        escala = min(1.0, janela / (fim - ini))
        evs = [(piso + 3 + (m - ini) * escala, tp, pr, v) for m, tp, pr, v in evs]
    ultimo = -1e9
    sessao = None
    build = _build(d, al.atraso_update if al.plataforma == "android" else 0)
    versao = None if (al.plataforma == "web" and r.random() < .05) else str(build)
    for minuto, tipo, props, v2 in evs:
        if v2 and r2 is None:
            continue
        rr = r2 if v2 else r
        minuto = min(max(minuto, 5.0), 24 * 60 - 1.0)
        if minuto - ultimo > 30:
            sessao = _uuid(rr)
            abre = max(minuto - rr.uniform(.2, 2.5), 1.0)
            eventos.append(_linha(al, d, abre, "app_aberto", {}, sessao, versao, rr))
            if r2 is not None and tipo != "cadastro_etapa":
                origem = "notificacao" if tipo == "notificacao_tocada" else "abertura"
                eventos.append(_linha(al, d, abre + .05, "tela_vista", {"tela": "hoje", "origem": origem},
                                      sessao, versao, r2))
        if tipo:
            eventos.append(_linha(al, d, minuto, tipo, props, sessao, versao, rr))
        ultimo = minuto


def _linha(al, d, minuto, tipo, props, sessao, versao, r):
    ocorrido = _local(d, minuto).astimezone(timezone.utc)
    if al.plataforma == "android" and r.random() < .07:
        atraso = timedelta(minutes=float(min(r.exponential(120), 36 * 60)))  # fila offline
    else:
        atraso = timedelta(seconds=float(r.uniform(.2, 3)))
    return (_uuid(r), al.id, tipo, d, ocorrido, ocorrido + atraso, al.plataforma, versao, sessao, _props(**props))


def _monta_tabelas(alunos, profs, vinculos, eventos, gabarito_dias) -> dict[str, pd.DataFrame]:
    utc = lambda s: pd.to_datetime(s, utc=True)

    ev = pd.DataFrame(eventos, columns=COLUNAS_EVENTOS)
    ev["dia"] = pd.to_datetime(ev["dia"]).dt.date
    ev["ocorrido_em"] = utc(ev["ocorrido_em"])
    ev["recebido_em"] = utc(ev["recebido_em"])
    ev = ev.sort_values(["ocorrido_em", "aluno_id"], kind="stable").reset_index(drop=True)

    profiles = pd.DataFrame(
        [{"id": a.id, "papel": "aluno", "nascimento": a.nascimento, "criado_em": a.criado_em} for a in alunos]
        + [{"id": p["id"], "papel": "personal" if p["conselho"] == "CREF" else "nutri",
            "nascimento": p["nascimento"], "criado_em": p["criado_em"]} for p in profs],
        columns=COLUNAS_PROFILES)
    profiles["criado_em"] = utc(profiles["criado_em"])

    profissionais = pd.DataFrame(profs)[COLUNAS_PROFISSIONAIS]
    profissionais["criado_em"] = utc(profissionais["criado_em"])

    vinc = pd.DataFrame(vinculos, columns=COLUNAS_VINCULOS)
    vinc["criado_em"] = utc(vinc["criado_em"])

    gab_alunos = pd.DataFrame([{
        "aluno_id": a.id, "perfil": a.perfil.nome, "objetivo": a.objetivo, "plataforma": a.plataforma,
        "peso_inicial": a.peso_inicial, "tem_personal_ativo": a.personal_status == "ativo",
        "tem_nutri_ativo": a.nutri_status == "ativo", "divisao_treino": ",".join(a.divisao[k] for k in sorted(a.divisao)),
        "abandonou_em": a.abandono, "grupo_fechar_dia": a.grupo,
    } for a in alunos])
    gab_dias = pd.DataFrame(gabarito_dias)

    return {"eventos": ev, "profiles": profiles, "vinculos": vinc, "profissionais": profissionais,
            "gabarito_alunos": gab_alunos, "gabarito_dias": gab_dias}


GABARITO = ("gabarito_alunos", "gabarito_dias")


def salvar(tabelas: dict[str, pd.DataFrame], pasta, formatos=("csv", "parquet")) -> list:
    """Grava um arquivo por tabela. O gabarito vai numa subpasta separada."""
    from pathlib import Path
    pasta = Path(pasta)
    feitos = []
    for nome, df in tabelas.items():
        destino = pasta / "gabarito" if nome in GABARITO else pasta
        destino.mkdir(parents=True, exist_ok=True)
        if "csv" in formatos:
            out = df.copy()
            for c in out.columns:
                if isinstance(out[c].dtype, pd.DatetimeTZDtype):
                    out[c] = out[c].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            f = destino / f"{nome}.csv"
            out.to_csv(f, index=False)
            feitos.append(f)
        if "parquet" in formatos:
            f = destino / f"{nome}.parquet"
            df.to_parquet(f, index=False)
            feitos.append(f)
    return feitos
