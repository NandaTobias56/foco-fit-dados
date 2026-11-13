import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gerador import Config, gerar  # noqa: E402

# Amostra menor que a padrão (500) para os testes rodarem rápido, grande o
# bastante para as hipóteses aparecerem com folga.
CFG = Config(alunos=220, seed=20261102)


@pytest.fixture(scope="session")
def tabelas():
    return gerar(CFG)


@pytest.fixture(scope="session")
def ev(tabelas):
    e = tabelas["eventos"].copy()
    e["hora_local"] = e["ocorrido_em"].dt.tz_convert("America/Sao_Paulo")
    return e


@pytest.fixture(scope="session")
def dias(tabelas):
    g = tabelas["gabarito_dias"].merge(tabelas["gabarito_alunos"], on="aluno_id")
    g["fds"] = [d.weekday() >= 5 for d in g["dia"]]
    return g
