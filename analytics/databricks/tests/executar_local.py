"""Roda os notebooks do lakehouse fora do Databricks, com PySpark local.

Os notebooks estão no formato "Databricks source" (.py). Este executor separa as células por
`# COMMAND ----------`, ignora as de markdown, resolve `%run ./00_config` e executa o resto num
mesmo namespace com `spark`, `dbutils` e `display` simulados. As tabelas viram parquet num
warehouse temporário e os schemas ficam sem catálogo (sem Unity Catalog localmente).
"""
from pathlib import Path

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
SEPARADOR = "# COMMAND ----------"


class _Widgets:
    def __init__(self, valores):
        self._v = dict(valores)

    def text(self, nome, padrao, rotulo=None):
        self._v.setdefault(nome, padrao)

    def dropdown(self, nome, padrao, opcoes, rotulo=None):
        self._v.setdefault(nome, padrao)

    def get(self, nome):
        return self._v[nome]


class _Secrets:
    def __init__(self, segredos):
        self._s = segredos

    def get(self, scope, chave):
        if (scope, chave) not in self._s:
            raise KeyError(f"{scope}/{chave}")
        return self._s[(scope, chave)]


class _Fs:
    def mkdirs(self, caminho):
        Path(caminho).mkdir(parents=True, exist_ok=True)


class DbutilsLocal:
    def __init__(self, widgets=None, segredos=None):
        self.widgets = _Widgets(widgets or {})
        self.secrets = _Secrets(segredos or {})
        self.fs = _Fs()


def _celulas(nome):
    texto = (NOTEBOOKS / nome).read_text(encoding="utf-8")
    for celula in texto.split(SEPARADOR):
        linhas = [l for l in celula.splitlines() if l.strip() != "# Databricks notebook source"]
        magicas = [l for l in linhas if l.startswith("# MAGIC")]
        if magicas:
            comando = magicas[0][len("# MAGIC"):].strip()
            if comando.startswith("%run"):
                yield ("run", comando.split()[1].removeprefix("./") + ".py")
            continue  # markdown
        yield ("python", "\n".join(linhas))


def executar(nome, spark, volume, widgets=None, segredos=None):
    """Executa um notebook e devolve o namespace final."""
    ns = {
        "spark": spark,
        "dbutils": DbutilsLocal({"catalogo": "", **(widgets or {})}, segredos),
        "display": lambda df: df.show(20, truncate=False),
        "FORMATO_TABELA": "parquet",
        "VOLUME_LOCAL": str(volume),
        "__name__": "__notebook__",
    }

    def rodar(arquivo):
        for tipo, conteudo in _celulas(arquivo):
            if tipo == "run":
                rodar(conteudo)
            elif conteudo.strip():
                exec(compile(conteudo, f"{arquivo}", "exec"), ns)

    rodar(nome)
    return ns


PIPELINE = ["01_bronze.py", "02_prata.py", "03_ouro.py", "04_kpis.py", "05_qualidade.py", "06_jornada_funil.py"]


def executar_pipeline(spark, volume, widgets=None, segredos=None):
    for nb in PIPELINE:
        executar(nb, spark, volume, widgets, segredos)
