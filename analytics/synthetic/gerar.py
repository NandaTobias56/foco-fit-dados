"""Linha de comando do gerador sintético. Veja README.md."""
import argparse
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gerador import Config, gerar, salvar  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description="Gera dados sintéticos do Foco Fit (contrato de eventos v1 e v2).")
    ap.add_argument("--alunos", type=int, default=Config.alunos)
    ap.add_argument("--profissionais", type=int, default=Config.profissionais)
    ap.add_argument("--inicio", type=date.fromisoformat, default=Config.inicio)
    ap.add_argument("--fim", type=date.fromisoformat, default=Config.fim)
    ap.add_argument("--seed", type=int, default=Config.seed)
    ap.add_argument("--saida", default=str(Path(__file__).resolve().parent / "saida"))
    ap.add_argument("--formatos", default="csv,parquet", help="csv, parquet ou os dois separados por vírgula")
    a = ap.parse_args(argv)

    t0 = time.time()
    cfg = Config(alunos=a.alunos, profissionais=a.profissionais, inicio=a.inicio, fim=a.fim, seed=a.seed)
    tabelas = gerar(cfg)
    arquivos = salvar(tabelas, a.saida, tuple(a.formatos.split(",")))
    for nome, df in tabelas.items():
        print(f"{nome:16s} {len(df):>9,d} linhas")
    print(f"{len(arquivos)} arquivos em {a.saida} ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
