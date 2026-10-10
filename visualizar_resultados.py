"""Genera gráficos PNG a partir de las tablas CSV producidas por el batch."""

import glob
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def read_spark_csv(folder: Path) -> pd.DataFrame:
    files = glob.glob(str(folder / "part-*.csv"))
    if not files:
        raise FileNotFoundError(f"No hay archivos part-*.csv en {folder}")
    return pd.concat((pd.read_csv(file) for file in files), ignore_index=True)


def bar_chart(frame, category, title, output, limit=15):
    frame["count"] = pd.to_numeric(frame["count"], errors="coerce")
    frame = frame.dropna(subset=["count"]).nlargest(limit, "count").sort_values("count")
    plt.figure(figsize=(10, 6))
    plt.barh(frame[category].astype(str), frame["count"], color="#2f75b5")
    plt.title(title)
    plt.xlabel("Número de siniestros")
    plt.ylabel(category.replace("_", " ").title())
    plt.tight_layout()
    plt.savefig(output, dpi=160)
    plt.close()


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "data/resultados_siniestros")
    charts = root / "graficos"
    charts.mkdir(parents=True, exist_ok=True)

    by_year = read_spark_csv(root / "por_anio")
    by_year["year"] = pd.to_numeric(by_year["year"], errors="coerce")
    by_year["count"] = pd.to_numeric(by_year["count"], errors="coerce")
    by_year = by_year.dropna(subset=["year", "count"]).sort_values("year")
    plt.figure(figsize=(10, 5))
    plt.plot(by_year["year"], by_year["count"], marker="o", color="#2f75b5")
    plt.title("Siniestros viales por año")
    plt.xlabel("Año")
    plt.ylabel("Número de siniestros")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(charts / "siniestros_por_anio.png", dpi=160)
    plt.close()

    for folder, field, title, filename in (
        ("por_gravedad", "severity", "Siniestros por gravedad", "siniestros_por_gravedad.png"),
        ("por_clase", "accident_class", "Siniestros por clase", "siniestros_por_clase.png"),
        ("por_localidad", "locality", "Siniestros por localidad", "siniestros_por_localidad.png"),
    ):
        data = read_spark_csv(root / folder)
        bar_chart(data, field, title, charts / filename)

    print(f"Gráficos guardados en {charts.resolve()}")


if __name__ == "__main__":
    main()
