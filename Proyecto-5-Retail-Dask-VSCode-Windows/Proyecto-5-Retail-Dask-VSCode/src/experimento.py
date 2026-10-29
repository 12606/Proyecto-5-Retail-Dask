"""Orquestación común para benchmark y optimización sin duplicar lógica."""
import time
from pathlib import Path

import pandas as pd

from src.preprocess import generar, preparar
from src.baseline_serial import ejecutar as ejecutar_serial
from src.evaluacion import a_tabla, evaluar, comprobar_equivalencia, tabla_desde_dataframe


def datos(root, series, dias, horizonte, seed=2026):
    ruta = Path(root)/'data'/'demanda_sintetica.parquet'
    print(f'Generando {series:,} series sintéticas... ', flush=True)
    inicio = time.perf_counter()
    generar(ruta, n_series=series, n_dias=dias, horizonte=horizonte, seed=seed)
    entrenamientos, reales, estadisticas = preparar(ruta, horizonte)
    t_prepro = time.perf_counter() - inicio
    estadisticas.to_csv(Path(root)/'data'/'estadisticas_series.csv', index=False)
    print(f'Preprocesamiento: {t_prepro:.2f} s | series: {len(entrenamientos):,}', flush=True)
    return entrenamientos, reales, t_prepro


def referencia_serial(series, horizonte, dias, reales):
    print('Entrenamiento y predicción serial con AutoARIMA...', flush=True)
    inicio = time.perf_counter()
    pred = ejecutar_serial(series, horizonte)
    tiempo = time.perf_counter()-inicio
    df = a_tabla(pred, dias)
    mae, rmse = evaluar(df, reales)
    print(f'Serial: {tiempo:.2f} s | MAE={mae:.4f} | RMSE={rmse:.4f}', flush=True)
    return tiempo, df, mae, rmse


def comparar(pred_delayed, pred_dataframe, dias, reales, referencia, tiempo_serial, tiempos):
    ddf = tabla_desde_dataframe(pred_dataframe, dias)
    delayed = a_tabla(pred_delayed, dias)
    comprobar_equivalencia(referencia, delayed)
    comprobar_equivalencia(referencia, ddf)
    filas = []
    for nombre, data, duracion in [
        ('Dask delayed', delayed, tiempos['dask_delayed_s']),
        ('Dask DataFrame', ddf, tiempos['dask_dataframe_s']),
    ]:
        mae, rmse = evaluar(data, reales)
        filas.append({'metodo': nombre, 'segundos': duracion,
                      'speedup': tiempo_serial/duracion, 'MAE': mae, 'RMSE': rmse})
    return pd.DataFrame(filas), ddf


def fila_serial(tiempo, mae, rmse):
    return pd.DataFrame([{'metodo': 'Serial', 'segundos': tiempo,
                          'speedup': 1.0, 'MAE': mae, 'RMSE': rmse}])
