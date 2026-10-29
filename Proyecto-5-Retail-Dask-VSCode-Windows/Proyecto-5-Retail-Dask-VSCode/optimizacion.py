"""Experimentos de configuración: baseline serial UNA vez, Dask para cada perfil."""
import os
for variable in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ[variable] = '1'

import argparse
import multiprocessing
from pathlib import Path
import time

import pandas as pd
from dask.distributed import Client, LocalCluster

from src.experimento import datos, referencia_serial, comparar, fila_serial
from src.forecast_dask import ejecutar_con_cliente
from src.evaluacion import guardar_grafica_optimizacion

ROOT = Path(__file__).resolve().parent
CONFIGURACIONES = [
    ('A', 2, 1, 50),
    ('B', 2, 2, 100),
    ('C', 4, 1, 50),
    ('D', 4, 2, 100),
]


def ejecutar(args):
    series, reales, prep = datos(ROOT, args.series, args.dias, args.horizonte, args.seed)
    tiempo_serial, base, mae, rmse = referencia_serial(series, args.horizonte, args.dias, reales)
    tablas, detalles = [], []
    for nombre, workers, hilos, lote in CONFIGURACIONES:
        print(f'\nConfiguración {nombre}: workers={workers}, hilos={hilos}, lote={lote}', flush=True)
        inicio = time.perf_counter()
        with LocalCluster(n_workers=workers, threads_per_worker=hilos,
                          processes=True, dashboard_address=None) as cluster:
            with Client(cluster) as client:
                arranque = time.perf_counter() - inicio
                pred_lotes, pred_frame, tiempos = ejecutar_con_cliente(
                    client, series, args.horizonte, lote)
        filas, _ = comparar(pred_lotes, pred_frame, args.dias, reales,
                           base, tiempo_serial, tiempos)
        filas.insert(0, 'configuracion', nombre)
        filas.insert(1, 'workers', workers)
        filas.insert(2, 'hilos', hilos)
        filas.insert(3, 'lote', lote)
        tablas.append(filas)
        detalles.append({'configuracion': nombre, 'workers': workers, 'hilos': hilos,
                         'lote': lote, 'arranque_cluster_s': arranque,
                         'preprocesamiento_s': prep, **tiempos})
        print(filas[['metodo','segundos','speedup','MAE','RMSE']].to_string(index=False), flush=True)

    resultado = pd.concat(tablas, ignore_index=True)
    carpeta = ROOT/'benchmarks'
    carpeta.mkdir(exist_ok=True)
    resultado.to_csv(carpeta/'optimizacion.csv', index=False)
    pd.DataFrame(detalles).to_csv(carpeta/'optimizacion_overhead.csv', index=False)
    guardar_grafica_optimizacion(resultado, carpeta/'graficas')
    mejor = resultado.loc[resultado['segundos'].idxmin()]
    print('\n=== MEJOR CONFIGURACIÓN PARA ESTA EJECUCIÓN ===')
    print(f'{mejor["configuracion"]} | {mejor["metodo"]} | '
          f'{mejor["segundos"]:.3f} s | speedup={mejor["speedup"]:.3f}')
    print(f'Baseline serial (entrenado una vez): {tiempo_serial:.3f} s')
    print(f'Resultados disponibles en {carpeta.resolve()}')
    print('Si el speedup < 1, Dask fue más lento: es un resultado válido.')


def parser():
    p = argparse.ArgumentParser(description='Optimizar Dask en CPU con 4 perfiles')
    p.add_argument('--series', type=int, default=100)
    p.add_argument('--dias', type=int, default=120)
    p.add_argument('--horizonte', type=int, default=14)
    p.add_argument('--seed', type=int, default=2026)
    return p


if __name__ == '__main__':
    multiprocessing.freeze_support()
    args = parser().parse_args()
    if args.series < 1 or args.horizonte < 1 or args.dias < 30:
        parser().error('series y horizonte > 0; dias >= 30')
    ejecutar(args)
