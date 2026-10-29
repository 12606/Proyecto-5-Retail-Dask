"""Benchmark completo: 1 baseline serial + 2 estrategias Dask."""
import os
# Limitar paralelismo numérico anidado (multiproceso Dask controla el paralelismo).
for variable in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'):
    os.environ[variable] = '1'

import argparse
import multiprocessing
from pathlib import Path

import pandas as pd
from src.experimento import datos, referencia_serial, comparar, fila_serial
from src.forecast_dask import ejecutar as ejecutar_paralelo
from src.evaluacion import guardar_graficas

ROOT = Path(__file__).resolve().parent


def ejecutar(args):
    series, reales, prep = datos(ROOT, args.series, args.dias, args.horizonte, args.seed)
    t_serial, base, mae, rmse = referencia_serial(series, args.horizonte, args.dias, reales)
    print(f'Dask: {args.workers} workers × {args.hilos} hilo(s), lotes de {args.lote}', flush=True)
    delayed, dataframe, tiempos = ejecutar_paralelo(
        series, args.horizonte, args.lote, args.workers, args.hilos)
    otras, pron = comparar(delayed, dataframe, args.dias, reales, base, t_serial, tiempos)
    tabla = pd.concat([fila_serial(t_serial, mae, rmse), otras], ignore_index=True)

    carpeta = ROOT/'benchmarks'
    carpeta.mkdir(exist_ok=True)
    tabla.to_csv(carpeta/'comparacion.csv', index=False)
    pd.DataFrame([{'series': args.series, 'dias_entrenamiento': args.dias,
                   'horizonte': args.horizonte, 'preprocesamiento_s': prep, **tiempos}]).to_csv(
        carpeta/'overhead.csv', index=False)
    pron.to_csv(carpeta/'pronosticos.csv', index=False)
    reales.to_csv(carpeta/'valores_reales.csv', index=False)
    guardar_graficas(tabla, pron, reales, carpeta/'graficas')

    print('\n=== RESULTADOS REALES ===')
    print(tabla.to_string(index=False, float_format=lambda x: f'{x:.4f}'))
    print(f'Arranque del clúster (separado): {tiempos["arranque_cluster_s"]:.4f} s')
    print(f'Tareas de control (proxy overhead): {tiempos["overhead_control_s"]:.4f} s')
    print('Nota: el overhead de control NO mide comunicación aislada.')
    print(f'CSV y gráficas: {carpeta.resolve()}')
    print('Benchmark terminado correctamente.')


def parser():
    p = argparse.ArgumentParser(description='Pronóstico de demanda en paralelo con Dask (CPU)')
    p.add_argument('--series', type=int, default=100, help='100 prueba rápida; 1000 prueba final')
    p.add_argument('--dias', type=int, default=120)
    p.add_argument('--horizonte', type=int, default=14)
    p.add_argument('--lote', type=int, default=50)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--hilos', type=int, default=1)
    p.add_argument('--seed', type=int, default=2026)
    return p


if __name__ == '__main__':
    multiprocessing.freeze_support()
    argumentos = parser().parse_args()
    if min(argumentos.series, argumentos.horizonte, argumentos.lote,
           argumentos.workers, argumentos.hilos) < 1 or argumentos.dias < 30:
        parser().error('series, horizonte, lote, workers e hilos > 0; dias >= 30')
    ejecutar(argumentos)
