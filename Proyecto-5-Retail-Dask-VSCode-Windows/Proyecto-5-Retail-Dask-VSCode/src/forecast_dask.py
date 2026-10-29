"""Pronósticos en paralelo: dask.delayed por lotes y dask.dataframe.map_partitions."""
import math
import time

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask import delayed
from dask.distributed import Client, LocalCluster, wait

from src.baseline_serial import pronosticar


def pronosticar_lote(lote, horizonte):
    return [pronosticar(item, horizonte) for item in lote]


def predecir_particion(particion, horizonte):
    """Una fila por serie, salida con una fila por paso del pronóstico."""
    filas = []
    for uid, valores in particion[['unique_id', 'valores']].itertuples(index=False, name=None):
        # Los arreglos se mantienen como object; cast explícito para StatsForecast.
        if isinstance(valores, str):
            raise TypeError('Una serie se convirtió a texto en Dask DataFrame')
        uid, pred = pronosticar((uid, np.asarray(valores, dtype=np.float64)), horizonte)
        filas.extend((uid, h, float(v)) for h, v in enumerate(pred))
    return pd.DataFrame(filas, columns=['unique_id', 'h', 'pred'])


def tarea_control(n):
    """Tarea trivial para estimar (no aislar) costo del scheduler."""
    return n


def medir_control(client, lotes):
    inicio = time.perf_counter()
    futuros = client.compute([delayed(tarea_control)(len(l)) for l in lotes])
    wait(futuros)
    client.gather(futuros)
    return time.perf_counter() - inicio


def ejecutar_con_cliente(client, series, horizonte=14, tamano_lote=50):
    """Mide ambos métodos con clúster activo, sin incluir arranque del clúster."""
    if tamano_lote < 1:
        raise ValueError('El lote debe ser positivo')
    lotes = [series[i:i+tamano_lote] for i in range(0, len(series), tamano_lote)]
    # Warming up del scheduler; fuera de tiempos de entrenamiento.
    client.submit(tarea_control, 1, pure=False).result()
    control = medir_control(client, lotes)

    inicio = time.perf_counter()
    tareas = [delayed(pronosticar_lote)(lote, horizonte) for lote in lotes]
    futuros = client.compute(tareas)
    resultados = client.gather(futuros)
    tiempo_delayed = time.perf_counter() - inicio
    pred_delayed = [serie for lote in resultados for serie in lote]

    # Evitar dataframe.convert-string=True: convertiría ndarray a str y rompería AutoARIMA.
    frame = pd.DataFrame({
        'unique_id': [uid for uid, _ in series],
        'valores': pd.Series([v for _, v in series], dtype=object),
    })
    meta = pd.DataFrame({
        'unique_id': pd.Series(dtype='object'),
        'h': pd.Series(dtype='int64'),
        'pred': pd.Series(dtype='float64'),
    })
    with dask.config.set({'dataframe.convert-string': False}):
        ddf = dd.from_pandas(frame, npartitions=math.ceil(len(series)/tamano_lote), sort=False)
        inicio = time.perf_counter()
        pred_df = ddf.map_partitions(predecir_particion, horizonte, meta=meta).compute(scheduler=client)
        tiempo_dataframe = time.perf_counter() - inicio

    if len(pred_df) != len(series)*horizonte:
        raise ValueError('Cantidad de predicciones incompleta en Dask DataFrame')
    return pred_delayed, pred_df, {
        'dask_delayed_s': tiempo_delayed,
        'dask_dataframe_s': tiempo_dataframe,
        'overhead_control_s': control,
        'numero_lotes': len(lotes),
    }


def ejecutar(series, horizonte=14, tamano_lote=50, workers=4, hilos=1):
    """Windows: usar procesos reales. Llamar bajo if __name__ == '__main__'."""
    inicio = time.perf_counter()
    # threads_per_worker=1 es la opción inicial para cálculos de CPU.
    with LocalCluster(n_workers=workers, threads_per_worker=hilos,
                      processes=True, dashboard_address=None) as cluster:
        with Client(cluster) as client:
            arranque = time.perf_counter()-inicio
            resultados, pred_df, tiempos = ejecutar_con_cliente(
                client, series, horizonte, tamano_lote)
    tiempos.update({
        'arranque_cluster_s': arranque,
        'workers': workers,
        'hilos_por_worker': hilos,
        'tamano_lote': tamano_lote,
    })
    return resultados, pred_df, tiempos
