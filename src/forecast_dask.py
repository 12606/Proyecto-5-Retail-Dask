"""Dos estrategias Dask: delayed por lotes y dataframe.map_partitions."""
import time
import pandas as pd
import dask.dataframe as dd
import dask
from dask import delayed
from dask.distributed import Client, LocalCluster, wait
from baseline_serial import pronosticar


def lote_pronosticos(lote, horizonte):
    return [pronosticar(item, horizonte) for item in lote]


def predecir_particion(df, horizonte):
    """Cada fila contiene una serie completa; una partición procesa varias series."""
    rows = []
    for row in df.itertuples(index=False):
        uid, valores = pronosticar((row.unique_id, row.valores), horizonte)
        rows.extend((uid, h, float(y)) for h, y in enumerate(valores))
    return pd.DataFrame(rows, columns=['unique_id', 'h', 'pred'])


def ejecutar(series, horizonte=14, tamano_lote=50, workers=4, hilos=2):
    """Retorna resultados delayed, dataframe y tiempos medidos.

    overhead_control_s mide un control de tareas triviales (proxy), NO la
    comunicación pura del scheduler. La resta entre tiempos tampoco la aísla.
    """
    lotes = [series[i:i+tamano_lote] for i in range(0, len(series), tamano_lote)]
    with LocalCluster(n_workers=workers, threads_per_worker=hilos,
                      processes=False, dashboard_address=None) as cluster:
        with Client(cluster) as client:
            # Warm-up del scheduler, excluido de las mediciones.
            client.submit(lambda: 1).result()
            inicio = time.perf_counter()
            control = client.compute([delayed(len)(lote) for lote in lotes])
            wait(control)
            _ = client.gather(control)
            overhead_control = time.perf_counter()-inicio

            inicio = time.perf_counter()
            tareas = [delayed(lote_pronosticos)(lote, horizonte) for lote in lotes]
            futuros = client.compute(tareas)
            resultado_lotes = client.gather(futuros)
            tiempo_delayed = time.perf_counter()-inicio
            resultado_delayed = [x for lote in resultado_lotes for x in lote]

            # Una fila = una serie; particiones ~ lotes de 50-100 series.
            frame = pd.DataFrame({'unique_id':[uid for uid,_ in series],
                                  'valores':[y for _,y in series]})
            # Dask convierte columnas object a string automáticamente si
            # dataframe.convert-string está activo. Eso rompe arrays NumPy.
            with dask.config.set({"dataframe.convert-string": False}):
                ddf = dd.from_pandas(frame, npartitions=len(lotes), sort=False)
            meta = pd.DataFrame({'unique_id':pd.Series(dtype='str'),
                                 'h':pd.Series(dtype='int64'),
                                 'pred':pd.Series(dtype='float64')})
            inicio = time.perf_counter()
            pred_df = ddf.map_partitions(predecir_particion, horizonte, meta=meta).compute()
            tiempo_dataframe = time.perf_counter()-inicio
    return resultado_delayed, pred_df, {
        'overhead_control_s':overhead_control,
        'dask_delayed_s':tiempo_delayed,
        'dask_dataframe_s':tiempo_dataframe,
        'lotes':len(lotes), 'workers':workers, 'hilos_por_worker':hilos,
    }
