"""Métricas, equivalencia de modelos, archivos de resultado y gráficas."""
from pathlib import Path
import numpy as np
import pandas as pd

from src.preprocess import INICIO


def a_tabla(pronosticos, n_dias):
    filas = [(uid, INICIO + pd.Timedelta(days=n_dias + paso), float(valor))
             for uid, valores in pronosticos for paso, valor in enumerate(valores)]
    return pd.DataFrame(filas, columns=['unique_id', 'ds', 'pred'])


def tabla_desde_dataframe(ddf_resultado, n_dias):
    pred = ddf_resultado.copy()
    pred['ds'] = INICIO + pd.to_timedelta(n_dias + pred['h'], unit='D')
    return pred[['unique_id', 'ds', 'pred']].copy()


def fusionar(pred, reales):
    merged = reales.merge(pred, on=['unique_id', 'ds'], how='left', validate='one_to_one')
    if len(merged) != len(reales) or merged['pred'].isna().any():
        raise ValueError('Predicciones incompletas o fechas desalineadas')
    if not np.isfinite(merged['pred'].to_numpy(dtype=float)).all():
        raise ValueError('Predicciones no finitas')
    return merged


def evaluar(pred, reales):
    merged = fusionar(pred, reales)
    errores = merged['actual'].to_numpy(dtype=float) - merged['pred'].to_numpy(dtype=float)
    return float(np.mean(np.abs(errores))), float(np.sqrt(np.mean(errores**2)))


def comprobar_equivalencia(referencia, candidata):
    a = referencia.sort_values(['unique_id','ds']).reset_index(drop=True)
    b = candidata.sort_values(['unique_id','ds']).reset_index(drop=True)
    if len(a) != len(b) or not a[['unique_id','ds']].equals(b[['unique_id','ds']]):
        raise ValueError('Los métodos no generaron las mismas claves y fechas')
    if not np.allclose(a['pred'], b['pred'], atol=1e-4, rtol=1e-5):
        diferencia = np.max(np.abs(a['pred'].to_numpy() - b['pred'].to_numpy()))
        raise ValueError(f'Los métodos difieren (máx={diferencia:.5f})')


def guardar_graficas(tabla, pronosticos, reales, carpeta):
    """Guardar PNG sin necesidad de abrir una ventana o tener GPU."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(tabla['metodo'], tabla['segundos'])
    ax.set(ylabel='Segundos (menor es mejor)', title='Tiempo de entrenamiento y predicción')
    ax.tick_params(axis='x', rotation=8)
    fig.tight_layout()
    fig.savefig(carpeta/'tiempos.png', dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(tabla['metodo'], tabla['speedup'])
    ax.axhline(1, linestyle='--', color='black', linewidth=1, label='Sin aceleración')
    ax.set(ylabel='Speedup (mayor es mejor)', title='Aceleración respecto al método serial')
    ax.legend()
    fig.tight_layout()
    fig.savefig(carpeta/'speedup.png', dpi=150)
    plt.close(fig)

    uid = reales['unique_id'].iloc[0]
    real = reales[reales['unique_id'] == uid].sort_values('ds')
    pron = pronosticos[pronosticos['unique_id'] == uid].sort_values('ds')
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.plot(real['ds'], real['actual'], label='Demanda observada', marker='o')
    ax.plot(pron['ds'], pron['pred'], label='AutoARIMA', marker='x')
    ax.set(title=f'Pronóstico SKU–tienda: {uid}', xlabel='Fecha', ylabel='Demanda')
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(carpeta/'pronostico_ejemplo.png', dpi=150)
    plt.close(fig)


def guardar_grafica_optimizacion(tabla, carpeta):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    for metodo, grupo in tabla.groupby('metodo'):
        ax.plot(grupo['configuracion'], grupo['segundos'], marker='o', label=metodo)
    ax.set(xlabel='Configuración', ylabel='Segundos', title='Optimización de workers e hilos')
    ax.legend()
    fig.tight_layout()
    fig.savefig(carpeta/'optimizacion_tiempos.png', dpi=150)
    plt.close(fig)
