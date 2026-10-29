"""Generación y preparación reproducible de series SKU–tienda con Dask DataFrame."""
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd

INICIO = pd.Timestamp('2025-01-01')


def generar(ruta, n_series=1000, n_dias=120, horizonte=14, seed=2026, tiendas=10):
    """Escribe una muestra reproducible, con dos faltantes SOLO en entrenamiento."""
    if n_series < 1 or n_dias < 30 or horizonte < 1 or tiendas < 1:
        raise ValueError('Parámetros de generación inválidos')
    rng = np.random.default_rng(seed)
    fechas = pd.date_range(INICIO, periods=n_dias + horizonte, freq='D')
    t = np.arange(len(fechas))
    bloques = []
    for i in range(n_series):
        base = rng.uniform(15, 90)
        estacional = rng.uniform(3, 13) * np.sin(2*np.pi*t/7 + rng.uniform(-1, 1))
        tendencia = rng.uniform(-0.05, 0.09) * t
        demanda = np.round(np.maximum(0, base + estacional + tendencia + rng.normal(0, 4, len(t))), 2)
        demanda[rng.choice(n_dias, size=2, replace=False)] = np.nan
        bloques.append(pd.DataFrame({
            'unique_id': f'T{i % tiendas:02d}_SKU{i:04d}', 'ds': fechas, 'y': demanda
        }))
    destino = Path(ruta)
    destino.parent.mkdir(parents=True, exist_ok=True)
    pd.concat(bloques, ignore_index=True).to_parquet(destino, index=False)
    return destino


def preparar(ruta, horizonte=14, particiones=4):
    """Dask calcula características y estadísticas; split temporal SIN fuga de datos."""
    ruta = Path(ruta)
    ddf = dd.read_parquet(str(ruta)).repartition(npartitions=max(1, particiones))
    ddf['ds'] = dd.to_datetime(ddf['ds'])
    ddf['dia_semana'] = ddf['ds'].dt.dayofweek
    ddf['mes'] = ddf['ds'].dt.month
    # Computación distribuida/particionada para características y estadísticas.
    ddf['y'] = ddf['y'].astype('float64')
    estadisticas = ddf.groupby('unique_id')['y'].agg(['count', 'mean', 'std']).compute()
    tabla = ddf.compute().sort_values(['unique_id', 'ds'])
    serie_entrenamiento, observaciones = [], []
    for uid, grupo in tabla.groupby('unique_id', sort=True):
        if len(grupo) <= horizonte:
            raise ValueError(f'Serie {uid} sin suficientes observaciones')
        train, test = grupo.iloc[:-horizonte], grupo.iloc[-horizonte:]
        mediana = train['y'].median()  # Nunca usar datos de test para imputar entrenamiento
        if pd.isna(mediana):
            mediana = 0.0
        y = train['y'].fillna(mediana).to_numpy(dtype=np.float64)
        if not np.isfinite(y).all():
            raise ValueError(f'Valores de entrenamiento inválidos en {uid}')
        # La generación sintética no inserta nulos en test; no evaluar valores imputados.
        if test['y'].isna().any():
            raise ValueError(f'Valores faltantes en test de {uid}')
        serie_entrenamiento.append((uid, y))
        observaciones.append(pd.DataFrame({
            'unique_id': uid, 'ds': test['ds'].to_numpy(),
            'actual': test['y'].to_numpy(dtype=np.float64),
        }))
    return serie_entrenamiento, pd.concat(observaciones, ignore_index=True), estadisticas.reset_index()
