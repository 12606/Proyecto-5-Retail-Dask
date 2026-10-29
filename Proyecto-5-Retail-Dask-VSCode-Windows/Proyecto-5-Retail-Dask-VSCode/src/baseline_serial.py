"""Baseline: un AutoARIMA por serie, ejecutado secuencialmente."""
import numpy as np
import warnings

# Mostrar advertencias repetitivas como máximo una vez por proceso.
warnings.filterwarnings('once', message=r'possible convergence problem.*', category=UserWarning)
from statsforecast.models import AutoARIMA


def pronosticar(item, horizonte=14):
    uid, y = item
    datos = np.asarray(y, dtype=np.float64)
    if datos.ndim != 1 or len(datos) < 30 or not np.isfinite(datos).all():
        raise ValueError(f'Serie inválida para {uid}: {datos.shape}')
    modelo = AutoARIMA(
        season_length=7,
        max_p=1, max_q=1, max_P=0, max_Q=0,
        max_d=1, max_D=1,
        stepwise=True, approximation=True,
        allowmean=True, allowdrift=False,
    )
    ajustado = modelo.fit(datos)
    pronostico = np.asarray(ajustado.predict(h=horizonte)['mean'], dtype=np.float64)
    if len(pronostico) != horizonte or not np.isfinite(pronostico).all():
        raise ValueError(f'Pronóstico inválido para {uid}')
    return uid, pronostico


def ejecutar(series, horizonte=14):
    return [pronosticar(item, horizonte) for item in series]
