"""Pronóstico secuencial AutoARIMA."""
import numpy as np
from statsforecast.models import AutoARIMA

def pronosticar(item,horizonte=14):
    uid,y=item
    # Asegurar un vector numérico incluso si Dask entrega una lista o texto serializado.
    if isinstance(y, str):
        import ast as _ast
        try:
            y = _ast.literal_eval(y)
        except (ValueError, SyntaxError):
            import re
            y = [float(v) for v in re.findall(r"[-+]?(?:\d*\.\d+|\d+\.?\d*)(?:[eE][-+]?\d+)?", y)]
    y = np.asarray(y, dtype=np.float64)
    if y.ndim != 1 or y.size == 0 or not np.isfinite(y).all():
        raise ValueError(f"Serie inválida para {uid}: forma={y.shape}")
    model=AutoARIMA(season_length=7,max_p=1,max_q=1,max_P=0,max_Q=0,
                    max_d=1,max_D=1,stepwise=True,approximation=True,
                    allowmean=True,allowdrift=False)
    fitted=model.fit(y)
    return uid,np.asarray(fitted.predict(h=horizonte)['mean'],dtype=float)

def ejecutar(series,horizonte=14):
    return [pronosticar(item,horizonte) for item in series]
