"""Verificaciones sin entrenamiento costoso (requiere instalar dependencias)."""
import numpy as np
import pandas as pd
import pytest

from src.evaluacion import evaluar, comprobar_equivalencia, a_tabla
from src.preprocess import generar, preparar


def test_generacion_reproducible(tmp_path):
    a = generar(tmp_path/'a.parquet', n_series=4, n_dias=30, horizonte=3)
    b = generar(tmp_path/'b.parquet', n_series=4, n_dias=30, horizonte=3)
    pd.testing.assert_frame_equal(pd.read_parquet(a), pd.read_parquet(b))
    series, reales, resumen = preparar(a, horizonte=3, particiones=2)
    assert len(series) == 4
    assert len(reales) == 12
    assert len(resumen) == 4
    assert all(len(v) == 30 and np.isfinite(v).all() for _, v in series)


def test_metrica_y_equivalencia():
    real = pd.DataFrame({'unique_id':['x','x'], 'ds':pd.date_range('2025-01-01', periods=2), 'actual':[2,4]})
    pred = real[['unique_id','ds']].copy()
    pred['pred'] = [3,3]
    assert evaluar(pred, real) == (1.0, 1.0)
    comprobar_equivalencia(pred, pred.copy())
    mala = pred.copy()
    mala.loc[0, 'pred'] = 10
    with pytest.raises(ValueError):
        comprobar_equivalencia(pred, mala)
