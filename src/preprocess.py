"""Generación reproducible y preparación de datos de demanda."""
from pathlib import Path
import numpy as np
import pandas as pd
import dask.dataframe as dd

def generar(ruta, n_series=1000, n_dias=120, horizonte=14, seed=2026):
    rng=np.random.default_rng(seed)
    dates=pd.date_range('2025-01-01',periods=n_dias+horizonte,freq='D')
    partes=[]
    for i in range(n_series):
        t=np.arange(len(dates)); base=rng.uniform(15,90)
        seasonal=rng.uniform(3,13)*np.sin(2*np.pi*t/7+rng.uniform(-1,1))
        trend=rng.uniform(-0.05,0.09)*t
        y=np.round(np.maximum(0,base+seasonal+trend+rng.normal(0,4,len(t))),2)
        y[rng.choice(n_dias,size=2,replace=False)]=np.nan
        partes.append(pd.DataFrame({'unique_id':f'T{i%10:02d}_SKU{i:04d}','ds':dates,'y':y}))
    path=Path(ruta);path.parent.mkdir(parents=True,exist_ok=True)
    pd.concat(partes,ignore_index=True).to_parquet(path,index=False)
    return path

def preparar(ruta, horizonte=14):
    ddf=dd.read_parquet(str(ruta))
    ddf['ds']=dd.to_datetime(ddf['ds'])
    ddf['dia_semana']=ddf['ds'].dt.dayofweek
    ddf['mes']=ddf['ds'].dt.month
    df=ddf.compute().sort_values(['unique_id','ds'])
    series=[]; reales=[]
    for uid,g in df.groupby('unique_id',sort=True):
        train=g.iloc[:-horizonte].copy();test=g.iloc[-horizonte:].copy()
        mediana=train['y'].median()
        series.append((uid,train['y'].fillna(mediana).fillna(0).to_numpy(dtype='float64')))
        reales.append(pd.DataFrame({'unique_id':uid,'ds':test['ds'].to_numpy(),
                                     'actual':test['y'].fillna(mediana).to_numpy()}))
    return series,pd.concat(reales,ignore_index=True)
