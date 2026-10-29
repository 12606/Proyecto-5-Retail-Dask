"""Ejemplo: python main.py --series 1000 --lote 50"""
import argparse, sys, time
from pathlib import Path
import numpy as np
import pandas as pd
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT/'src'))
from preprocess import generar, preparar
from baseline_serial import ejecutar as serial
from forecast_dask import ejecutar as paralelo


def evaluar(pred, reales):
    combinado = reales.merge(pred, on=['unique_id','ds'], validate='one_to_one')
    if len(combinado) != len(reales) or combinado['pred'].isna().any():
        raise ValueError('Pronósticos incompletos')
    err = combinado['actual'].to_numpy()-combinado['pred'].to_numpy()
    return float(np.mean(np.abs(err))), float(np.sqrt(np.mean(err**2)))


def tabla_pronosticos(resultados, inicio, dias):
    rows = [(uid, inicio+pd.Timedelta(days=dias+h), float(v))
            for uid, vals in resultados for h,v in enumerate(vals)]
    return pd.DataFrame(rows, columns=['unique_id','ds','pred'])


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--series',type=int,default=1000)
    p.add_argument('--dias',type=int,default=120)
    p.add_argument('--horizonte',type=int,default=14)
    p.add_argument('--lote',type=int,default=50)
    p.add_argument('--workers',type=int,default=4)
    p.add_argument('--hilos',type=int,default=2)
    args=p.parse_args()
    if min(args.series,args.horizonte,args.lote,args.workers,args.hilos)<1 or args.dias<30:
        p.error('Parámetros inválidos')
    ruta=generar(ROOT/'data'/'demanda_sintetica.parquet',args.series,args.dias,args.horizonte)
    inicio=time.perf_counter()
    series,reales=preparar(ruta,args.horizonte)
    prep=time.perf_counter()-inicio
    inicio=time.perf_counter()
    r_serial=serial(series,args.horizonte)
    t_serial=time.perf_counter()-inicio
    r_delayed, r_ddf, tiempos=paralelo(series,args.horizonte,args.lote,args.workers,args.hilos)
    fecha=pd.Timestamp('2025-01-01')
    serial_df=tabla_pronosticos(r_serial,fecha,args.dias)
    delayed_df=tabla_pronosticos(r_delayed,fecha,args.dias)
    dataframe_df=r_ddf.copy()
    dataframe_df['ds']=fecha+pd.to_timedelta(args.dias+dataframe_df['h'],unit='D')
    comparacion=[]
    for nombre,pron,t in [('Serial',serial_df,t_serial),('Dask delayed',delayed_df,tiempos['dask_delayed_s']),('Dask DataFrame',dataframe_df,tiempos['dask_dataframe_s'])]:
        mae,rmse=evaluar(pron,reales)
        comparacion.append({'metodo':nombre,'segundos':t,'speedup':t_serial/t,'MAE':mae,'RMSE':rmse})
    tabla=pd.DataFrame(comparacion)
    assert np.allclose(tabla['MAE'],tabla['MAE'].iloc[0],atol=1e-5)
    assert np.allclose(tabla['RMSE'],tabla['RMSE'].iloc[0],atol=1e-5)
    carpeta=ROOT/'benchmarks'; carpeta.mkdir(exist_ok=True)
    tabla.to_csv(carpeta/'comparacion.csv',index=False)
    pd.DataFrame([{'series':args.series,'preprocesamiento_s':prep,**tiempos}]).to_csv(carpeta/'overhead.csv',index=False)
    dataframe_df.to_csv(carpeta/'pronosticos.csv',index=False)
    print(tabla.to_string(index=False))
    print('\nOverhead control (proxy, no comunicación aislada):',round(tiempos['overhead_control_s'],4),'s')
