# Proyecto 5 — Retail: pronóstico de demanda con Dask

Proyecto de computación paralela: **1,000 series SKU–tienda**, 10 tiendas, 120 días de entrenamiento y 14 de prueba, demanda sintética con estacionalidad y ruido, modelo AutoARIMA.

## Abrir en Google Colab

Sube `notebooks/Proyecto_5_Retail_Dask_Colab.ipynb` a [Google Colab](https://colab.research.google.com/) y ejecuta todas las celdas. El notebook es autocontenido: crea los módulos, ejecuta el benchmark y genera los resultados. **No contiene resultados precalculados**.

## Ejecutar localmente

```bash
pip install -r requirements.txt
python main.py --series 1000 --lote 50 --workers 4 --hilos 2
```

Para una prueba rápida, usa `--series 20` o `--series 100` antes de ejecutar las 1,000 series.

## Estructura

- `data/`: dataset sintético generado al ejecutar.
- `src/preprocess.py`: datos y preprocesamiento con Dask DataFrame.
- `src/baseline_serial.py`: AutoARIMA secuencial.
- `src/forecast_dask.py`: Dask delayed por lotes y predicción mediante `map_partitions`.
- `benchmarks/`: CSV de comparación, overhead y pronósticos; gráficas del notebook.
- `notebooks/`: Google Colab.
- `main.py`: benchmark reproducible y verificaciones de equivalencia.

## Métricas y limitaciones

- `segundos`: tiempo de entrenamiento + predicción, **sin preprocesamiento**.
- `speedup`: tiempo serial dividido por tiempo del método.
- `MAE` y `RMSE`: error sobre los mismos 14 días de prueba.
- `overhead_control_s`: tiempo de tareas triviales; **proxy de overhead de scheduler**, no medición aislada de comunicación.
- El clúster usa 4 workers y 2 hilos cada uno, con `processes=False` para compatibilidad con Colab. El rendimiento depende del runtime y del GIL.

El enunciado menciona una reducción de tiempo a ~1/3 como **referencia**; el proyecto mide resultados reales, no garantiza esa cifra.

## Corrección de Dask DataFrame
Se desactiva `dataframe.convert-string` al construir el DataFrame para preservar los vectores NumPy de cada serie. El pronosticador valida y convierte explícitamente los datos a `float64`. El notebook corregido inicia con 100 series para diagnóstico; cambiar a 1000 para la prueba final.

## Optimización experimental en Google Colab

Abrir `notebooks/Proyecto_5_Retail_Dask_Colab_OPTIMIZACION.ipynb` y ejecutar las celdas en orden. La sección 9 compara cuatro configuraciones de workers/hilos/lotes. Comenzar con `SERIES_OPT = 100` y luego usar `1000` para la evaluación completa. Los CSV y gráficas se guardan en `benchmarks/`. Cada configuración vuelve a entrenar los tres métodos; no se prometen aceleraciones en Colab.
