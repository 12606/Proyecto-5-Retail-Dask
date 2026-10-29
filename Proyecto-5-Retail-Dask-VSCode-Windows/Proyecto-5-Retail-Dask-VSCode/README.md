# Proyecto 5 — Retail: pronóstico de demanda con Dask (Windows / VS Code)

**Objetivo:** comparar el pronóstico de demanda de **1,000 series SKU–tienda** usando ejecución serial, `dask.delayed` por lotes de 50–100 series y `dask.dataframe.map_partitions`. Se utiliza **AutoARIMA** de StatsForecast y un clúster local Dask que funciona **únicamente con CPU**.

Este proyecto está adaptado para **AMD Ryzen 7 7730U (8 núcleos / 16 hilos), 16 GB RAM y Windows de 64 bits**. Recomendación inicial: **4 workers, 1 hilo por worker y lotes de 50**. Se prueban otras configuraciones para medir, no para presuponer, aceleración.

## A. Preparación desde cero en Visual Studio Code (Windows)

1. Instala **Python 3.11 de 64 bits** desde [python.org](https://www.python.org/downloads/) (marca *Add python.exe to PATH* si se ofrece). También se admite Python 3.12. Instala **Visual Studio Code** con la extensión **Python** de Microsoft.
2. Descarga y **extrae el ZIP** del proyecto, por ejemplo en `Documentos\GitHub\Proyecto-5-Retail-Dask-VSCode`.
3. Abre **VS Code → Archivo → Abrir carpeta**, seleccionando esa carpeta (debe contener `main.py` y `requirements.txt`).
4. Desde el Explorador, haz doble clic en **`configurar_windows.bat`** (o ejecútalo desde CMD). Crea `.venv` e instala las dependencias. **Se necesita Internet solo para la instalación inicial.**
5. En VS Code, pulsa `Ctrl+Shift+P` → **Python: Select Interpreter** → selecciona `.venv\Scripts\python.exe`.
6. Ejecuta **`ejecutar_menu.bat`** (doble clic) o, en la terminal integrada de VS Code (PowerShell), usa:

   ```powershell
   .\.venv\Scripts\python.exe menu.py
   ```

   **No es necesario activar PowerShell**, evitando problemas con su política de ejecución.

### Alternativa: instalación paso a paso desde PowerShell

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe menu.py
```

Si `py -3.11` no existe, instala Python 3.11 de 64 bits o utiliza `py -3.12`. Si el mensaje dice **No module named dask**, revisa que hayas seleccionado el intérprete `.venv`.

## B. Ejecución y experimentos

**Prueba de diagnóstico (100 series):**

```powershell
.\.venv\Scripts\python.exe main.py --series 100 --lote 50 --workers 4 --hilos 1
```

**Prueba final (1,000 series) para la entrega:**

```powershell
.\.venv\Scripts\python.exe main.py --series 1000 --lote 50 --workers 4 --hilos 1
```

**Optimización de Dask con cuatro perfiles (100 series para empezar):**

```powershell
.\.venv\Scripts\python.exe optimizacion.py --series 100
```

**Optimización final sobre las 1,000 series:**

```powershell
.\.venv\Scripts\python.exe optimizacion.py --series 1000
```

La optimización compara estos perfiles:

| Perfil | Workers | Hilos/worker | Lote |
|---|---:|---:|---:|
| A | 2 | 1 | 50 |
| B | 2 | 2 | 100 |
| C | 4 | 1 | 50 |
| D | 4 | 2 | 100 |

El baseline serial se entrena **una sola vez** por ejecución de `optimizacion.py`; los métodos paralelos se entrenan bajo cada perfil y se validan contra las predicciones seriales. Los tiempos **no incluyen el arranque del clúster**, cuyo costo se registra por separado. **No hay garantía de speedup > 1**.

## C. Dataset y evaluación

- Dataset **sintético y reproducible**, sin descargas externas: 1,000 series independientes distribuidas en 10 tiendas; 134 días/serie (120 entrenamiento + 14 prueba), con estacionalidad semanal, tendencia y ruido.
- `generar()` inserta dos faltantes **solo en el entrenamiento**; `preparar()` imputa usando la mediana de **entrenamiento** para evitar fuga de datos.
- El preprocesamiento usa **Dask DataFrame** para lectura, creación de variables y agregación de estadísticas por serie.
- La evaluación compara tiempo de entrenamiento + predicción, `speedup = serial / paralelo`, MAE y RMSE sobre el mismo holdout de 14 días.
- Las predicciones por cada método se validan para comprobar que **no cambien al paralelizar**.
- Se mide el tiempo de tareas de control del scheduler **solo como proxy de overhead**. No se asegura que mida la comunicación aislada entre workers.
- Los mensajes `possible convergence problem` del modelo ARIMA son **advertencias**, no necesariamente errores fatales; deben discutirse en conclusiones.

### Salidas generadas al ejecutar

| Ruta | Contenido |
|---|---|
| `data/demanda_sintetica.parquet` | Dataset sintético regenerable (no se sube a Git por defecto) |
| `data/estadisticas_series.csv` | Resumen de series calculado usando Dask |
| `benchmarks/comparacion.csv` | Serial, Dask Delayed, Dask DataFrame: tiempos, MAE, RMSE y speedup |
| `benchmarks/overhead.csv` | Costo de preparación, arranque del clúster y tareas de control |
| `benchmarks/pronosticos.csv` | Predicciones de Dask DataFrame |
| `benchmarks/valores_reales.csv` | Demanda real del periodo de prueba |
| `benchmarks/optimizacion.csv` | Comparación de los perfiles A, B, C y D |
| `benchmarks/optimizacion_overhead.csv` | Overhead por configuración |
| `benchmarks/graficas/*.png` | Comparativas y ejemplo de pronóstico |

Los archivos de resultados **solo existirán después de ejecutar** los experimentos. El programa **no trae métricas inventadas**. Para entregar, ejecuta 1,000 series y conserva los CSV y PNG generados.

## D. Estructura del repositorio

```text
Proyecto-5-Retail-Dask-VSCode/
├── .vscode/                  # Configuraciones de VS Code
├── data/                     # Dataset generado
├── src/
│   ├── preprocess.py         # Dataset, limpieza y Dask DataFrame
│   ├── baseline_serial.py    # AutoARIMA serial
│   ├── forecast_dask.py      # Dask Delayed y map_partitions
│   ├── experimento.py        # Benchmark común
│   └── evaluacion.py         # MAE, RMSE, gráficas y comparación
├── benchmarks/graficas/      # CSV y PNG de los experimentos
├── notebooks/                # Versión previa de Google Colab (opcional)
├── tests/                    # Pruebas unitarias básicas
├── main.py                   # Benchmark principal
├── optimizacion.py           # Matriz de 4 configuraciones
├── menu.py                   # Menú interactivo
├── configurar_windows.bat    # Configurar entorno Python en Windows
├── ejecutar_menu.bat         # Abrir menú
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

## E. Subirlo a GitHub Desktop

1. **Extrae** el ZIP y abre la carpeta del proyecto.
2. En **GitHub Desktop → File → Add local repository**. Si te indica que no es un repositorio Git, selecciona **Create a repository here**.
3. Revisa **Changes**, escribe el commit `Adaptar Proyecto 5 a VS Code con Dask multiproceso` y selecciona **Commit to main**.
4. Haz clic en **Publish repository**. Después de obtener resultados, haz otro commit con `Agregar pruebas y resultados de rendimiento` y pulsa **Push origin**.
5. `.venv/` y el dataset Parquet se excluyen de Git porque se pueden regenerar. Los CSV y PNG **sí se pueden incluir** para que tu profesor vea las métricas.

## F. Solución de problemas

- **Windows no crea workers:** ejecuta `main.py` o `optimizacion.py` como script (no selecciones y ejecutes solo el cuerpo de la función en la ventana interactiva); el proyecto incluye `if __name__ == '__main__'` y `multiprocessing.freeze_support()`.
- **Muchos warnings de ARIMA:** son advertencias de convergencia; si el programa termina y se guardan CSV, revisa los errores MAE/RMSE en vez de ocultar las advertencias sin analizarlas.
- **Falta RAM o el sistema se ralentiza:** cierra otras aplicaciones, ejecuta 100 series o prueba `--workers 2 --hilos 1` antes del benchmark final.
- **Dask sale más lento que serial:** es un resultado posible por overhead, recursos y duración de cada serie. Documenta el comportamiento real.
- **No se pueden instalar paquetes:** revisa Internet y usa Python 3.11 o 3.12. Repite `configurar_windows.bat`.

## G. Pruebas automáticas opcionales

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
```