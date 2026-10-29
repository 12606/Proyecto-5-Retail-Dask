# Proyecto 5 — Retail: Pronóstico de demanda con Dask

Proyecto de **Computación Paralela** desarrollado en Python para pronosticar la demanda diaria de productos en múltiples tiendas utilizando **Dask y StatsForecast (AutoARIMA)**.

El proyecto trabaja con **1,000 series temporales SKU–tienda**, distribuidas en 10 tiendas, utilizando 120 días de entrenamiento y 14 días de prueba. Los datos sintéticos incluyen estacionalidad y ruido.

## Tecnologías utilizadas

- Python
- Dask Distributed y Dask DataFrame
- StatsForecast (AutoARIMA)
- NumPy y Pandas
- Matplotlib
- Google Colab
- Visual Studio Code

## Versiones disponibles

Este repositorio contiene dos implementaciones del mismo proyecto:

| Versión | Ubicación | Descripción |
|---|---|---|
| Google Colab | `notebooks/` | Ejecución en la nube mediante notebooks |
| Visual Studio Code | `vscode/` | Ejecución local en Windows utilizando CPU |

**No se requiere GPU para ejecutar el proyecto.** El procesamiento paralelo se realiza utilizando Dask LocalCluster y los recursos del procesador.

## 1. Ejecución en Google Colab

1. Abre [Google Colab](https://colab.research.google.com/).
2. Selecciona **Archivo → Abrir cuaderno → GitHub**.
3. Busca este repositorio y abre el notebook dentro de `notebooks/`.
4. Ejecuta las celdas en orden.

El notebook genera los datos sintéticos, entrena los modelos, realiza los pronósticos y calcula los resultados.

Para las pruebas de optimización, utiliza el notebook `Proyecto_5_Retail_Dask_Colab_OPTIMIZACION.ipynb`, si está disponible.

La sección de optimización compara distintas configuraciones de workers, hilos y tamaños de lote. Se recomienda comenzar con 100 series y después realizar el experimento completo con 1,000.

## 2. Ejecución local en Visual Studio Code

La versión de escritorio se encuentra en la carpeta `vscode/`.

### Requisitos

- Windows de 64 bits.
- Python 3.11 o 3.12.
- Visual Studio Code con la extensión de Python.
- Conexión a Internet para instalar dependencias.

### Instalación

Abre una terminal de VS Code en la raíz del repositorio y entra en la carpeta de la versión local:

```powershell
cd vscode
```

Ejecuta el configurador:

```powershell
.\configurar_windows.bat
```

Este archivo crea un entorno virtual `.venv` e instala las dependencias.

Después, ejecuta el menú:

```powershell
.\ejecutar_menu.bat
```

También puedes iniciar el menú utilizando Python:

```powershell
.\.venv\Scripts\python.exe menu.py
```

### Menú interactivo

La versión de Visual Studio Code permite:

1. Ejecutar una prueba rápida de 100 series.
2. Ejecutar la prueba completa de 1,000 series.
3. Realizar benchmarks personalizados.
4. Comparar configuraciones de Dask.
5. Consultar los resultados CSV.
6. Abrir las gráficas generadas.

### Ejecutar directamente desde la terminal

Benchmark principal:

```powershell
.\.venv\Scripts\python.exe main.py --series 1000 --lote 50 --workers 4 --hilos 1
```

Optimización:

```powershell
.\.venv\Scripts\python.exe optimizacion.py --series 1000
```

La configuración inicial recomendada para un **AMD Ryzen 7 7730U con 16 GB de RAM** es de 4 workers y 1 hilo por worker.

## 3. Estructura del repositorio

```text
Proyecto-5-Retail-Dask/
├── data/
├── src/
│   ├── preprocess.py
│   ├── baseline_serial.py
│   └── forecast_dask.py
├── benchmarks/
├── notebooks/
│   └── Proyecto_5_Retail_Dask_Colab.ipynb
├── vscode/
│   ├── data/
│   ├── src/
│   ├── benchmarks/
│   ├── notebooks/
│   ├── tests/
│   ├── main.py
│   ├── menu.py
│   ├── optimizacion.py
│   ├── configurar_windows.bat
│   ├── ejecutar_menu.bat
│   ├── requirements.txt
│   └── README.md
├── main.py
├── requirements.txt
├── .gitignore
└── README.md
```

## 4. Estrategias de paralelización

Se comparan tres métodos:

**Ejecución serial:** entrenamiento y pronóstico de cada serie mediante un ciclo secuencial.

**Dask Delayed:** procesamiento paralelo de series agrupadas en lotes para reducir el costo de planificación.

**Dask DataFrame:** entrenamiento y predicción sobre particiones mediante `map_partitions`.

El objetivo es evaluar cómo se comporta cada estrategia al trabajar con múltiples series temporales.

## 5. Métricas de evaluación

| Métrica | Descripción |
|---|---|
| Tiempo | Duración del entrenamiento y predicción |
| Speedup | Tiempo serial dividido entre tiempo paralelo |
| MAE | Error absoluto medio |
| RMSE | Raíz del error cuadrático medio |
| Overhead | Estimación del costo de las tareas de control |

El tiempo del benchmark principal no incluye el preprocesamiento inicial. El overhead de control es una aproximación del costo del scheduler y no una medición aislada de toda la comunicación.

Un speedup mayor que 1 indica una aceleración frente al método serial.

## 6. Resultados y optimización

El proyecto permite comparar distintas configuraciones de:

- Número de workers.
- Hilos por worker.
- Tamaño de lote.
- Cantidad de series temporales.

Los resultados y gráficas se almacenan en las carpetas `benchmarks/` de cada implementación.

No se garantiza que Dask siempre sea más rápido que el método secuencial: el rendimiento depende de los recursos disponibles y del costo de distribuir las tareas.

## 7. Consideraciones técnicas

En Google Colab, Dask puede utilizar `processes=False` por compatibilidad con el entorno. En la versión local para Windows se utilizan procesos de trabajo de CPU.

Para evitar errores de tipos de datos con AutoARIMA, los vectores de demanda se convierten a NumPy `float64`.

Las advertencias estadísticas de convergencia deben distinguirse de errores que interrumpen la ejecución.

El resultado de referencia del documento, que indica una posible reducción del tiempo a un tercio, no se considera garantizado. Las conclusiones se basarán en las mediciones reales.

## 8. Control de versiones con GitHub

Ambas implementaciones se mantienen dentro de un mismo repositorio para facilitar su mantenimiento, documentación y comparación de resultados.

Los cambios pueden publicarse mediante GitHub Desktop utilizando **Commit to main** y **Push origin**.

No deben subirse entornos virtuales `.venv`, archivos de caché ni dependencias instaladas localmente.

---

**Proyecto académico — Computación Paralela**

Pronóstico de demanda de múltiples series temporales mediante procesamiento secuencial y paralelo con Dask.