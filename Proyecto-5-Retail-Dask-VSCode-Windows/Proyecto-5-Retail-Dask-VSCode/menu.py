"""Menú de terminal interactivo para Windows/Visual Studio Code."""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def solicitar_entero(mensaje, por_defecto, minimo=1):
    while True:
        entrada = input(f'{mensaje} [{por_defecto}]: ').strip()
        if not entrada:
            return por_defecto
        try:
            n = int(entrada)
            if n >= minimo:
                return n
        except ValueError:
            pass
        print(f'Introduce un entero mayor o igual a {minimo}.')


def ejecutar_archivo(nombre, argumentos):
    comando = [sys.executable, str(ROOT/nombre), *argumentos]
    print('\n$ ' + ' '.join(comando) + '\n', flush=True)
    try:
        resultado = subprocess.run(comando, cwd=ROOT, check=False)
        if resultado.returncode:
            print(f'\nERROR: el programa terminó con código {resultado.returncode}.')
    except KeyboardInterrupt:
        print('\nSe interrumpió la ejecución.')
    input('\nPresiona Enter para volver al menú...')


def mostrar_resultados():
    import csv
    carpeta = ROOT/'benchmarks'
    for nombre in ('comparacion.csv','overhead.csv','optimizacion.csv'):
        archivo = carpeta/nombre
        if not archivo.exists():
            print(f'No existe aún: {archivo.name}')
            continue
        print(f'\n--- {nombre} ---')
        with archivo.open(encoding='utf-8-sig', newline='') as file:
            reader = csv.reader(file)
            for i, linea in enumerate(reader):
                if i == 12:
                    print('... (el resto está en el CSV)')
                    break
                print('  |  '.join(linea))
    input('\nPresiona Enter para volver al menú...')


def main():
    while True:
        print('''
========================================================
  PROYECTO 5 - PRONOSTICO DE DEMANDA (CPU / DASK)
========================================================
  1) Prueba rápida: 100 series, 4 workers, 1 hilo
  2) Prueba final: 1000 series, 4 workers, 1 hilo
  3) Benchmark con configuración personalizada
  4) Optimización: 4 configuraciones de Dask
  5) Consultar los resultados CSV
  6) Abrir carpeta de gráficas
  0) Salir
''')
        opcion = input('Elige una opción: ').strip()
        if opcion == '0':
            print('Fin.')
            return
        if opcion in ('1','2'):
            n = '100' if opcion == '1' else '1000'
            ejecutar_archivo('main.py',['--series',n,'--lote','50','--workers','4','--hilos','1'])
        elif opcion == '3':
            n = solicitar_entero('Series', 1000)
            lotes = solicitar_entero('Tamaño de lote', 50)
            workers = solicitar_entero('Workers', 4)
            hilos = solicitar_entero('Hilos por worker', 1)
            ejecutar_archivo('main.py', ['--series',str(n),'--lote',str(lotes),
                                          '--workers',str(workers),'--hilos',str(hilos)])
        elif opcion == '4':
            n = solicitar_entero('Series para optimización (100 rápido, 1000 final)',100)
            ejecutar_archivo('optimizacion.py', ['--series',str(n)])
        elif opcion == '5':
            mostrar_resultados()
        elif opcion == '6':
            carpeta = ROOT/'benchmarks'/'graficas'
            carpeta.mkdir(parents=True, exist_ok=True)
            if os.name == 'nt':
                os.startfile(carpeta)
            else:
                print('Revisa la carpeta:', carpeta)
        else:
            print('Opción no reconocida.')


if __name__ == '__main__':
    main()
