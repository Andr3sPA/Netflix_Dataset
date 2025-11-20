#!/usr/bin/env python3
"""
Script de prueba para ejecutar consultas contra la BD usando el `engine` definido en `study.py`.

Ejecuta:
  - `SELECT version()` para verificar la conexión
  - `SELECT count(*) FROM staging_netflix` para comprobar si la tabla de staging existe y cuántas filas tiene

Este script solo usa el código (no ejecuta nada en la terminal desde aquí).
"""

from study import engine
from sqlalchemy import text


def run_test_queries():
    try:
        with engine.connect() as conn:
            # Query 1: versión del servidor
            ver = conn.execute(text("SELECT version()"))
            version_val = ver.scalar()
            print("DB version:", version_val)

            # Query 2: contar filas en staging_netflix (si existe)
            try:
                cnt = conn.execute(text("SELECT count(*) FROM staging_netflix"))
                print("staging_netflix row count:", cnt.scalar())
            except Exception as e:
                # No propagamos el error: la tabla puede no existir
                print("Nota: no se pudo leer 'staging_netflix' (quizá no existe). Error:", e)

    except Exception as e:
        print("Error al conectar o ejecutar consultas:")
        raise


if __name__ == '__main__':
    run_test_queries()
