import json
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaAdminClient, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "generos")
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "generosdb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "Mayvaldav032702."),
}

GENEROS = {"F": "Femenino", "M": "Masculino"}


def esperar(nombre, fn, intentos=30, pausa=2):
    """Reintenta hasta que el servicio esté listo."""
    for i in range(1, intentos + 1):
        try:
            return fn()
        except (psycopg2.OperationalError, NoBrokersAvailable) as e:
            print(f"Esperando {nombre} ({i}/{intentos})...")
            time.sleep(pausa)
    print(f"No se pudo conectar a {nombre}.")
    sys.exit(1)


def conectar_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS generos (
                id SERIAL PRIMARY KEY,
                genero VARCHAR(20) NOT NULL,
                broker VARCHAR(100) NOT NULL,
                topic VARCHAR(100) NOT NULL,
                particion INT,
                kafka_offset BIGINT,
                creado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    return conn


def conectar_kafka():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="generos-admin")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    return admin, producer


def mostrar_broker(admin):
    info = admin.describe_cluster()
    print("\n--- BROKER KAFKA ---")
    print(f"Bootstrap server : {KAFKA_BOOTSTRAP}")
    print(f"Cluster ID       : {info.get('cluster_id')}")
    for b in info.get("brokers", []):
        print(f"Broker           : id={b['node_id']} {b['host']}:{b['port']}")
    print(f"Topic            : {KAFKA_TOPIC}")
    print("--------------------\n")


def pedir_genero():
    while True:
        op = input("Digite el género (F = Femenino, M = Masculino, S = Salir): ").strip().upper()
        if op == "S":
            return None
        if op in GENEROS:
            return GENEROS[op]
        print("Opción inválida. Use F, M o S.")


def listar(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, genero, broker, topic, particion, kafka_offset, creado_en FROM generos ORDER BY id")
        filas = cur.fetchall()
    print("\n--- REGISTROS EN POSTGRESQL ---")
    for f in filas:
        print(f"#{f[0]} | {f[1]} | {f[2]} | topic={f[3]} p={f[4]} off={f[5]} | {f[6]:%Y-%m-%d %H:%M:%S}")
    print(f"Total: {len(filas)}\n")


def main():
    conn = esperar("PostgreSQL", conectar_db)
    admin, producer = esperar("Kafka", conectar_kafka)

    while True:
        genero = pedir_genero()
        if genero is None:
            break

        mostrar_broker(admin)

        meta = producer.send(
            KAFKA_TOPIC,
            {"genero": genero, "fecha": datetime.now(timezone.utc).isoformat()},
        ).get(timeout=15)
        print(f"Mensaje enviado a Kafka -> topic={meta.topic} partición={meta.partition} offset={meta.offset}")

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO generos (genero, broker, topic, particion, kafka_offset) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (genero, KAFKA_BOOTSTRAP, meta.topic, meta.partition, meta.offset),
            )
            nuevo_id = cur.fetchone()[0]
        print(f"Guardado en PostgreSQL con id={nuevo_id}\n")

    listar(conn)
    producer.close()
    admin.close()
    conn.close()


if __name__ == "__main__":
    main()
