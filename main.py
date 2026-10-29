import json
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from google.cloud import firestore
from kafka import KafkaAdminClient, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Cargar variables de entorno desde .env
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "generos")
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5433")),
    "dbname": os.getenv("DB_NAME", "generosdb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

FIRESTORE_COLLECTION = os.getenv("FIRESTORE_COLLECTION", "generos")

# Opciones sugeridas; el usuario también puede escribir cualquier otro género.
GENEROS_SUGERIDOS = [
    genero.strip()
    for genero in os.getenv(
        "GENEROS_SUGERIDOS",
        "Femenino,Masculino,No binario,Otro,Prefiero no decirlo",
    ).split(",")
    if genero.strip()
]


def conectar_firestore():
    """Usa la cuenta de servicio indicada o el archivo local ./firebase-key.json."""
    ruta = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not ruta or not os.path.isfile(ruta):
        if os.path.isfile("firebase-key.json"):
            ruta = os.path.abspath("firebase-key.json")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta
        else:
            print("Falta la llave de Firebase. Coloca 'firebase-key.json' en la carpeta o define GOOGLE_APPLICATION_CREDENTIALS.")
            sys.exit(1)
    return firestore.Client()


def subir_a_firestore(fs, registro_id, genero, meta):
    fs.collection(FIRESTORE_COLLECTION).document(str(registro_id)).set(
        {
            "id": registro_id,
            "genero": genero,
            "broker": KAFKA_BOOTSTRAP,
            "topic": meta.topic,
            "particion": meta.partition,
            "offset": meta.offset,
            "creado_en": firestore.SERVER_TIMESTAMP,
        }
    )


def esperar(nombre, fn, intentos=30, pausa=2):
    """Reintenta hasta que el servicio esté listo."""
    for i in range(1, intentos + 1):
        try:
            return fn()
        except (psycopg2.OperationalError, NoBrokersAvailable, ValueError, OSError) as e:
            print(f"Esperando {nombre} ({i}/{intentos})... ({type(e).__name__})")
            time.sleep(pausa)
    print(f"No se pudo conectar a {nombre}.")
    sys.exit(1)


def conectar_db():
    if not DB_CONFIG["password"]:
        print("[!] Error: DB_PASSWORD no está definida en las variables de entorno (.env).")
        sys.exit(1)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS generos (
                id SERIAL PRIMARY KEY,
                genero VARCHAR(100) NOT NULL,
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
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="generos-admin",
        api_version=(2, 5, 0),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=(2, 5, 0),
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


def mostrar_opciones():
    """Muestra las opciones sugeridas una sola vez al iniciar."""
    print("\nGéneros sugeridos:")
    for i, genero in enumerate(GENEROS_SUGERIDOS, 1):
        print(f"  {i}. {genero}")
    print("  También puedes escribir cualquier otro género (por ejemplo: 'Binario').\n")


def pedir_genero():
    while True:
        valor = input("Elige un número o escribe el género que desees (o 'S' para salir): ").strip()
        if not valor:
            print("El género no puede estar vacío.")
            continue
        if len(valor) > 100:
            print("El género no puede superar los 100 caracteres.")
            continue
        if valor.upper() == "S":
            return None
        if valor.isdigit():
            indice = int(valor)
            if 1 <= indice <= len(GENEROS_SUGERIDOS):
                return GENEROS_SUGERIDOS[indice - 1]
            print(
                f"Opción inválida. Elige un número entre 1 y {len(GENEROS_SUGERIDOS)} "
                "o escribe el género que desees."
            )
            continue
        # Texto libre: se acepta cualquier género, aunque no esté en la lista.
        return valor


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
    fs = conectar_firestore()
    mostrar_opciones()

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
        print(f"Guardado en PostgreSQL con id={nuevo_id}")

        try:
            subir_a_firestore(fs, nuevo_id, genero, meta)
            print(f"Subido a Firestore: {FIRESTORE_COLLECTION}/{nuevo_id}\n")
        except Exception as e:
            print(f"No se pudo subir a Firestore: {e}\n")

    listar(conn)
    producer.close()
    admin.close()
    conn.close()


if __name__ == "__main__":
    main()