import json
import os
import signal
import sys
import time

import psycopg2
from dotenv import load_dotenv
from google.cloud import firestore
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Cargar variables de entorno desde el archivo .env
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "generos")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "generos-consumer-group")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5433")),
    "dbname": os.getenv("DB_NAME", "generosdb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

FIRESTORE_COLLECTION = os.getenv("FIRESTORE_COLLECTION", "generos")


def conectar_firestore():
    """Conecta con Firebase Firestore usando GOOGLE_APPLICATION_CREDENTIALS o fallback local."""
    ruta = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not ruta or not os.path.isfile(ruta):
        if os.path.isfile("firebase-key.json"):
            ruta = os.path.abspath("firebase-key.json")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta
        else:
            print(f"[!] Advertencia: Llave de Firebase no encontrada en '{ruta}' ni en './firebase-key.json'.")
            print("    Los registros se guardarán en PostgreSQL, pero no en Firestore.")
            return None

    try:
        print(f"[+] Conectando a Firebase con credenciales: {ruta}")
        return firestore.Client()
    except Exception as e:
        print(f"[!] Error al inicializar Firestore Client: {e}")
        return None


def subir_a_firestore(fs, registro_id, genero, topic, partition, offset, fecha_envio):
    """Guarda el documento en Firestore de manera inmediata."""
    if fs is None:
        return
    doc_ref = fs.collection(FIRESTORE_COLLECTION).document(str(registro_id))
    doc_ref.set(
        {
            "id": registro_id,
            "genero": genero,
            "broker": KAFKA_BOOTSTRAP,
            "topic": topic,
            "particion": partition,
            "offset": offset,
            "fecha_envio": fecha_envio,
            "creado_en": firestore.SERVER_TIMESTAMP,
        }
    )


def conectar_db():
    """Conecta a PostgreSQL y crea la tabla con soporte para texto libre si no existe."""
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


def conectar_consumer():
    """Inicializa el consumidor de Kafka en modo streaming."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        api_version=(2, 5, 0),
    )


def esperar(nombre, fn, intentos=30, pausa=2):
    """Reintenta la conexión hasta que el servicio esté listo."""
    for i in range(1, intentos + 1):
        try:
            return fn()
        except (psycopg2.OperationalError, NoBrokersAvailable, ValueError, OSError) as e:
            print(f"Esperando {nombre} ({i}/{intentos})... ({type(e).__name__})")
            time.sleep(pausa)
    print(f"No se pudo conectar a {nombre}.")
    sys.exit(1)


def main():
    print("=" * 65)
    print(" INICIANDO CONSUMIDOR STREAMING DE KAFKA ".center(65, "="))
    print("=" * 65)

    conn = esperar("PostgreSQL", conectar_db)
    consumer = esperar("Kafka Consumer", conectar_consumer)
    fs = conectar_firestore()

    print(f"\n[+] Conectado a Kafka Broker : {KAFKA_BOOTSTRAP}")
    print(f"[+] Escuchando Topic        : {KAFKA_TOPIC}")
    print(f"[+] Grupo de Consumidores   : {KAFKA_GROUP_ID}")
    print(f"[+] Base de Datos           : PostgreSQL ({DB_CONFIG['host']}:{DB_CONFIG['port']})")
    print(f"[+] Colección Firestore     : {FIRESTORE_COLLECTION}")
    print(f"[+] Estado Firestore        : {'ACTIVO (Streaming a la nube)' if fs else 'INACTIVO (sin credenciales)'}")
    print("\n>>> STREAMING ACTIVO: Esperando eventos... (Ctrl + C para salir) <<<\n" + "-" * 65)

    def salir(sig, frame):
        print("\n\nCerrando conexiones del consumidor...")
        consumer.close()
        conn.close()
        print("Consumidor finalizado con éxito.")
        sys.exit(0)

    signal.signal(signal.SIGINT, salir)
    signal.signal(signal.SIGTERM, salir)

    try:
        # Bucle continuo de streaming de eventos
        for msg in consumer:
            payload = msg.value
            genero = payload.get("genero", "Desconocido")
            fecha_envio = payload.get("fecha", "N/A")

            print(f"\n⚡ [STREAMING EVENTO DETECTADO]")
            print(f"   Topic: {msg.topic} | Partición: {msg.partition} | Offset: {msg.offset}")
            print(f"   Género recibido: '{genero}' | Fecha: {fecha_envio}")

            # 1. Guardar en PostgreSQL
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO generos (genero, broker, topic, particion, kafka_offset)
                        VALUES (%s, %s, %s, %s, %s) RETURNING id
                        """,
                        (genero, KAFKA_BOOTSTRAP, msg.topic, msg.partition, msg.offset),
                    )
                    nuevo_id = cur.fetchone()[0]
                print(f"   ✔ [PostgreSQL] Guardado exitoso con ID={nuevo_id}")
            except Exception as e:
                print(f"   ✖ [PostgreSQL Error]: {e}")
                continue

            # 2. Guardar en Firestore en tiempo real
            if fs:
                try:
                    subir_a_firestore(fs, nuevo_id, genero, msg.topic, msg.partition, msg.offset, fecha_envio)
                    print(f"   ✔ [Firestore] Documento creado en: {FIRESTORE_COLLECTION}/{nuevo_id}")
                except Exception as e:
                    print(f"   ✖ [Firestore Error]: {e}")
            else:
                print(f"   ⚠ [Firestore] Omitido: No hay conexión con Firestore activa.")

    except Exception as e:
        print(f"\n[ERROR EN CONSUMIDOR]: {e}")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
