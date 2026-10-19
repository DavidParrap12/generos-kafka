import json
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaAdminClient, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Cargar variables de entorno desde .env
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "generos")


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


def esperar(nombre, fn, intentos=30, pausa=2):
    """Reintenta la conexión hasta que Kafka esté listo."""
    for i in range(1, intentos + 1):
        try:
            return fn()
        except (NoBrokersAvailable, ValueError, OSError) as e:
            print(f"Esperando {nombre} ({i}/{intentos})... ({type(e).__name__})")
            time.sleep(pausa)
    print(f"No se pudo conectar a {nombre}.")
    sys.exit(1)


def mostrar_broker(admin):
    info = admin.describe_cluster()
    print("\n--- INFORMACIÓN DEL BROKER KAFKA ---")
    print(f"Bootstrap Server : {KAFKA_BOOTSTRAP}")
    print(f"Cluster ID       : {info.get('cluster_id')}")
    for b in info.get("brokers", []):
        print(f"Broker Activo    : id={b['node_id']} {b['host']}:{b['port']}")
    print(f"Topic Destino    : {KAFKA_TOPIC}")
    print("------------------------------------\n")


def pedir_genero():
    while True:
        valor = input("Ingrese el género que desee (o digite 'S' para salir): ").strip()
        if not valor:
            print("El texto no puede estar vacío. Por favor ingrese un género.")
            continue
        if valor.upper() == "S":
            return None
        return valor


def main():
    print("=" * 65)
    print(" PRODUCTOR DE KAFKA (ENTRADA LIBRE DE GÉNERO) ".center(65, "="))
    print("=" * 65)

    admin, producer = esperar("Kafka", conectar_kafka)
    mostrar_broker(admin)

    while True:
        genero = pedir_genero()
        if genero is None:
            print("\nSaliendo del productor...")
            break

        mensaje = {
            "genero": genero,
            "fecha": datetime.now(timezone.utc).isoformat(),
        }

        # Enviar mensaje al topic de Kafka
        meta = producer.send(KAFKA_TOPIC, mensaje).get(timeout=15)
        print(f"\n✔ Evento publicado a Kafka en streaming:")
        print(f"   Topic: {meta.topic} | Partición: {meta.partition} | Offset: {meta.offset}")
        print(f"   Datos: {mensaje}\n")

    producer.close()
    admin.close()


if __name__ == "__main__":
    main()
