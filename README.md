# Géneros + Kafka + PostgreSQL + Firebase

Arquitectura de mensajería desacoplada usando Apache Kafka, PostgreSQL y Firebase Firestore.

## Estructura de la Arquitectura

- **Productor (`producer.py`)**: Interfaz interactiva por consola. Muestra géneros sugeridos (`Femenino`, `Masculino`, `No binario`, `Otro`, `Prefiero no decirlo`) y permite elegir uno por número o **escribir cualquier otro género libremente**. Envía el evento al topic de Kafka `generos`.
- **Broker (Apache Kafka)**: Recibe y almacena los mensajes de los eventos en el topic `generos`.
- **Consumidor (`consumer.py`)**: Servicio en segundo plano. Escucha continuamente el topic `generos` y, por cada evento recibido, lo guarda en **PostgreSQL** y en **Firebase Firestore**.

---

## Cómo Ejecutar con Docker

1. **Colocar la llave de Firebase:**
   Descarga la clave de servicio de Firebase / Google Cloud, nómbrala `firebase-key.json` y colócala en esta carpeta raíz del proyecto.

2. **Levantar la infraestructura y el Consumidor:**
   ```bash
   docker compose up -d postgres kafka consumer
   ```

3. **Ver los logs del Consumidor en tiempo real:**
   ```bash
   docker compose logs -f consumer
   ```

4. **Ejecutar el Productor interactivo:**
   En otra terminal:
   ```bash
   docker compose run --rm producer
   ```
   Verás las opciones sugeridas: puedes escribir el número de una opción o teclear cualquier otro género (por ejemplo, `Binario`). Para salir, escribe `S`.
   Las opciones se pueden cambiar desde `.env` con `GENEROS_SUGERIDOS=Opcion1,Opcion2,Opcion3`.

5. **Consultar los registros guardados en PostgreSQL:**
   ```bash
   docker compose exec postgres psql -U postgres -d generosdb -c "SELECT * FROM generos;"
   ```

6. **Detener los servicios:**
   ```bash
   docker compose down        # Conserva datos
   docker compose down -v     # Borra datos de los volúmenes
   ```

---

## Cómo Ejecutar en Local (Sin Docker)

1. Tener activos Kafka en `localhost:29092` y PostgreSQL en `localhost:5433`.
2. Definir la variable de Firebase:
   - En PowerShell:
     ```powershell
     $env:GOOGLE_APPLICATION_CREDENTIALS = "$PWD\firebase-key.json"
     ```
3. En una terminal, ejecutar el consumidor:
   ```powershell
   python consumer.py
   ```
4. En otra terminal, ejecutar el productor:
   ```powershell
   python producer.py
   ```
