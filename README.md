# Géneros + Kafka + PostgreSQL

Programa en Python que pide el género (F/M), muestra el broker de Kafka, envía el mensaje al topic `generos` y lo guarda en PostgreSQL. Todo corre en Docker.

## Ejecutar

```bash
docker compose up -d postgres kafka
docker compose run --rm app
```

Para ver lo guardado en la base de datos:

```bash
docker compose exec postgres psql -U postgres -d generosdb -c "SELECT * FROM generos;"
```

Para apagar todo:

```bash
docker compose down        # conserva los datos
docker compose down -v     # borra también los datos
```
