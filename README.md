# Microservicios con Kafka y Django

Este ejemplo implementa 5 microservicios:
- `ordering_service` (Django)
- `billing_service` (Django)
- `shipping_service` (Django)
- `inventory_service` (Django)
- `notification_service` (Django)

Se comunican con Kafka usando los tópicos:
- `orders`
- `payments`
- `shipments`

Bases de datos:
- `commercial_db`: compartida por `ordering_service` y `billing_service`
- `logistics_db`: compartida por `shipping_service` e `inventory_service`

## Despliegue en Nube

### Configuración de Servicios Externos

1. **Confluent Cloud Kafka**:
   - Crear cluster en [confluent.cloud](https://confluent.cloud)
   - Crear tópicos: `orders`, `payments`, `shipments`
   - Generar API Key y Secret

2. **PostgreSQL en Nube**:
   - Usar RDS (AWS), Cloud SQL (GCP), Azure Database, etc.
   - Crear 2 bases de datos: `commercial_db` y `logistics_db`

3. **SendGrid para Emails**:
   - Crear cuenta en [sendgrid.com](https://sendgrid.com)
   - Generar API Key
   - Verificar email remitente

### Variables de Entorno

Copiar `.env.example` a `.env` y configurar con valores reales:

```bash
cp .env.example .env
```

Variables requeridas:
- `KAFKA_BOOTSTRAP_SERVERS`: URL del cluster Confluent
- `KAFKA_API_KEY` y `KAFKA_API_SECRET`: Credenciales Confluent
- `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`: DB en nube
- `SMTP_PASSWORD`: API Key de SendGrid
- `EMAIL_FROM`: Email verificado en SendGrid

### Desplegar

```bash
# Construir y ejecutar servicios
docker-compose up --build

# Ejecutar consumidores en background
docker-compose exec billing_service python manage.py run_billing_consumer &
docker-compose exec shipping_service python manage.py run_shipping_consumer &
docker-compose exec inventory_service python manage.py run_inventory_consumer &
docker-compose exec notification_service python manage.py run_notification_consumer &
```

## Desarrollo Local

Para desarrollo local con Kafka local, descomentar las secciones `zookeeper` y `kafka` en `docker-compose.yml` y usar `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`.

```bash
docker compose up --build
```

## Productos disponibles
- `101` - Camiseta Azul
- `102` - Zapatos Running
- `103` - Mochila Outdoor

## Clientes parametrizados
- `customer_id`: 1, email: `danielsafo@unisabana.edu.co`
- `customer_id`: 2, email: `daniel.saavedra.fon@gmail.com`

## Ejemplo de orden
POST `http://localhost:8001/api/orders/`

```json
{
  "customer_id": 1,
  "product_id": 101,
  "quantity": 2
}
```

## Flujo de Eventos

1. Orden creada → `OrderCreated` en `orders`
2. Stock reservado → `StockReserved` (consumido por shipping)
3. Pago procesado → `PaymentProcessed` en `payments`
4. Envío generado → `ShipmentCreated` en `shipments`
5. Notificaciones por email para cada evento

## Notas de Seguridad

- Nunca commitear `.env` con secrets reales
- Usar variables de entorno para toda configuración
- Configurar VPCs y security groups en nube
```

## Comportamiento
1. `Ordering` crea la orden en `commercial_db` y publica `OrderCreated` en `orders`
2. `Billing` procesa el pago y publica `PaymentProcessed` en `payments`
3. `Inventory` valida stock y reserva unidades en `logistics_db`, luego publica `StockReserved` en `shipments`
4. `Shipping` genera el envío tras validar pago y disponibilidad, publica `ShipmentCreated` en `shipments`
5. `Notification` escucha todos los eventos (`orders`, `payments`, `shipments`), consulta el email del pedido y envía correos reales al usuario
