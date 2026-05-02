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

## Levantar el sistema

Antes de ejecutar, configure las variables SMTP en el entorno o en `docker-compose.yml`:
- `SMTP_HOST` (por ejemplo `smtp.gmail.com`)
- `SMTP_PORT` (por ejemplo `587`)
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_USE_TLS` (`True` o `False`)
- `EMAIL_FROM`

```bash
docker compose up --build
```

## Comportamiento
1. `Ordering` crea la orden en `commercial_db` y publica `OrderCreated` en `orders`
2. `Billing` procesa el pago y publica `PaymentProcessed` en `payments`
3. `Inventory` valida stock y reserva unidades en `logistics_db`, luego publica `StockReserved` en `shipments`
4. `Shipping` genera el envío tras validar pago y disponibilidad, publica `ShipmentCreated` en `shipments`
5. `Notification` escucha todos los eventos (`orders`, `payments`, `shipments`), consulta el email del pedido y envía correos reales al usuario
