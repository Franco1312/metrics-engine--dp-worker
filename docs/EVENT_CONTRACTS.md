# Contratos de Eventos - Control Plane → Data Plane

Este documento describe los contratos de eventos que el **Control Plane** publica a través de **SNS** para que el **Data Plane** (worker) los consuma y ejecute.

## Evento: `metric_run_requested`

**Dirección**: Control Plane → Data Plane  
**Canal**: SNS Topic (FIFO o Standard) → SQS Queue  
**Propósito**: Solicitar la ejecución de una métrica

### Estructura del Evento

```json
{
  "type": "metric_run_requested",
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "metricCode": "ratio.reserves_to_base",
  "expressionType": "series_math",
  "expressionJson": {
    "op": "ratio",
    "left": { "series_code": "BCRA_RESERVAS_USD_M_D" },
    "right": { "series_code": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D" },
    "scale": 1
  },
  "inputs": [
    {
      "datasetId": "bcra_infomondia_series",
      "seriesCode": "BCRA_RESERVAS_USD_M_D"
    },
    {
      "datasetId": "bcra_infomondia_series",
      "seriesCode": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D"
    }
  ],
  "catalog": {
    "datasets": {
      "bcra_infomondia_series": {
        "manifestPath": "bcra_infomondia_series/current/manifest.json"
      }
    }
  },
  "output": {
    "basePath": "s3://bucket-name/metrics/ratio.reserves_to_base/"
  }
}
```

### Campos Requeridos

| Campo | Tipo | Descripción | Requerido |
|-------|------|-------------|-----------|
| `type` | `string` | Siempre `"metric_run_requested"` | ✅ Sí |
| `runId` | `string` (UUID) | Identificador único del run generado por el control plane | ✅ Sí |
| `metricCode` | `string` | Código de la métrica (ej: `"ratio.reserves_to_base"`) | ✅ Sí |
| `expressionType` | `string` | Tipo de expresión: `"series_math"`, `"window_op"`, o `"composite"` | ✅ Sí |
| `expressionJson` | `object` | Expresión de la métrica validada | ✅ Sí |
| `inputs` | `array` | Lista de inputs (datasetId + seriesCode) requeridos | ✅ Sí |
| `catalog` | `object` | Catálogo de manifest paths por dataset | ✅ Sí |
| `output` | `object` | Configuración de salida (basePath en S3) | ✅ Sí |

### Campos Opcionales

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `schema` | `string` | Schema version (opcional) |
| `messageGroupId` | `string` | Solo para topics FIFO. Usa el `runId` |
| `messageDeduplicationId` | `string` | Solo para topics FIFO. Formato: `"{runId}:{type}"` |

### MessageAttributes (SNS)

El evento incluye los siguientes MessageAttributes cuando se publica a SNS:

```json
{
  "type": {
    "DataType": "String",
    "StringValue": "metric_run_requested"
  },
  "metricCode": {
    "DataType": "String",
    "StringValue": "ratio.reserves_to_base"
  }
}
```

**Uso**: Los MessageAttributes permiten filtrar mensajes en SNS subscriptions sin necesidad de parsear el body completo. El worker usa estos atributos para validar el tipo de evento.

## Tipos de Expresiones

### 1. Series Math Expression (`expressionType: "series_math"`)

Operaciones binarias entre dos series o expresiones anidadas.

#### Operaciones Soportadas

| `op` | Descripción | Fórmula |
|------|-------------|---------|
| `"ratio"` | División | `left / right` |
| `"multiply"` | Multiplicación | `left * right` |
| `"subtract"` | Sustracción | `left - right` |
| `"add"` | Suma | `left + right` |

#### Campos

- `op` (string, requerido): Tipo de operación
- `left` (object, requerido): Serie o expresión anidada
- `right` (object, requerido): Serie o expresión anidada
- `scale` (number, opcional): Factor de escala (multiplica el resultado)

### 2. Window Operation Expression (`expressionType: "window_op"`)

Operaciones de ventana sobre una serie.

#### Operaciones Soportadas

| `op` | Descripción |
|------|-------------|
| `"sma"` | Simple Moving Average (promedio móvil simple) |
| `"ema"` | Exponential Moving Average (promedio móvil exponencial) |
| `"sum"` | Suma de valores en la ventana |
| `"max"` | Valor máximo en la ventana |
| `"min"` | Valor mínimo en la ventana |
| `"lag"` | Lag operation - retorna el valor de `window` períodos atrás |

#### Campos

- `op` (string, requerido): Tipo de operación
- `series` (object, requerido): Serie o expresión anidada
- `window` (number, requerido): Tamaño de la ventana en períodos (entero positivo >= 1)

### 3. Composite Expression (`expressionType: "composite"`)

Agregaciones sobre múltiples series.

#### Operaciones Soportadas

| `op` | Descripción |
|------|-------------|
| `"sum"` | Suma de todos los valores de las series en cada timestamp |
| `"avg"` | Promedio de todos los valores de las series en cada timestamp |
| `"max"` | Valor máximo entre todas las series en cada timestamp |
| `"min"` | Valor mínimo entre todas las series en cada timestamp |

#### Campos

- `op` (string, requerido): Tipo de operación
- `operands` (array, requerido): Array de referencias a series (mínimo 2)

## Expresiones Anidadas

Tanto `left` y `right` en `series_math`, como `series` en `window_op`, pueden ser expresiones anidadas.

### Ejemplo: Series Math con Window Operation Anidada

```json
{
  "op": "ratio",
  "left": {
    "op": "subtract",
    "left": { "series_code": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D" },
    "right": {
      "op": "lag",
      "series": { "series_code": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D" },
      "window": 30
    }
  },
  "right": {
    "op": "lag",
    "series": { "series_code": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D" },
    "window": 30
  },
  "scale": 1
}
```

## Parsing del Mensaje SNS → SQS

Cuando el mensaje llega a través de SNS → SQS, el formato es:

```json
{
  "Type": "Notification",
  "MessageId": "uuid-sns",
  "TopicArn": "arn:aws:sns:us-east-1:123:metric-run-request.fifo",
  "Message": "{...evento JSON...}",
  "MessageAttributes": {
    "type": { "Type": "String", "Value": "metric_run_requested" },
    "metricCode": { "Type": "String", "Value": "ratio.reserves_to_base" }
  },
  "Timestamp": "2025-01-15T10:30:05Z"
}
```

El worker:
1. Detecta si `Type == "Notification"` (mensaje de SNS)
2. Parsea `Message` como JSON
3. Usa `MessageAttributes` para validar `type` y `metricCode`
4. Crea el `MetricRunRequestedEvent` con los datos parseados

## Validaciones del Worker

El worker valida:

1. ✅ El evento tiene `type == "metric_run_requested"`
2. ✅ Todos los campos requeridos están presentes
3. ✅ La expresión es válida y puede ejecutarse
4. ✅ Los inputs referencian series disponibles
5. ✅ El manifest del dataset existe en S3

## Ejemplos Completos

Ver `tests/unit/test_event_parsing.py` para ejemplos completos de cada tipo de expresión.


