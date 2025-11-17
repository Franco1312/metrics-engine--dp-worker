# Flujo del Sistema - Metrics Worker

## Punto de Entrada

**Archivo**: `metrics_worker/infrastructure/runtime/main.py`
- **Función**: `main()` → `main_loop()`
- **Inicio**: Al ejecutar el worker (`python -m metrics_worker.infrastructure.runtime.main`)

## Flujo Completo

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. INICIALIZACIÓN (main_loop)                                   │
│    - Configurar logging (structlog)                             │
│    - Cargar Settings (Pydantic)                                 │
│    - Iniciar servidor de métricas (Prometheus)                 │
│    - Crear adaptadores:                                            │
│      • S3IO → S3CatalogAdapter (catalog)                         │
│      • ParquetReader (data_reader)                               │
│      • JsonlWriter (output_writer)                               │
│      • SNSPublisher (event_bus)                                  │
│      • SystemClock (clock)                                       │
│      • SQSConsumer (sqs_consumer)                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. LOOP PRINCIPAL (while not shutdown_event)                     │
│                                                                  │
│    ┌──────────────────────────────────────────────────────────┐ │
│    │ 2.1. RECEPCIÓN DE MENSAJE                                │ │
│    │     SQSConsumer.receive_message()                         │ │
│    │     ├─ SQS long-polling (WaitTimeSeconds=20)             │ │
│    │     ├─ Parsear Body JSON                                  │ │
│    │     ├─ Detectar si es SNS-wrapped o directo              │ │
│    │     ├─ Extraer MessageAttributes (type, metricCode)      │ │
│    │     └─ Validar y crear MetricRunRequestedEvent           │ │
│    └──────────────────────────────────────────────────────────┘ │
│                              │                                   │
│                              ▼                                   │
│    ┌──────────────────────────────────────────────────────────┐ │
│    │ 2.2. IDEMPOTENCIA                                        │ │
│    │     JsonlWriter.check_run_marker()                       │ │
│    │     ├─ Verificar S3: metrics/{code}/runs/{runId}.ok     │ │
│    │     └─ Si existe → skip y delete message                 │ │
│    └──────────────────────────────────────────────────────────┘ │
│                              │                                   │
│                              ▼                                   │
│    ┌──────────────────────────────────────────────────────────┐ │
│    │ 2.3. PROCESAMIENTO PRINCIPAL                              │ │
│    │     handle_run_request.run()                              │ │
│    │                                                            │ │
│    │     ┌──────────────────────────────────────────────────┐ │ │
│    │     │ 2.3.1. PUBLICAR EVENTO INICIO                    │ │ │
│    │     │     publish_started()                             │ │ │
│    │     │     └─ SNSPublisher.publish_started()             │ │ │
│    │     │        └─ SNS Topic: metric_run_started           │ │ │
│    │     └──────────────────────────────────────────────────┘ │ │
│    │                       │                                   │ │
│    │                       ▼                                   │ │
│    │     ┌──────────────────────────────────────────────────┐ │ │
│    │     │ 2.3.2. PLANIFICAR LECTURAS                       │ │ │
│    │     │     planner.plan_reads()                          │ │ │
│    │     │     ├─ Analizar expression_json                   │ │ │
│    │     │     ├─ Identificar series requeridas              │ │ │
│    │     │     └─ Determinar columnas y filtros              │ │ │
│    │     └──────────────────────────────────────────────────┘ │ │
│    │                       │                                   │ │
│    │                       ▼                                   │ │
│    │     ┌──────────────────────────────────────────────────┐ │ │
│    │     │ 2.3.3. LEER DATOS                                │ │ │
│    │     │     Para cada input:                              │ │ │
│    │     │     ├─ Catalog.get_dataset_manifest()             │ │ │
│    │     │     │  └─ S3CatalogAdapter → S3IO.get_json()    │ │ │
│    │     │     ├─ Extraer parquet_files del manifest        │ │ │
│    │     │     ├─ Construir paths: projectionsPath +        │ │ │
│    │     │     │  parquet_file_path                          │ │ │
│    │     │     └─ ParquetReader.read_series_from_paths()    │ │ │
│    │     │        └─ PyArrow Dataset:                        │ │ │
│    │     │           • Column pruning (obs_time, value)      │ │ │
│    │     │           • Predicate pushdown (series_code)      │ │ │
│    │     │           • Leer desde S3                         │ │ │
│    │     └──────────────────────────────────────────────────┘ │ │
│    │                       │                                   │ │
│    │                       ▼                                   │ │
│    │     ┌──────────────────────────────────────────────────┐ │ │
│    │     │ 2.3.4. EVALUAR EXPRESIÓN                         │ │ │
│    │     │     expression_eval.evaluate_expression()          │ │ │
│    │     │     ├─ series_math: add/subtract/multiply/ratio   │ │ │
│    │     │     ├─ window_op: sma/ema/sum/max/min/lag          │ │ │
│    │     │     └─ composite: sum/avg/max/min multi-serie      │ │ │
│    │     └──────────────────────────────────────────────────┘ │ │
│    │                       │                                   │ │
│    │                       ▼                                   │ │
│    │     ┌──────────────────────────────────────────────────┐ │ │
│    │     │ 2.3.5. ESCRIBIR RESULTADOS                       │ │ │
│    │     │     ├─ Clock.format_version_ts()                 │ │ │
│    │     │     ├─ JsonlWriter.write_jsonl()                  │ │ │
│    │     │     │  └─ S3: metrics/{code}/{versionTs}/data/   │ │ │
│    │     │     ├─ build_output_manifest()                    │ │ │
│    │     │     ├─ JsonlWriter.write_manifest()                │ │ │
│    │     │     │  ├─ metrics/{code}/{versionTs}/manifest.json│ │ │
│    │     │     │  └─ metrics/{code}/current/manifest.json   │ │ │
│    │     │     └─ JsonlWriter.create_run_marker()            │ │ │
│    │     │        └─ metrics/{code}/runs/{runId}.ok         │ │ │
│    │     └──────────────────────────────────────────────────┘ │ │
│    │                       │                                   │ │
│    │                       ▼                                   │ │
│    │     ┌──────────────────────────────────────────────────┐ │ │
│    │     │ 2.3.6. VALIDAR MANIFEST                          │ │ │
│    │     │     validate_output_manifest()                    │ │ │
│    │     │     ├─ run_id === runId                           │ │ │
│    │     │     ├─ metric_code === metricCode                 │ │ │
│    │     │     ├─ version_ts presente                        │ │ │
│    │     │     └─ row_count >= 0                             │ │ │
│    │     └──────────────────────────────────────────────────┘ │ │
│    │                       │                                   │ │
│    │                       ▼                                   │ │
│    │     ┌──────────────────────────────────────────────────┐ │ │
│    │     │ 2.3.7. PUBLICAR EVENTO COMPLETADO                │ │ │
│    │     │     publish_completed.run_success()                │ │ │
│    │     │     └─ SNSPublisher.publish_completed()           │ │ │
│    │     │        └─ SNS Topic: metric_run_completed          │ │ │
│    │     └──────────────────────────────────────────────────┘ │ │
│    └──────────────────────────────────────────────────────────┘ │
│                              │                                   │
│                              ▼                                   │
│    ┌──────────────────────────────────────────────────────────┐ │
│    │ 2.4. FINALIZACIÓN                                        │ │
│    │     ├─ Incrementar métrica: runs_succeeded               │ │
│    │     └─ SQSConsumer.delete_message()                      │ │
│    └──────────────────────────────────────────────────────────┘ │
│                                                                  │
│    [Si hay error]                                               │
│    ├─ Incrementar métrica: runs_failed                          │
│    ├─ Log error con exc_info                                    │
│    └─ publish_completed.run_failure() → SNS                    │
└─────────────────────────────────────────────────────────────────┘
```

## Capas de Arquitectura

### 1. **Interfaces** (Punto de Entrada)
- `main.py`: Loop principal y orquestación
- `sqs_run_worker.py`: Adaptador SQS → Use Cases (no usado actualmente)

### 2. **Application** (Lógica de Negocio)
- **Use Cases**:
  - `handle_run_request.py`: Orquestación principal
  - `publish_started.py`: Publicar inicio
  - `publish_heartbeat.py`: Publicar progreso (pendiente implementar)
  - `publish_completed.py`: Publicar finalización
  - `build_output_manifest.py`: Construir manifest
  - `validate_output_manifest.py`: Validar manifest

- **Services**:
  - `expression_eval.py`: Evaluación de expresiones
  - `window_ops.py`: Operaciones de ventana
  - `planner.py`: Planificación de lecturas

- **DTOs**:
  - `events.py`: Modelos de eventos (Pydantic)
  - `catalog.py`: Modelos de catálogo (Pydantic)

### 3. **Domain** (Entidades y Contratos)
- `entities.py`: Entidades de dominio
- `ports.py`: Interfaces (abstracciones)
- `errors.py`: Errores de dominio
- `types.py`: Tipos y aliases

### 4. **Infrastructure** (Implementaciones)
- **AWS**:
  - `sqs_consumer.py`: Consumidor SQS
  - `sns_publisher.py`: Publicador SNS
  - `s3_io.py`: Operaciones S3

- **IO**:
  - `parquet_reader.py`: Lector Parquet (PyArrow)
  - `jsonl_writer.py`: Escritor JSONL

- **Runtime**:
  - `catalog_adapter.py`: Adaptador de catálogo
  - `clock.py`: Reloj del sistema
  - `health.py`: Servidor de métricas

- **Config**:
  - `settings.py`: Configuración (Pydantic Settings)

- **Observability**:
  - `logging.py`: Configuración de logging
  - `metrics.py`: Métricas Prometheus

## Flujo de Datos

1. **SQS** → `SQSConsumer` → `MetricRunRequestedEvent` (DTO)
2. **Use Case** → `CatalogPort` → `S3CatalogAdapter` → `S3IO` → **S3** (manifest)
3. **Use Case** → `DataReaderPort` → `ParquetReader` → **S3** (Parquet)
4. **Use Case** → `ExpressionEval` → **Resultado** (DataFrame)
5. **Use Case** → `OutputWriterPort` → `JsonlWriter` → **S3** (JSONL + manifest)
6. **Use Case** → `EventBusPort` → `SNSPublisher` → **SNS** (eventos)

## Eventos Publicados

1. **metric_run_started**: Al inicio del procesamiento
2. **metric_run_heartbeat**: Durante el procesamiento (pendiente)
3. **metric_run_completed**: Al finalizar (SUCCESS o FAILURE)

## Idempotencia

- **Marcador S3**: `metrics/{metricCode}/runs/{runId}.ok`
- **Verificación**: Antes de procesar, verificar si existe el marcador
- **Creación**: Después de escribir manifest y validar, crear marcador
- **Reintento**: Si el marcador existe, publicar SUCCESS sin reprocesar

