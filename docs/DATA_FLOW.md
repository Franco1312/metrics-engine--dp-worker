# Flujo de Datos y Paths en el Worker

Este documento explica cómo el worker recibe, procesa y utiliza los paths para leer datos desde S3.

## 1. Recepción del Evento

### 1.1 Evento `metric_run_requested`

El worker recibe eventos desde SQS con la siguiente estructura:

```json
{
  "type": "metric_run_requested",
  "runId": "...",
  "metricCode": "...",
  "expressionType": "...",
  "expressionJson": {...},
  "inputs": [...],
  "catalog": {
    "datasets": {
      "dataset_id": {
        "manifestPath": "datasets/bcra_infomondia_series/current/manifest.json",
        "projectionsPath": "projections/bcra_infomondia_series"
      }
    }
  },
  "output": {
    "basePath": "s3://bucket/metrics/metric_code/"
  }
}
```

**Ubicación del código:**
- `metrics_worker/application/dto/events.py` - Definición del evento
- `metrics_worker/infrastructure/aws/sqs_consumer.py` - Recepción y parsing
- `metrics_worker/infrastructure/runtime/main.py` - Loop principal

### 1.2 Paths Recibidos

Del evento se extraen dos paths importantes por cada dataset:

1. **`manifestPath`**: Ruta relativa al manifest del dataset en S3
   - Ejemplo: `"datasets/bcra_infomondia_series/current/manifest.json"`
   - Se usa para leer la metadata del dataset

2. **`projectionsPath`**: Ruta base donde están las proyecciones
   - Ejemplo: `"datasets/bcra_infomondia_series/projections"` (las proyecciones están dentro de `datasets/`)
   - Se combina con los `parquet_files` del manifest para construir paths completos

**Ubicación del código:**
- `metrics_worker/domain/types.py` - `DatasetInfo` con `manifestPath` y `projectionsPath`
- `metrics_worker/application/use_cases/handle_run_request.py` - Líneas 124-126

## 2. Lectura del Manifest

### 2.1 Proceso de Lectura

1. **Extracción del manifestPath del evento:**
   ```python
   dataset_info = catalog_info["datasets"][dataset_id]
   manifest_path = dataset_info["manifestPath"]  # "datasets/.../manifest.json"
   ```

2. **Lectura desde S3:**
   ```python
   manifest_dict = await catalog.get_dataset_manifest(manifest_path)
   ```
   - Usa `S3CatalogAdapter.get_dataset_manifest()`
   - Internamente llama a `S3IO.get_json(manifest_path)`
   - Construye: `s3://{bucket}/{manifest_path}`

3. **Parsing del manifest:**
   ```python
   dataset_manifest = DatasetManifest(**manifest_dict)
   ```

**Ubicación del código:**
- `metrics_worker/infrastructure/runtime/catalog_adapter.py` - Líneas 20-49
- `metrics_worker/infrastructure/aws/s3_io.py` - Líneas 22-30
- `metrics_worker/application/dto/catalog.py` - Modelo `DatasetManifest`

### 2.2 Estructura del Manifest

El manifest tiene la siguiente estructura:

```json
{
  "version_id": "v20251111_014138_730866",
  "dataset_id": "bcra_infomondia_series",
  "created_at": "2025-11-11T05:13:32Z",
  "collection_date": "2025-11-11T04:41:38Z",
  "data_points_count": 82257,
  "series_count": 17,
  "series_codes": ["BCRA_BADLAR_PRIVADOS_TNA_D", ...],
  "date_range": {
    "min_obs_time": "2002-12-31T00:00:00Z",
    "max_obs_time": "2025-11-07T00:00:00Z"
  },
  "parquet_files": [
    "BCRA_DEP_PRIVADOS_PLAZO_ARS_BN_D/year=2025/month=11/data.parquet"
  ],
  "partitions": [
    "BCRA_BADLAR_PRIVADOS_TNA_D/year=2003/month=01/"
  ],
  "partition_strategy": "series_year_month"
}
```

**Campos importantes:**
- `parquet_files`: Lista de paths relativos a los archivos parquet
- `series_codes`: Lista de series disponibles en el dataset

## 3. Construcción de Paths de Proyecciones

### 3.1 Filtrado de Parquet Files

Se filtran los `parquet_files` que corresponden a las series necesarias:

```python
affected_parquet_files = _filter_parquet_files_for_series(
    dataset_manifest.parquet_files,
    series_codes,  # Series que necesitamos leer
)
```

**Lógica de filtrado:**
- Para cada `parquet_file` en el manifest
- Si el `series_code` aparece en el path del `parquet_file`, se incluye
- Ejemplo: `"BCRA_DEP_PRIVADOS_PLAZO_ARS_BN_D/year=2025/month=11/data.parquet"` contiene `"BCRA_DEP_PRIVADOS_PLAZO_ARS_BN_D"`

**Ubicación del código:**
- `metrics_worker/application/use_cases/handle_run_request.py` - Líneas 144-147, 202-212

### 3.2 Construcción de Paths Completos

Para cada serie, se construyen los paths completos:

```python
# 1. Filtrar parquet_files para esta serie específica
series_parquet_files = [
    pf for pf in affected_parquet_files
    if series_code in pf
]

# 2. Construir paths completos: projectionsPath + parquet_file_path
full_paths = [
    S3Path.join(projections_path, parquet_file)
    for parquet_file in series_parquet_files
]
```

**Ejemplo:**
- `projectionsPath`: `"datasets/bcra_infomondia_series/projections"`
- `parquet_file`: `"BCRA_DEP_PRIVADOS_PLAZO_ARS_BN_D/year=2025/month=11/data.parquet"`
- **Path completo**: `"datasets/bcra_infomondia_series/projections/BCRA_DEP_PRIVADOS_PLAZO_ARS_BN_D/year=2025/month=11/data.parquet"`
- **Path S3 completo**: `"s3://bucket/datasets/bcra_infomondia_series/projections/BCRA_DEP_PRIVADOS_PLAZO_ARS_BN_D/year=2025/month=11/data.parquet"`

**Ubicación del código:**
- `metrics_worker/application/use_cases/handle_run_request.py` - Líneas 159-174
- `metrics_worker/infrastructure/aws/s3_path.py` - `S3Path.join()`

## 4. Lectura de Datos desde Proyecciones

### 4.1 Proceso de Lectura

1. **Llamada al reader:**
   ```python
   series_df = await data_reader.read_series_from_paths(
       full_paths,  # Lista de paths completos
       series_code,
   )
   ```

2. **Conversión a paths S3 completos:**
   ```python
   full_s3_paths = [
       S3Path.to_full_path(self.bucket, path) 
       for path in parquet_paths
   ]
   # Resultado: ["s3://bucket/datasets/.../projections/.../data.parquet", ...]
   # Nota: Las proyecciones están dentro de datasets/, no en un path separado
   ```

3. **Lectura con PyArrow:**
   ```python
   dataset = ds.dataset(full_s3_paths, format="parquet", filesystem=S3FileSystem())
   scanner = dataset.scanner(
       columns=["obs_time", "value", "internal_series_code"],
       filter=ds.field("internal_series_code") == series_code,
   )
   table = scanner.to_table()
   ```

**Ubicación del código:**
- `metrics_worker/infrastructure/io/parquet_reader.py` - Líneas 102-188
- `metrics_worker/infrastructure/aws/s3_path.py` - `S3Path.to_full_path()`

## 5. Configuración del Bucket

### 5.1 Dónde se Configura

El bucket se configura mediante la variable de entorno `AWS_S3_BUCKET`:

1. **Archivo `.env`** (recomendado):
   ```bash
   AWS_S3_BUCKET=ingestor-datasets
   ```

2. **Script `run_local.sh`** (valor por defecto):
   ```bash
   export AWS_S3_BUCKET="${AWS_S3_BUCKET:-test-bucket}"
   ```

3. **Variable de entorno del sistema** (tiene prioridad)

### 5.2 Dónde se Usa

El bucket se usa en todas las operaciones de S3:

- **S3IO**: Todas las operaciones (`get_json`, `put_json`, `put_object`)
- **S3CatalogAdapter**: Lectura de manifests
- **ParquetReader**: Construcción de paths S3 completos
- **ParquetWriter**: Escritura de resultados

**Ubicación del código:**
- `metrics_worker/infrastructure/config/settings.py` - Línea 10
- `metrics_worker/infrastructure/aws/s3_io.py` - Línea 20

## 6. Puntos de Falla y Diagnóstico

### 6.1 Problema: Manifest No Encontrado

**Error:**
```
NoSuchKey: The specified key does not exist
RuntimeError: Failed to read S3 object datasets/bcra_infomondia_series/current/manifest.json
```

**Posibles causas:**
1. **Bucket incorrecto**: Verificar `AWS_S3_BUCKET` en `.env` o logs
2. **Path incorrecto**: El `manifestPath` en el evento no existe en S3
3. **Permisos**: Las credenciales no tienen acceso al bucket/path

**Diagnóstico:**
- Revisar logs: `"getting_manifest"` muestra `manifest_path` y `bucket`
- Verificar que el bucket sea correcto: `"settings_loaded"` muestra el bucket
- Verificar que el path existe en S3: `aws s3 ls s3://bucket/datasets/.../`

**Ubicación del código:**
- `metrics_worker/infrastructure/runtime/catalog_adapter.py` - Líneas 20-49
- `metrics_worker/infrastructure/aws/s3_io.py` - Líneas 22-30

### 6.2 Problema: Parquet Files No Encontrados

**Error:**
```
ValueError: No parquet files found for series BCRA_XXX in dataset YYY
```

**Posibles causas:**
1. **Filtrado incorrecto**: El `series_code` no aparece en ningún `parquet_file` del manifest
2. **Manifest desactualizado**: Los `parquet_files` en el manifest no coinciden con los archivos reales
3. **Paths incorrectos**: Los `parquet_files` en el manifest tienen formato incorrecto

**Diagnóstico:**
- Revisar logs: `"reading_series_from_dataset"` muestra `parquet_files_count`
- Verificar `parquet_files` en el manifest
- Verificar que los paths en `parquet_files` contengan el `series_code`

**Ubicación del código:**
- `metrics_worker/application/use_cases/handle_run_request.py` - Líneas 144-147, 159-168

### 6.3 Problema: Paths de Proyecciones Incorrectos

**Error:**
```
OSError: Failed to open dataset at s3://bucket/projections/.../data.parquet
```

**Posibles causas:**
1. **projectionsPath incorrecto**: El path base no existe en S3
2. **Combinación incorrecta**: `projectionsPath + parquet_file` no coincide con la estructura real
3. **Archivos no existen**: Los archivos parquet no están en la ubicación esperada

**Diagnóstico:**
- Revisar logs: `"reading_series_from_paths"` muestra `parquet_paths` y `full_s3_paths`
- Verificar estructura en S3: `aws s3 ls s3://bucket/projections/.../`
- Verificar que `projectionsPath` del evento sea correcto

**Ubicación del código:**
- `metrics_worker/application/use_cases/handle_run_request.py` - Líneas 170-174
- `metrics_worker/infrastructure/io/parquet_reader.py` - Líneas 102-142

### 6.4 Problema: Credenciales Incorrectas

**Error:**
```
InvalidClientTokenId: No account found for the given parameters
```

**Posibles causas:**
1. **Credenciales faltantes**: `AWS_ACCESS_KEY_ID` o `AWS_SECRET_ACCESS_KEY` no configuradas
2. **Credenciales inválidas**: Las credenciales están expiradas o son incorrectas
3. **Permisos insuficientes**: Las credenciales no tienen permisos para leer del bucket

**Diagnóstico:**
- Revisar logs: `"settings_loaded"` muestra `has_access_key` y `has_secret_key`
- Verificar `.env` tiene las credenciales correctas
- Verificar permisos IAM de las credenciales

**Ubicación del código:**
- `metrics_worker/infrastructure/runtime/main.py` - Líneas 44-76

## 7. Flujo Completo Visualizado

```
1. SQS Event → metric_run_requested
   ├─ catalog.datasets[dataset_id].manifestPath
   │  └─ Ejemplo: "datasets/bcra_infomondia_series/current/manifest.json"
   └─ catalog.datasets[dataset_id].projectionsPath
      └─ Ejemplo: "datasets/bcra_infomondia_series/projections"

2. Leer Manifest
   manifestPath → S3IO.get_json() → s3://{bucket}/{manifestPath}
   └─ Obtiene: parquet_files, series_codes, etc.

3. Filtrar Parquet Files
   parquet_files + series_codes → _filter_parquet_files_for_series()
   └─ Resultado: affected_parquet_files

4. Construir Paths Completos
   projectionsPath + parquet_file → S3Path.join()
   └─ Ejemplo: "datasets/.../projections" + "BCRA_XXX/.../data.parquet"
   └─ Resultado: "datasets/.../projections/BCRA_XXX/.../data.parquet"

5. Leer Datos
   full_paths → ParquetReader.read_series_from_paths()
   ├─ Convierte a S3 paths: s3://{bucket}/datasets/.../projections/...
   └─ Lee con PyArrow: ds.dataset(full_s3_paths)
```

## 8. Checklist de Diagnóstico

Cuando hay un error, verificar en orden:

- [ ] **Bucket configurado correctamente**
  - Log: `"settings_loaded"` → `bucket`
  - Variable: `AWS_S3_BUCKET` en `.env`

- [ ] **Credenciales configuradas**
  - Log: `"settings_loaded"` → `has_access_key: true`, `has_secret_key: true`
  - Variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` en `.env`

- [ ] **Manifest existe en S3**
  - Log: `"getting_manifest"` → `manifest_path`, `bucket`
  - Verificar: `aws s3 ls s3://{bucket}/{manifest_path}`

- [ ] **Parquet files en manifest**
  - Log: `"reading_series_from_dataset"` → `parquet_files_count > 0`
  - Verificar: `parquet_files` en el manifest contiene los archivos esperados

- [ ] **Paths de proyecciones correctos**
  - Log: `"reading_series_from_paths"` → `parquet_paths`, `full_s3_paths`
  - Verificar: `aws s3 ls s3://{bucket}/{projectionsPath}/...`
  - **Importante**: Las proyecciones están dentro de `datasets/`, el path completo será `s3://bucket/datasets/.../projections/...`

- [ ] **Series en parquet files**
  - Verificar: Los `parquet_files` contienen el `series_code` en su path
  - Verificar: Los archivos parquet tienen la columna `internal_series_code` con el valor correcto

## 9. Logs Clave para Debugging

Buscar estos eventos en los logs:

1. **`settings_loaded`**: Configuración inicial (bucket, credenciales)
2. **`getting_manifest`**: Intento de leer manifest (path, bucket)
3. **`manifest_not_found`**: Error al leer manifest (path, bucket, archivos disponibles)
4. **`reading_dataset_series`**: Inicio de lectura (dataset_id, manifest_path, projections_path, series_codes)
5. **`reading_series_from_dataset`**: Detalles de lectura (parquet_files_count)
6. **`reading_series_from_paths`**: Paths específicos usados (parquet_paths, full_s3_paths)
7. **`series_read_success_from_paths`**: Lectura exitosa (row_count, parquet_files_count)
8. **`failed_to_open_dataset_from_paths`**: Error al abrir archivos parquet

## 10. Comandos Útiles para Diagnóstico

```bash
# Verificar bucket configurado
grep AWS_S3_BUCKET .env

# Verificar credenciales (sin mostrar valores)
grep -E "AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY" .env | sed 's/=.*/=***/'

# Listar manifests disponibles
aws s3 ls s3://{bucket}/datasets/ --recursive | grep manifest.json

# Listar proyecciones disponibles (están dentro de datasets/)
aws s3 ls s3://{bucket}/datasets/{dataset_id}/projections/ --recursive

# Verificar un manifest específico
aws s3 cp s3://{bucket}/datasets/{dataset_id}/current/manifest.json - | jq .

# Verificar archivos parquet de una serie (proyecciones dentro de datasets/)
aws s3 ls s3://{bucket}/datasets/{dataset_id}/projections/ --recursive | grep {series_code}
```

