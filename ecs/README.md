# ECS Deployment Configuration

El servicio `metrics-engine-dp-worker` ya está desplegado en ECS.

## Estado actual

- ✅ **Task Definition**: `metrics-engine-dp-worker` registrada
- ✅ **Servicio ECS**: `metrics-engine-dp-worker` creado en cluster `metrics-engine-cluster`
- ✅ **CloudWatch Log Group**: `/ecs/metrics-engine-dp-worker` creado
- ✅ **ECR Repository**: `706341500093.dkr.ecr.us-east-1.amazonaws.com/metrics-engine-dp-worker` existe

## Lo que falta configurar

### 1. Configurar permisos IAM para el usuario de CI/CD

El usuario IAM `metrics-engine-dp-worker-deploy` necesita permisos para ECR y ECS.

**Opción A: Desde AWS CLI**

```bash
# Crear la política
aws iam create-policy \
  --policy-name metrics-engine-dp-worker-ci-cd \
  --policy-document file://ecs/iam-policy-ci-cd.json \
  --region us-east-1

# Adjuntar la política al usuario (reemplaza ACCOUNT_ID con 706341500093)
aws iam attach-user-policy \
  --user-name metrics-engine-dp-worker-deploy \
  --policy-arn arn:aws:iam::706341500093:policy/metrics-engine-dp-worker-ci-cd
```

**Opción B: Desde la consola AWS**

1. Ve a IAM > Policies > Create policy
2. En la pestaña JSON, pega el contenido de `ecs/iam-policy-ci-cd.json`
3. Click en "Next" y dale un nombre: `metrics-engine-dp-worker-ci-cd`
4. Click en "Create policy"
5. Ve a IAM > Users > `metrics-engine-dp-worker-deploy` > Add permissions
6. Selecciona "Attach policies directly"
7. Busca y selecciona `metrics-engine-dp-worker-ci-cd`
8. Click en "Add permissions"

### 2. Secrets de GitHub Actions (para CI/CD)

Una vez que el usuario IAM tenga los permisos, configura los secrets en GitHub:

**Configurar en**: Settings > Secrets and variables > Actions > New repository secret

- **`AWS_ACCESS_KEY_ID`**: Access key del usuario `metrics-engine-dp-worker-deploy`
- **`AWS_SECRET_ACCESS_KEY`**: Secret key correspondiente

**Nota**: Asegúrate de que el secret key no tenga espacios ni saltos de línea al copiarlo.

### 3. Verificar permisos de IAM Roles (opcional)

Los roles IAM ya están configurados en la task definition, pero verifica que tengan los permisos correctos:

**Execution Role (`ecsTaskExecutionRole`)** necesita:
- ECR: `ecr:GetAuthorizationToken`, `ecr:BatchCheckLayerAvailability`, `ecr:GetDownloadUrlForLayer`, `ecr:BatchGetImage`
- CloudWatch Logs: `logs:CreateLogStream`, `logs:PutLogEvents`

**Task Role (`ecsTaskRole`)** necesita:
- S3: `GetObject`, `PutObject`, `ListBucket` en `ingestor-datasets`
- SNS: `Publish` en los topics:
  - `arn:aws:sns:us-east-1:706341500093:metric-run-started.fifo`
  - `arn:aws:sns:us-east-1:706341500093:metric-run-heartbeat.fifo`
  - `arn:aws:sns:us-east-1:706341500093:metric-run-completed.fifo`
- SQS: `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility` en la cola de requests

## Despliegue automático

Una vez configurados los secrets de GitHub Actions, los despliegues futuros se realizarán automáticamente cuando hagas push a `main` o `master`.

El workflow:
1. Construye la imagen Docker
2. La sube a ECR
3. Actualiza la task definition con la nueva imagen
4. Actualiza el servicio ECS existente

## Comandos útiles

### Verificar estado del servicio

```bash
aws ecs describe-services \
  --cluster metrics-engine-cluster \
  --services metrics-engine-dp-worker \
  --region us-east-1
```

### Ver logs

```bash
aws logs tail /ecs/metrics-engine-dp-worker --follow --region us-east-1
```

### Escalar el servicio

```bash
aws ecs update-service \
  --cluster metrics-engine-cluster \
  --service metrics-engine-dp-worker \
  --desired-count 3 \
  --region us-east-1
```

### Ver tareas corriendo

```bash
aws ecs list-tasks \
  --cluster metrics-engine-cluster \
  --service-name metrics-engine-dp-worker \
  --region us-east-1
```
