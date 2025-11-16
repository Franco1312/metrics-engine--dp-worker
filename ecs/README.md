# ECS Deployment Configuration

El servicio `metrics-engine-dp-worker` ya está desplegado en ECS.

## Estado actual

- ✅ **Task Definition**: `metrics-engine-dp-worker` registrada
- ✅ **Servicio ECS**: `metrics-engine-dp-worker` creado en cluster `metrics-engine-cluster`
- ✅ **CloudWatch Log Group**: `/ecs/metrics-engine-dp-worker` creado
- ✅ **ECR Repository**: `706341500093.dkr.ecr.us-east-1.amazonaws.com/metrics-engine-dp-worker` existe

## Lo que falta configurar

### 1. Secrets de GitHub Actions (para CI/CD)

Para que el workflow de GitHub Actions pueda desplegar automáticamente, necesitas configurar los siguientes secrets en GitHub:

**Configurar en**: Settings > Secrets and variables > Actions > New repository secret

- **`AWS_ACCESS_KEY_ID`**: Access key de AWS con permisos para:
  - ECR: `ecr:GetAuthorizationToken`, `ecr:BatchCheckLayerAvailability`, `ecr:GetDownloadUrlForLayer`, `ecr:BatchGetImage`, `ecr:PutImage`, `ecr:InitiateLayerUpload`, `ecr:UploadLayerPart`, `ecr:CompleteLayerUpload`
  - ECS: `ecs:RegisterTaskDefinition`, `ecs:DescribeServices`, `ecs:UpdateService`, `ecs:DescribeTaskDefinition`
  
- **`AWS_SECRET_ACCESS_KEY`**: Secret key correspondiente

**Nota**: Puedes usar el mismo usuario IAM que usaste para crear el servicio, o crear uno específico para CI/CD.

### 2. Verificar permisos de IAM Roles (opcional)

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
