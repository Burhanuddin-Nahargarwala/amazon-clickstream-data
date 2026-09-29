#!/bin/bash
# Roll out a version to the ECS Express service.
#   New release: ./deploy.sh v4   -> builds the current code, pushes it as v4, deploys it
#   Rollback:    ./deploy.sh v3   -> v3 already exists in ECR, so it is redeployed as-is (no rebuild)
# Each release tag should also exist as a git tag, so the code behind any image can be checked out.
set -euo pipefail

TAG="${1:?Usage: ./deploy.sh <image-tag>}"
REGION=us-east-1
ACCOUNT_ID=512357470856
REGISTRY="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"
IMAGE="$REGISTRY/enqurious/clickstream-api:$TAG"
SERVICE_ARN="arn:aws:ecs:$REGION:$ACCOUNT_ID:service/default/clickstream-api"
SECRET_ARN="arn:aws:secretsmanager:$REGION:$ACCOUNT_ID:secret:clickstream-api/access-token-cN0Vde"

cd "$(dirname "$0")"

if aws ecr describe-images --repository-name enqurious/clickstream-api --image-ids imageTag="$TAG" \
     --region "$REGION" >/dev/null 2>&1; then
  echo "Image $TAG already exists in ECR - redeploying it without rebuilding (rollback)."
else
  aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
  docker build -t "$IMAGE" .
  docker push "$IMAGE"
fi

aws ecs update-express-gateway-service \
  --service-arn "$SERVICE_ARN" \
  --region "$REGION" \
  --primary-container "{
    \"image\": \"$IMAGE\",
    \"containerPort\": 8000,
    \"environment\": [
      {\"name\": \"aws-region\", \"value\": \"$REGION\"},
      {\"name\": \"dynamodb-region\", \"value\": \"$REGION\"}
    ],
    \"secrets\": [{\"name\": \"ACCESS_TOKEN\", \"valueFrom\": \"$SECRET_ARN\"}]
  }" \
  --query "service.status.statusCode" --output text

echo "Rolling out $IMAGE ..."
aws ecs wait services-stable --cluster default --services clickstream-api --region "$REGION"
echo "Done: https://cl-e5785023185c46e3b4358e06d84a852b.ecs.us-east-1.on.aws"
