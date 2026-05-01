#!/usr/bin/env bash

set -e

SERVICE_NAME="churn_prediction_service"
IMAGE_TAG="churn_prediction_service:latest"

echo "================================="
echo "Building Bento..."
echo "================================="

bentoml build

echo "================================="
echo "Containerizing Bento -> Docker image"
echo "================================="

bentoml containerize ${SERVICE_NAME}:latest \
    --image-tag ${IMAGE_TAG}

echo "================================="
echo "Done!"
echo "Docker image created: ${IMAGE_TAG}"
echo "================================="

docker images | grep churn_prediction_service
