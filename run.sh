#!/usr/bin/env bash

set -e

echo "================================="
echo "STEP 1: Build Bento Image"
echo "================================="

cd ./churn_serving
./build_bento_image.sh
cd ..

echo "================================="
echo "STEP 2: Docker Compose Deploy"
echo "================================="

docker compose up -d --build

echo "================================="
echo "Deployment completed!"
echo "================================="
