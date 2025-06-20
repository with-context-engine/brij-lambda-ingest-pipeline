#!/bin/bash

# Complete deployment script for the ingest pipeline
# This script deploys the infrastructure using Terraform

set -e

echo "🚀 Starting deployment of ingest pipeline..."
echo ""

# Check if we're in the correct directory
if [ ! -f "terraform.tf" ]; then
    echo "❌ Error: terraform.tf not found. Please run this script from the project root."
    exit 1
fi

# Step 1: Initialize Terraform if needed
if [ ! -d ".terraform" ]; then
    echo "🔧 Step 1: Initializing Terraform..."
    terraform init -upgrade
    echo ""
fi

# Step 2: Plan the deployment
echo "📋 Step 2: Planning Terraform deployment..."
terraform plan -out=plan.out
echo ""

# Step 3: Ask for confirmation
read -p "Do you want to proceed with the deployment? (y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🏗️  Step 3: Applying Terraform configuration..."
    terraform apply -auto-approve "plan.out"
    echo ""
    echo "✅ Deployment completed successfully!"
    echo ""
    echo "Your ingest pipeline is now ready to:"
    echo "  • Process PDF files uploaded to s3://brij-v1-bucket/upload/"
    echo "  • Convert PDF pages to PNG images"
    echo "  • Store PNG files in s3://brij-v1-bucket/raw/"
    echo "  • Create Label Studio tasks in s3://brij-v1-bucket/ingest/"
else
    echo "❌ Deployment cancelled."
    exit 1
fi