#!/bin/bash

# Harness Configuration Validation Script
echo "=== Harness Configuration Validation ==="

# Check if .harness directory exists
if [ ! -d ".harness" ]; then
    echo "❌ Error: .harness directory not found"
    exit 1
fi

echo "✅ .harness directory found"

# Check pipeline configuration
PIPELINE_FILE=".harness/pipelines/deployment-pipeline.yaml"
if [ -f "$PIPELINE_FILE" ]; then
    echo "✅ Pipeline configuration found: $PIPELINE_FILE"
    
    # Validate YAML syntax
    if command -v yq &> /dev/null; then
        if yq eval '.' "$PIPELINE_FILE" > /dev/null 2>&1; then
            echo "✅ Pipeline YAML syntax is valid"
        else
            echo "❌ Pipeline YAML syntax error"
        fi
    else
        echo "⚠️  Warning: yq not found, skipping YAML validation"
    fi
else
    echo "❌ Error: Pipeline configuration not found: $PIPELINE_FILE"
fi

# Check environment configurations
ENV_DIR=".harness/environments"
if [ -d "$ENV_DIR" ]; then
    echo "✅ Environment directory found: $ENV_DIR"
    
    ENV_COUNT=$(find "$ENV_DIR" -name "*.yaml" -o -name "*.yml" | wc -l)
    echo "✅ Found $ENV_COUNT environment configuration(s)"
    
    # Check for required environments
    for env in "dev" "staging" "production"; do
        if find "$ENV_DIR" -name "*${env}*" | grep -q .; then
            echo "✅ $env environment configuration found"
        else
            echo "⚠️  Warning: $env environment configuration not found"
        fi
    done
else
    echo "❌ Error: Environment directory not found: $ENV_DIR"
fi

# Check infrastructure configurations
INFRA_DIR=".harness/infrastructures"
if [ -d "$INFRA_DIR" ]; then
    echo "✅ Infrastructure directory found: $INFRA_DIR"
    
    INFRA_COUNT=$(find "$INFRA_DIR" -name "*.yaml" -o -name "*.yml" | wc -l)
    echo "✅ Found $INFRA_COUNT infrastructure configuration(s)"
else
    echo "❌ Error: Infrastructure directory not found: $INFRA_DIR"
fi

# Check service configurations
SERVICE_DIR=".harness/services"
if [ -d "$SERVICE_DIR" ]; then
    echo "✅ Service directory found: $SERVICE_DIR"
    
    SERVICE_COUNT=$(find "$SERVICE_DIR" -name "*.yaml" -o -name "*.yml" | wc -l)
    echo "✅ Found $SERVICE_COUNT service configuration(s)"
else
    echo "❌ Error: Service directory not found: $SERVICE_DIR"
fi

# Check for README
README_FILE=".harness/README.md"
if [ -f "$README_FILE" ]; then
    echo "✅ README documentation found: $README_FILE"
else
    echo "⚠️  Warning: README documentation not found: $README_FILE"
fi

echo ""
echo "=== Configuration Structure ==="
tree .harness 2>/dev/null || find .harness -type f | sed 's|[^/]*/|- |g'

echo ""
echo "=== Validation Summary ==="
echo "Configuration validation completed."
echo "Review any warnings or errors above before importing to Harness."

# Check if harness CLI is available
if command -v harness &> /dev/null; then
    echo ""
    echo "✅ Harness CLI found. You can now import configurations using:"
    echo "   harness pipeline import --file .harness/pipelines/deployment-pipeline.yaml"
    echo "   harness environment import --file .harness/environments/dev-environment.yaml"
    echo "   # ... (see README.md for complete import commands)"
else
    echo ""
    echo "⚠️  Harness CLI not found. Install it to import configurations automatically."
    echo "   You can also import configurations manually through the Harness UI."
fi