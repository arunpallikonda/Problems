# Harness Pipeline with Environment Selection and Dropdown Variables

This repository contains a complete Harness pipeline configuration that allows you to:
- Select from predefined environments (dev, staging, production)
- Choose from multiple dropdown variables for deployment configuration
- Execute shell scripts on target servers using environment-specific users

## Pipeline Structure

### Main Pipeline: `deployment-pipeline.yaml`
- **Environment Selection**: Choose from dev, staging, or production environments
- **Dropdown Variables**:
  - `service_type`: web_service, api_service, database_service
  - `deployment_mode`: rolling, blue_green, canary
  - `database_action`: none, migrate, backup, restore
  - `notification_channels`: email, slack, teams, none

### Shell Script Execution
The pipeline executes three main shell script steps:
1. **Pre-deployment Health Check**: System resource verification
2. **Custom Script Execution**: Environment-specific deployment logic
3. **Post-deployment Verification**: Health checks and service validation

## Environment Configuration

### Development Environment (`dev-environment.yaml`)
- **SSH User**: `deploy-dev`
- **Database Host**: `dev-db.internal.com`
- **Log Level**: DEBUG
- **Port**: 8080

### Staging Environment (`staging-environment.yaml`)
- **SSH User**: `deploy-staging`
- **Database Host**: `staging-db.internal.com`
- **Log Level**: INFO
- **Port**: 8080

### Production Environment (`production-environment.yaml`)
- **SSH User**: `deploy-prod`
- **Database Host**: `prod-db.internal.com`
- **Log Level**: ERROR
- **Port**: 80

## Infrastructure Definitions

Each environment has its own infrastructure definition with:
- SSH connector configuration
- Target server hostnames
- Environment-specific SSH credentials
- User configuration from environment variables

## Setup Instructions

### 1. Import Configurations into Harness

1. **Upload Pipeline**:
   ```bash
   # Import the main pipeline
   harness-cli pipeline import --file .harness/pipelines/deployment-pipeline.yaml
   ```

2. **Create Environments**:
   ```bash
   # Import environment configurations
   harness-cli environment import --file .harness/environments/dev-environment.yaml
   harness-cli environment import --file .harness/environments/staging-environment.yaml
   harness-cli environment import --file .harness/environments/production-environment.yaml
   ```

3. **Setup Infrastructure**:
   ```bash
   # Import infrastructure definitions
   harness-cli infrastructure import --file .harness/infrastructures/dev-infrastructure.yaml
   harness-cli infrastructure import --file .harness/infrastructures/staging-infra.yaml
   harness-cli infrastructure import --file .harness/infrastructures/production-infrastructure.yaml
   ```

4. **Import Service**:
   ```bash
   # Import service configuration
   harness-cli service import --file .harness/services/application-service.yaml
   ```

### 2. Configure SSH Connectors

For each environment, create SSH connectors in Harness:

1. **Development Connector** (`dev_ssh_connector`):
   - Connection Type: SSH
   - Host: `dev-server-01.internal.com`
   - Username: `deploy-dev`
   - SSH Key/Password: Configure as per your setup

2. **Staging Connector** (`staging_ssh_connector`):
   - Connection Type: SSH
   - Host: `staging-server-01.internal.com`
   - Username: `deploy-staging`
   - SSH Key/Password: Configure as per your setup

3. **Production Connector** (`production_ssh_connector`):
   - Connection Type: SSH
   - Host: `prod-server-01.internal.com`
   - Username: `deploy-prod`
   - SSH Key/Password: Configure as per your setup

### 3. Configure SSH Credentials

Create SSH credential sets for each environment:
- `dev_ssh_credentials`
- `staging_ssh_credentials`  
- `production_ssh_credentials`

## How to Use the Pipeline

### 1. Trigger Pipeline
1. Navigate to your Harness project
2. Go to Pipelines and select "Deployment Pipeline with Environment Selection"
3. Click "Run Pipeline"

### 2. Select Options
When running the pipeline, you'll be prompted to select:

**Environment**: Choose from dropdown
- Development Environment (dev)
- Staging Environment (staging)
- Production Environment (production)

**Service Type**: Choose from dropdown
- web_service: Deploys web applications (nginx-based)
- api_service: Deploys API services (tomcat-based)
- database_service: Deploys database services (mysql-based)

**Deployment Mode**: Choose from dropdown
- rolling: Standard rolling deployment
- blue_green: Blue-green deployment strategy
- canary: Canary deployment with gradual rollout

**Database Action**: Choose from dropdown (optional)
- none: No database action
- migrate: Run database migrations
- backup: Create database backup
- restore: Restore database from backup

**Notification Channels**: Choose from dropdown (optional)
- email: Email notifications
- slack: Slack notifications
- teams: Microsoft Teams notifications
- none: No notifications

### 3. Monitor Execution
The pipeline will execute with the selected configuration:
- Shell scripts run with the environment-specific SSH user
- Variables are passed to scripts for conditional logic
- Rollback procedures are available if deployment fails

## Customization

### Adding New Environments
1. Create a new environment YAML file in `.harness/environments/`
2. Define environment-specific variables including `ssh_user`
3. Create corresponding infrastructure definition
4. Update pipeline environment dropdown options

### Adding New Variables
1. Edit `deployment-pipeline.yaml`
2. Add new variables in the `variables` section
3. Use `<+input>.allowedValues(option1,option2,option3)` for dropdowns
4. Reference variables in shell scripts as `<+pipeline.variables.variable_name>`

### Modifying Shell Scripts
1. Edit the script content in the pipeline YAML
2. Use environment variables: `<+env.variables.variable_name>`
3. Use pipeline variables: `<+pipeline.variables.variable_name>`
4. Add error handling and logging as needed

## Security Considerations

1. **SSH Keys**: Store SSH private keys securely in Harness secrets
2. **Credentials**: Use Harness credential management for sensitive data
3. **Access Control**: Configure RBAC for pipeline execution permissions
4. **Audit**: Enable audit logging for deployment activities

## Troubleshooting

### Common Issues

1. **SSH Connection Failed**:
   - Verify SSH connector configuration
   - Check SSH credentials and keys
   - Ensure target servers are accessible

2. **Environment Variables Not Found**:
   - Verify environment configuration is imported
   - Check variable names and references
   - Ensure environment is properly linked to infrastructure

3. **Script Execution Errors**:
   - Check shell script syntax
   - Verify file permissions on target servers
   - Review environment-specific paths and commands

### Debug Steps

1. **Enable Debug Logging**:
   - Set log level to DEBUG in pipeline
   - Review execution logs in Harness console

2. **Test SSH Connectivity**:
   - Use Harness connectivity test feature
   - Manually test SSH from delegate to target servers

3. **Validate Variables**:
   - Print variables in shell scripts for verification
   - Check variable scope and availability

## Support

For issues or questions:
1. Check Harness documentation
2. Review pipeline execution logs
3. Contact your DevOps team for environment-specific issues