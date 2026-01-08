# Dolby Alarms Collector - Docker Setup

This Docker setup runs the MCS Dolby alarms collection script in a containerized environment.

## Files Included

- **Dockerfile** - Container definition
- **docker-compose.yml** - Docker Compose configuration with environment variables
- **dolby_alarms_sqlserver.py** - Updated script that reads from environment variables
- **requirements.txt** - Python dependencies
- **.dockerignore** - Files to exclude from Docker image

## Setup Instructions

### Option 1: Using Docker Compose (Recommended)

1. **Edit docker-compose.yml** and update the environment variables if needed:
   ```yaml
   environment:
     SQL_PASSWORD: "daznTXE123!"  # Update these values
     MCS_PASSWORD: "source_uploader"
   ```

2. **Build and run the container:**
   ```bash
   docker-compose up -d
   ```

3. **View logs:**
   ```bash
   docker-compose logs -f dolby-alarms
   ```

4. **Stop the container:**
   ```bash
   docker-compose down
   ```

### Option 2: Using Docker CLI

1. **Build the image:**
   ```bash
   docker build -t dolby-alarms-collector .
   ```

2. **Run the container:**
   ```bash
   docker run -d \
     --name dolby-alarms \
     -e SQL_SERVER="10.145.148.30,30001" \
     -e SQL_USERNAME="sa" \
     -e SQL_PASSWORD="daznTXE123!" \
     -e MCS_IP_PRIMARY="10.111.203.55" \
     -e MCS_USERNAME="source_uploader" \
     -e MCS_PASSWORD="source_uploader" \
     -v dolby-tokens:/app/tokens \
     dolby-alarms-collector
   ```

3. **View logs:**
   ```bash
   docker logs -f dolby-alarms
   ```

4. **Stop the container:**
   ```bash
   docker stop dolby-alarms
   docker rm dolby-alarms
   ```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| MCS_IP_PRIMARY | 10.111.203.55 | Primary MCS server IP |
| MCS_IP_SECONDARY | 10.111.203.55 | Secondary MCS server IP |
| MCS_PORT | 443 | MCS port |
| MCS_USERNAME | source_uploader | MCS username |
| MCS_PASSWORD | source_uploader | MCS password |
| SQL_SERVER | 10.145.148.30,30001 | SQL Server address and port |
| SQL_DATABASE | master | SQL Server database |
| SQL_USERNAME | sa | SQL Server username |
| SQL_PASSWORD | (empty) | SQL Server password **[REQUIRED]** |

## Network Requirements

The container needs to be able to reach:
- **MCS Server**: 10.111.203.55:443
- **SQL Server**: 10.145.148.30:30001

If using Docker on the same network, this should work automatically. If using Docker on a different network or machine, ensure network connectivity between the container and these services.

## Persistent Storage

Token data is stored in a Docker volume named `dolby-tokens`. This ensures tokens persist across container restarts.

## Running on Ubuntu (Portainer)

1. Go to **Portainer**
2. Navigate to **Stacks** 
3. Click **Add Stack**
4. Paste the contents of `docker-compose.yml`
5. Update the environment variables (especially `SQL_PASSWORD`)
6. Click **Deploy the stack**

## Troubleshooting

### Container keeps restarting
- Check logs: `docker logs dolby-alarms`
- Verify SQL Server credentials in environment variables
- Ensure network connectivity to MCS and SQL Server

### Token file permission errors
- The container creates `/app/tokens` directory automatically
- If permissions issues occur, check volume mounting

### SQL Server connection failures
- Verify `SQL_SERVER`, `SQL_USERNAME`, `SQL_PASSWORD` are correct
- Ensure SQL Server is running and accessible from the container
- Check if firewall allows connection on port 30001

## Updates

To update the script:
1. Modify the source files
2. Rebuild the image: `docker build -t dolby-alarms-collector .`
3. Restart the container with the new image

## Example Output

```
Starting Dolby alarms collector...
MCS Primary: https://10.111.203.55:443
SQL Server: 10.145.148.30,30001

--- Iteration 1 ---
Fetching Dolby alarms at 22:07:20...
Refreshing access token...
Token refreshed successfully
Found 2 Dolby alarm events
Database updated successfully
Waiting 60 seconds before next collection...
```
