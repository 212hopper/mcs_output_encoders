FROM ubuntu:22.04

# Install Python and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3-pip \
    curl \
    gnupg2 \
    unixodbc \
    unixodbc-dev \
    odbcinst \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
    && curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN python3.11 -m pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY mcs_output_encoders.py .

# Create a directory for token storage
RUN mkdir -p /app/tokens

# Run the script
CMD ["python3.11", "mcs_output_encoders.py"]