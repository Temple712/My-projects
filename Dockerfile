# Use the official Microsoft SQL Server image as the base
FROM mcr.microsoft.com/mssql/server:2019-latest

# Set environment variables required for SQL Server
ENV ACCEPT_EULA=Y
ENV SA_PASSWORD
ENV MSSQL_PID=Developer

# Switch to root user to install additional packages
USER root

# Install necessary packages and tools like sqlcmd
RUN apt-get update && \
    apt-get install -y curl apt-transport-https locales gnupg2 && \
    locale-gen en_US.UTF-8 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | tee /etc/apt/sources.list.d/msprod.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools unixodbc-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add sqlcmd to the PATH environment variable
ENV PATH="$PATH:/opt/mssql-tools/bin"

# Expose the default SQL Server port
EXPOSE 1433
COPY ./create_database.sql /opt/mssql/scripts/create_database.sql

# Add an entrypoint script to run SQL Server and execute the SQL script
COPY ./entrypoint.sh /opt/mssql/scripts/entrypoint.sh
RUN chmod +x /opt/mssql/scripts/entrypoint.sh
# Set the entry point to start SQL Server

# Switch back to the mssql user to run SQL Server
USER mssql
# Set the entry point to start SQL Server
ENTRYPOINT ["/opt/mssql/scripts/entrypoint.sh"]

