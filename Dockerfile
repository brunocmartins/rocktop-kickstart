FROM quay.io/astronomer/astro-runtime:13.1.0

USER root
ENV AIRFLOW__PROVIDERS_ODBC__ALLOW_DRIVER_IN_EXTRA=true

# Check if Debian version is supported
RUN DEBIAN_VERSION=$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2 | cut -d '.' -f 1) && \
    if ! echo "11 12" | grep -q "$DEBIAN_VERSION"; then \
        echo "Debian $DEBIAN_VERSION is not currently supported."; \
        exit 1; \
    fi && \
    # Download the Microsoft repo package
    curl -sSL -O https://packages.microsoft.com/config/debian/$DEBIAN_VERSION/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    # Update apt repositories
    apt-get update && \
    # Install CA certificates
    apt-get install -y ca-certificates && \
    # Install msodbcsql18
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    # Optional: install bcp and sqlcmd
    ACCEPT_EULA=Y apt-get install -y mssql-tools18 && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> /etc/bash.bashrc && \
    # Optional: install unixODBC dev headers
    apt-get install -y unixodbc-dev && \
    # Optional: install kerberos library for debian-slim
    apt-get install -y libgssapi-krb5-2 && \
    # Clean up apt caches to reduce image size
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
