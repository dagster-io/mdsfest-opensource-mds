FROM astrocrpublic.azurecr.io/runtime:3.0-2

# Install Sling for data replication
USER root

RUN curl -LO "https://github.com/slingdata-io/sling-cli/releases/download/v1.4.9/sling_linux_arm64.tar.gz" && \
    tar -xzf sling_linux_arm64.tar.gz && \
    mv sling /usr/local/bin/ && \
    chmod +x /usr/local/bin/sling

# Install dbt for data transformations
USER astro
RUN pip install dbt-core==1.9.4 dbt-duckdb==1.9.3

# Verify installations
RUN sling --version && dbt --version
