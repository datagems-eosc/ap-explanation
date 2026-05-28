ARG PG_VERSION=17
FROM postgres:${PG_VERSION}

ARG PG_VERSION=17
ARG PROVSQL_VERSION=master

# Install build dependencies, git, and other necessary tools
RUN apt-get update && apt-get install -y \
    build-essential \
    libgmp-dev \
    libmpfr-dev \
    libmpc-dev \
    postgresql-server-dev-${PG_VERSION} \
    ca-certificates \
    unzip\
    mercurial\
    libgmp-dev\
    libboost-dev\
    libboost-serialization-dev\
    git \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Compile and install ProvSQL
RUN git clone --branch ${PROVSQL_VERSION} --depth 1 https://github.com/PierreSenellart/provsql.git /provsql

WORKDIR /provsql

RUN make && make install

# Add custom configuration
RUN mkdir -p /etc/postgresql && \
    echo "shared_preload_libraries = 'provsql'" >> /etc/postgresql/postgresql.conf && \
    echo "listen_addresses = '*'" >> /etc/postgresql/postgresql.conf && \
    echo "provsql.active = 0" >> /etc/postgresql/postgresql.conf && \
    echo "local all all trust" > /etc/postgresql/pg_hba.conf && \
    echo "host all all 0.0.0.0/0 trust" >> /etc/postgresql/pg_hba.conf

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]