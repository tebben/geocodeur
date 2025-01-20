FROM postgres:16.2-bookworm

ENV POSTGRESQL_VERSION_MAJOR=16

# Install dependencies, gcc/make/cmake/postgresql-server-dev-16
# are needed for building extensions using pgxn
RUN set -eux; \
  apt-get update; \
  apt-get install -y --no-install-recommends \
  curl \
  ca-certificates \
  pgxnclient \
  gcc \
  make \
  cmake \
  postgresql-server-dev-16 \
  ; \
  rm -rf /var/lib/apt/lists/*

# Copy over pg config, will be moved later to overwrite the default
#COPY postgresql.conf /postgresql.conf

# put z_ in front so it will be executed after the default postgis.sh script
#COPY init.sh /docker-entrypoint-initdb.d/z_init.sh

# Copy over entrypoint script
#COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Copy over pgxman.yaml to install extensions
COPY pgxman.yaml /tmp/pgxman/pgxman.yaml

# Install pgxman and extensions
RUN curl -sfL https://install.pgx.sh | sh -s -- /tmp/pgxman/pgxman.yaml && \
  rm -rf /tmp/pgxman

# chmod the entrypoint script
#RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 5432

#ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
