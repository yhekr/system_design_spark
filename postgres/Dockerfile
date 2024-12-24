FROM postgres:13

COPY ./postgres/create-multiple-postgresql-databases.sh /docker-entrypoint-initdb.d/

RUN chmod +x /docker-entrypoint-initdb.d/create-multiple-postgresql-databases.sh