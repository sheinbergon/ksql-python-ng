FROM confluentinc/cp-ksql-cli:5.4.10

RUN apt update && apt install -y supervisor &&\
    mkdir /var/log/ksql

COPY ./conf/ksql-server.conf /etc/supervisor/conf.d
COPY ./conf/ksqlserver.properties /etc/ksql/
COPY ./startup.sh .

ENTRYPOINT ["./startup.sh"]
