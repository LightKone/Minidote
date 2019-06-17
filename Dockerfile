FROM erlang:22

ENV NODE_NAME "minidote@127.0.0.1"
ENV SHORT_NAME "false"

ENV LOG_DIR "/var/minidote/logs"
ENV OP_LOG_DIR "/var/minidote/op_log"
ENV MINIDOTE_POOL_SIZE 100
ENV MINIDOTE_PORT 8087
ENV MINIDOTE_MAX_CONNECTIONS 1024

COPY . /opt/minidote_src

RUN cd /opt/minidote_src \
    && make rel \
    && cp -r _build/default/rel/minidote /opt/

# Distributed Erlang Port Mapper
EXPOSE 4369
# PB Port for Minidote
EXPOSE 8087

# Distributed Erlang
EXPOSE 9100

CMD ["/opt/minidote/bin/minidote", "foreground"]
