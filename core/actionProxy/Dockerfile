# Dockerfile for docker skeleton (useful for running blackbox binaries, scripts, or Python 3 actions) .
FROM python:3.6.1-alpine

# Upgrade and install basic Python dependencies.
RUN apk add --no-cache bash \
 && apk add --no-cache --virtual .build-deps \
        bzip2-dev \
        gcc \
        libc-dev \
  && pip install --upgrade pip setuptools six \
  && pip install --no-cache-dir gevent==1.2.1 flask==0.12 \
  && apk del .build-deps

ENV FLASK_PROXY_PORT 8080

RUN mkdir -p /actionProxy  \
  && chgrp -R 0 /actionProxy \
  && chmod -R g+rwX /actionProxy
ADD actionproxy.py /actionProxy/

RUN mkdir -p /action
ADD stub.sh /action/exec
RUN chmod +x /action/exec \
  && chgrp -R 0 /action \
  && chmod -R g+rwX /action

CMD ["/bin/bash", "-c", "cd actionProxy && python -u actionproxy.py"]
