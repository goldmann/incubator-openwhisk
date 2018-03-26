FROM scala

USER root

# Install swagger-ui
RUN mkdir /swagger-ui \
  && curl -sSL --no-verbose https://github.com/swagger-api/swagger-ui/archive/v3.6.0.tar.gz | tar -zxf - -C /swagger-ui --strip-components=2 swagger-ui-3.6.0/dist \
  && sed -i s#http://petstore.swagger.io/v2/swagger.json#/api/v1/api-docs#g /swagger-ui/index.html

# Copy app jars
ADD build/distributions/controller.tar /

COPY init.sh /
RUN chmod +x init.sh

EXPOSE 8080

USER 1000

CMD ["/init.sh", "0"]
