FROM openwhisk/java8action:1.0.0

# OpenShift compatibility
RUN chgrp -R 0 /javaAction \
  && chmod -R g+rwX /javaAction
