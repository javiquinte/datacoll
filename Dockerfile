FROM mongo

MAINTAINER Javier Quinteros

RUN apt update && apt install -y python3-cherrypy3 python3-gnupg git && mkdir -p /opt
RUN cd /opt && git clone https://github.com/javiquinte/datacoll.git && cd datacoll && git checkout mongo

# Define default command.
RUN mongod --fork --syslog && mongo < /opt/datacoll/docker/createSchema.js
RUN cp /opt/datacoll/datacoll.cfg.sample /opt/datacoll/datacoll.cfg

EXPOSE 27017
EXPOSE 28017
EXPOSE 8080

CMD ["python3", "datacoll.py"]