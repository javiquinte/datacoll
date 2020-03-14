FROM mongo:bionic
# mongo:latest?

MAINTAINER Javier Quinteros

RUN apt update && apt install -y python3-cherrypy3 git && mkdir -p /opt
RUN cd /opt && git clone https://github.com/javiquinte/datacoll.git

COPY docker/createSchema,js /tmp/createSchema.js

# Define default command.
RUN mongod && mongo < /tmp/createSchema.js

EXPOSE 27017
EXPOSE 28017
EXPOSE 8080

CMD ["python3", "datacoll.py"]
