FROM ubuntu:20.04 AS omigo-data-analytics
ENV DEBIAN_FRONTEND=noninteractive

# base package update
RUN apt-get update
RUN apt-get install -y wget curl apt-utils cron vim

# install go compiler
RUN apt-get install -y cron wget
RUN apt-get install -y python3 python3-pip python3-venv 
RUN apt-get install -y graphviz

# environment variables
#ENV PATH="$PATH:/usr/local/go/bin"
ENV BASEDIR="/code"

# create data directories
RUN mkdir -p $BASEDIR

# copy go files, run scripts
COPY --chown=root example-notebooks $BASEDIR/example-notebooks
COPY deploy/docker-entrypoint.sh $BASEDIR/docker-entrypoint.sh

# define working directory
WORKDIR /code

# build
RUN python3 -m pip install --upgrade build
RUN pip3 install omigo-core omigo-ext --upgrade
RUN pip3 install jupyterlab
RUN pip3 install yfinance 
RUN pip3 install graphviz 

# entrypoint
ENTRYPOINT ["./deploy/docker-entrypoint.sh"]
