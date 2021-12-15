FROM ubuntu AS tsv-data-analytics 

# base package update
RUN apt-get update
RUN apt-get install -y wget curl apt-utils cron vim

# define repo
RUN sh -c "echo 'deb http://download.opensuse.org/repositories/network:/messaging:/zeromq:/release-stable/xUbuntu_18.04/ /' > /etc/apt/sources.list.d/network:messaging:zeromq:release-stable.list"

# install go compiler
RUN apt-get install -y cron wget
RUN apt-get install -y python3 python3-pip python3-venv

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
RUN pip3 install tsv_data_analytics tsv_data_analytics_ext --upgrade
RUN pip3 install jupyterlab

# entrypoint
ENTRYPOINT ["./docker-entrypoint.sh"]
