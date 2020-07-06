# This Dockerfile is used to create an image for the "radicalpilot" service in
# the stack.yml docker-compose file in this directory.
# When a container is launched from this image with no arguments, the container
# will run an sshd daemon.
# Example:
#     docker build -t radicalpilot -f radicalpilot.dockerfile ..

FROM ubuntu:bionic

RUN apt-get update && \
    apt-get install -y \
        curl \
        dnsutils \
        gcc \
        git \
        openssh-server \
        iputils-ping \
        python3-dev \
        python3-venv \
        vim && \
    rm -rf /var/lib/apt/lists/*

# Reference https://docs.docker.com/engine/examples/running_ssh_service/
RUN mkdir /var/run/sshd

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]


RUN groupadd radical && useradd -g radical -s /bin/bash -m rp
USER rp

RUN (cd ~rp && python3 -m venv rp-venv)

RUN (cd ~rp && \
    rp-venv/bin/pip install --upgrade \
        pip \
        setuptools \
        wheel && \
    rp-venv/bin/pip install --upgrade \
        coverage \
        flake8 \
        'mock==2.0.0' \
        netifaces \
        ntplib \
        pylint \
        pymongo \
        pytest \
        python-hostlist \
        setproctitle \
        )

RUN . ~rp/rp-venv/bin/activate && \
    pip install --upgrade \
        'radical.saga>=1.0' \
        'radical.utils>=1.1'

COPY --chown=rp:radical . /radical.pilot

RUN (. ~rp/rp-venv/bin/activate && \
    cd /radical.pilot && \
    ~rp/rp-venv/bin/pip install .)


USER root

# Set the environment variable that Radical Pilot uses to find its MongoDB instance.
# Radical Pilot assumes the user is defined in the same database as in the URL.
# The Docker entry point creates users in the "admin" database, so we can just
# tell RP to use the same. The username and password are configured in the env
# passed to the mongo container in stack.yml. The service name from stack.yml
# also determines the URL host name.
# Note that the default mongodb port number is 27017.
ENV RADICAL_PILOT_DBURL="mongodb://root:password@mongo:27017/admin"

RUN echo "export RADICAL_PILOT_DBURL=$RADICAL_PILOT_DBURL" >> /etc/profile
