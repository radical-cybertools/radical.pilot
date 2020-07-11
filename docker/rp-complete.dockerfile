# When a container is launched from this image with no arguments,
# the entry point script will launch and initialize a MongoDB instance.
# It will take a few seconds to be ready for connections, after which
# radical.pilot will be able to use pymongo to connect.
# Example:
#     docker build -t rp-complete -f rp-complete.dockerfile ..
#     docker run --rm --name rp_test -d rp-complete
#     # Either use '-d' with 'run' or issue the 'exec' in a separate terminal
#     # after waiting a few seconds for the DB to be ready to accept connections.
#     docker exec -ti -u rp rp_test bash -c "cd /radical.pilot && /home/rp/rp-venv/bin/python -m pytest /radical.pilot/tests"
#     # The examples need the venv to be activated in order to find supporting
#     # shell scripts on the default PATH. The current working directory also
#     # needs to be writable.
#     docker exec -ti -u rp rp_test bash -c "cd && . /home/rp/rp-venv/bin/activate && python /radical.pilot/examples/00*"
#     # If '-d' was used with 'run', you can just kill the container when done.
#     docker kill rp_test

FROM mongo:bionic
# Reference https://github.com/docker-library/mongo/blob/master/4.2/Dockerfile

RUN groupadd -r radical && useradd -r -g radical -m rp

RUN apt-get update && \
    apt-get install -y \
        curl \
        dnsutils \
        gcc \
        git \
        iputils-ping \
        python3-dev \
        python3-venv \
        vim && \
    rm -rf /var/lib/apt/lists/*

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

# Note that the following environment variables have special meaning to the
# `mongo` Docker container entry point script.
ENV MONGO_INITDB_ROOT_USERNAME=root
ENV MONGO_INITDB_ROOT_PASSWORD=password

# Set the environment variable that Radical Pilot uses to find its MongoDB instance.
# Radical Pilot assumes the user is defined in the same database as in the URL.
# The Docker entry point creates users in the "admin" database, so we can just
# tell RP to use the same.
# Note that the default mongodb port number is 27017.
ENV RADICAL_PILOT_DBURL="mongodb://$MONGO_INITDB_ROOT_USERNAME:$MONGO_INITDB_ROOT_PASSWORD@localhost:27017/admin"
