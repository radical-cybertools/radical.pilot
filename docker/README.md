# Docker recipes

Containerized RADICAL Pilot testing can be performed two ways.
Either the (required) MongoDB server can run in the same container
as RP, or in a separate service container.

These recipes provide an environment with a password-less `rp` user
and a `/home/rp/rp-venv` Python virtual environment ready for use.

## Monolithic container

`rp-complete.dockerfile` provides a recipe for a complete container
to run MongoDB and the RP stack. Note that the MongoDB instance can
take a few seconds to start up. The easiest way to use the container
is to `run` the container's default mongod service, wait a few moments,
and then `exec` RP scripts or a shell in the container.

When a container is launched from the resulting image with no arguments,
the entry point script will launch and initialize a MongoDB instance.
It will take a few seconds to be ready for connections, after which
radical.pilot will be able to use pymongo to connect (internally).

When building the container, the local repository is used as the source
for the radical.pilot installation. The root of the repository is
referenced as the final argument to `docker build`.

### Example

From repository root directory

    docker build -t rp-complete -f docker/rp-complete.dockerfile .

From the `docker` directory

    docker build -t rp-complete -f rp-complete.dockerfile ..

Use the image to launch a container in the background with the name "rp_test".
Tell docker to remove the container after it exits.

    docker run --rm --name rp_test -d rp-complete

Either use `-d` with `run`, or issue the `exec` in a separate terminal
after waiting a few seconds for the DB to be ready to accept connections.

    docker exec -ti -u rp rp_test bash -c \
    ". /home/rp/rp-venv/bin/activate && cd ~/radical.pilot && ~/rp-venv/bin/python -m pytest tests"

The examples need the venv to be activated in order to find supporting
shell scripts on the default PATH. The current working directory also
needs to be writable.

    docker exec -ti -u rp rp_test bash -c \
    "cd && . /home/rp/rp-venv/bin/activate && python radical.pilot/examples/00*"

If '-d' was used with 'run', you must kill or stop the container when done.

    docker stop rp_test

## Docker Compose stack

`compose.yml` provides a recipe for `docker compose`
(see https://docs.docker.com/compose/reference/).
The stack relies on two public container images
(`mongo:bionic` for the database service and
`mongo-express` for a database admin console) and a custom image
(to be built locally) providing the sshd service and login environment.
The custom image will be built automatically (with `docker compose --build`)
to provide a login environment with radical.pilot pre-installed and configured.
(See `radicalpilot.dockerfile` for details.)

One service container is launched with each of these three images.
Services in the stack are named *mongo*, *mongo-express*, and *login*, respectively.
Use `docker compose` to run docker commands that reference the service name
instead of the container name.
(The actual container names are irrelevant.)

The following examples are assumed to run from the `docker` directory in your
local copy of this repository. If run from a different directory, you will have
to give the path to the `compose.yml` file with the
[`-f` flag](https://docs.docker.com/engine/reference/commandline/compose/#use--f-to-specify-name-and-path-of-one-or-more-compose-files).

### Building and launching services

Bring the services up with

    docker compose up --build -d

(See also https://docs.docker.com/engine/reference/commandline/compose_up/#options)

### Using the container stack

The service running the "radicalpilot" image (built from
radicalpilot.dockerfile in this directory) has an sshd server running and a
Python environment configured for the "rp" user.
Once the services are up, start a shell in the `loging` service container with

    docker compose exec -ti -u rp login bash

or invoke a Radical Pilot test with, e.g.

    docker compose exec -ti -u rp login bash -c "cd && . /home/rp/rp-venv/bin/activate && python radical.pilot/examples/00*"

### Port mapping services to the host machine

Port 8081 is mapped from the *mongo-express* service container for the mongodb web interface.
To configure or disable, either edit compose.yml or use and explicit list of services to exclude.

    docker compose up -d login mongo

Compose can also map the sshd port from the login service container to
port 2345 on the host machine loopback address (127.0.0.1).
Edit the compose.yml file to configure.

### Don't forget to bring down the stack!

Shut down and clean up with

    docker compose down

## Development

The local repository is copied into the image when it is built.
To update the radical.pilot software without rebuilding the image(s),
you can use `docker cp` or a `tar` pipeline.

For a small number of files, for instance,

    docker cp ../examples/misc/raptor.cfg rp_test:/home/rp/radical.pilot/examples/misc/

To update the package from source, change directory to the root of the repository
or use `-C <path>`.

For monolithic `rp_test` container as described above:

    # From repo base
    tar cf - . | docker cp - rp_test:/home/rp/radical.pilot/
    # From docker directory
    tar cf - -C .. . | docker cp - rp_test:/home/rp/radical.pilot/

For `docker compose`:

    # From repo base
    tar cf - . | docker compose -f docker/compose.yml cp - login:/home/rp/radical.pilot/
    # From docker directory
    tar cf - -C .. . | docker compose cp - login:/home/rp/radical.pilot/

The `uid` in the container will not match your uid in the host, and the `cp` ran as root.
First, update the ownership and permissions of the transferred files.
Then reinstall the package in the container. 

    docker exec -u root rp_test bash -c "chown -R rp /home/rp && chmod u+rx -R /home/rp/radical.pilot"
    docker exec -u rp rp_test bash -c ". ~/rp-venv/bin/activate && cd ~/radical.pilot && pip install --upgrade ."
    # or
    docker compose exec -u root login bash -c "chown -R rp /home/rp && chmod u+rx -R /home/rp/radical.pilot"
    docker compose exec -u rp login bash -c ". ~/rp-venv/bin/activate && cd ~/radical.pilot && pip install --upgrade ."
