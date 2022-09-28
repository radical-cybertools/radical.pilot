# Docker recipes

Containerized RADICAL Pilot testing can be performed two ways.
Either the (required) MongoDB server can run in the same container
as RP, or in a separate service container.

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

### Example

From repository root directory

    docker build -t rp-complete -f docker/rp-complete.dockerfile .
    docker run --rm --name rp_test -d rp-complete

Either use '-d' with 'run' or issue the 'exec' in a separate terminal
after waiting a few seconds for the DB to be ready to accept connections.

    docker exec -ti -u rp rp_test bash -c \
    ". /home/rp/rp-venv/bin/activate && cd ~/radical.pilot && ~/rp-venv/bin/python -m pytest tests"

The examples need the venv to be activated in order to find supporting
shell scripts on the default PATH. The current working directory also
needs to be writable.

    docker exec -ti -u rp rp_test bash -c \
    "cd && . /home/rp/rp-venv/bin/activate && python radical.pilot/examples/00*"

If '-d' was used with 'run', you can just kill or stop the container when done.

    docker stop rp_test

## Docker Compose stack

(Warning: Still under development.)

`compose.yml` provides a recipe for `docker compose`
(see https://docs.docker.com/compose/reference/).
The stack relies on two public
container images (`mongo:bionic` for the database service and
`mongo-express` for a database admin console) and a custom image
(to be built locally) providing the sshd service and login environment.
See `radicalpilot.dockerfile` for instructions on
building the (required) `radicalpilot` container image.

One service container is launched with each of these three images.
Services in the stack are named *mongo*, *mongo-express*, and *login*, respectively.
Use `docker compose` to run docker commands that reference the service name
instead of the container name.
(The actual container names are irrelevant.)

The following examples are assumed to run from the `docker` directory in your
local copy of this repository. If run from a different directory, you will have
to give the path to the `compose.yml` file with the
[`-f` flag](https://docs.docker.com/engine/reference/commandline/compose/#use--f-to-specify-name-and-path-of-one-or-more-compose-files).

Bring the services up with

    docker compose --build -d up

(See also https://docs.docker.com/engine/reference/commandline/compose_up/#options)

Shut down and clean up with

    docker compose down

The service running the "radicalpilot" image (built from
radicalpilot.dockerfile in this directory) has an sshd server running and a
Python environment configured for the "rp" user.
Once the services are up, start a shell in the `loging` service container with

    docker compose exec -ti -u rp login bash

or invoke a Radical Pilot test with, e.g.

    docker compose exec -ti -u rp login bash -c "cd && . /home/rp/rp-venv/bin/activate && python /radical.pilot/examples/00*"
