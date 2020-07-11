# Exit on errors.
set -ev

cd docker

# Build the image
docker build -t rp-complete -f rp-complete.dockerfile ..

# Launch the container
docker run --rm --name rp_test -d rp-complete

# Give a few seconds for mongodb to start up.
sleep 5

# Test examples
docker exec -ti -u rp rp_test bash -c "cd && . /home/rp/rp-venv/bin/activate && python /radical.pilot/examples/00*"

# Shut down the container.
docker kill rp_test
