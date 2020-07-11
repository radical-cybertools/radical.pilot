# Exit on errors.
set -ev

cd docker

# Build the supporting image for the login container.
docker build -t radicalpilot -f radicalpilot.dockerfile ..

# Launch the services stack.
docker-compose -f stack.yml up -d

# Give a few seconds for mongodb to start up.
sleep 5

# Test examples.
docker exec -ti -u rp docker_login_1 bash -c "cd && . /home/rp/rp-venv/bin/activate && python /radical.pilot/examples/00*"

# Shut down the services.
docker-compose -f stack.yml down
