
# define TGT as pbs, local, slurm etc

PWD=$(shell pwd)

deploy:
	$(PWD)/../../../bin/radical-pilot-deploy-ompi.sh $(PWD)/ompi/ 2>&1 | tee deploy.log

workload: pbs
	$(PWD)/mk_workload.sh

test: workload
	$(PWD)/test_orte.sh

clean:
	rm -rf workload nodes dvm.log dvm.uri

local:
	cp nodes.local nodes

pbs:
	$(shell cat $(PBS_NODEFILE) | sort | uniq -c | awk '{print $$2":"$$1}' > nodes)


