
# define TGT as pbs, local, slurm etc

PWD=$(shell pwd)

deploy:
	$(PWD)/../../../bin/radical-pilot-deploy-ompi.sh $(PWD)/ompi/ 2>&1 | tee deploy.log

workload:
	$(PWD)/mk_workload.sh nodes.$(TGT)

test:
	$(PWD)/test_orte.sh $(TGT)

clean:
	rm -rf workload

distclean: clean
	rm -rf ompi

pbs:
	cp $(PBS_NODEFILE) nodes.pbs
	make -C . workload
	make -C . test


