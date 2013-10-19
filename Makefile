
.PHONY: all doc clean

all: doc

doc:
	-test -d sinon.bak && echo "sinon.bak/ exists"
	test ! -d sinon.bak
	mv sinon sinon.bak
	mkdir sinon
	cp    sinon.bak/api/*.py sinon/
	cp -R sinon.bak/utils/   sinon/
	make -C doc html
	rm -rf sinon
	mv sinon.bak sinon

test:
	nosetests tests.restapi --config=tests/nose.cfg

copyright:

pylint:
	@rm -f pylint.out ;\
	for f in `find sinon -name \*.py`; do \
		echo "checking $$f"; \
		( \
	    res=`pylint -r n -f text $$f 2>&1 | grep -e '^[FE]'` ;\
		  test -z "$$res" || ( \
		       echo '----------------------------------------------------------------------' ;\
		       echo $$f ;\
		       echo '----------------------------------------------------------------------' ;\
		  		 echo $$res | sed -e 's/ \([FEWRC]:\)/\n\1/g' ;\
		  		 echo \
		  ) \
		) | tee -a pylint.out; \
	done ; \
	test "`cat pylint.out | wc -c`" = 0 || false && rm -f pylint.out


viz:
	gource -s 0.1 -i 0 --title sinon --max-files 99999 --max-file-lag -1 --user-friction 0.3 --user-scale 0.5 --camera-mode overview --highlight-users --hide progress,filenames -r 25 -viewport 1024x1024

clean:
	-rm -rf build/ src/sinon.egg-info/ temp/ MANIFEST dist/ src/sinon/VERSION pylint.out *.egg
	make -C doc clean
	find . -name \*.pyc -exec rm -f {} \;

# pages: gh-pages
# 
# gh-pages:
# 	make clean
# 	make doc
# 	git add -f doc/build/html/*
# 	git add -f doc/build/html/*/*
# 	git add -f doc/build/doctrees/*
# 	git add -f doc/build/doctrees/*/*
# 	git add -f doc/source/*
# 	git add -f doc/source/*/*
# 	git  ci -m 'regenerate documentation'
# 	git co gh-pages
# 	git rebase devel
# 	git co devel
# 	git push --all
# 
