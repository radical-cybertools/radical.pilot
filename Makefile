
.PHONY: all doc clean

all: doc

doc:
	make -C doc html

test:
	nosetests tests.restapi --config=tests/nose.cfg

copyright:

pylint:
	@for f in `find sinon -name \*.py`; do \
	  res=`pylint -r n -f text $$f 2>&1 | grep -e '^[FE]'` ;\
		test -z "$$res" || ( \
		     echo '----------------------------------------------------------------------' ;\
		     echo $$f ;\
		     echo '-----------------------------------'   ;\
				 echo $$res | sed -e 's/ \([FEWRC]:\)/\n\1/g' ;\
				 echo \
		) \
	done | tee pylint.out;\
	test "`cat pylint.out | wc -c`" = 0 || false && true

viz:
	gource -s 0.1 -i 0 --title sinon --max-files 99999 --max-file-lag -1 --user-friction 0.3 --user-scale 0.5 --camera-mode overview --highlight-users --hide progress,filenames -r 25 -viewport 1024x1024

clean:
	-rm -rf build/ saga.egg-info/ temp/ MANIFEST dist/ saga_pilot.egg-info
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
