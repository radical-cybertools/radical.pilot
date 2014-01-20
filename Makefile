
.PHONY: clean

clean:
	-rm -rf build/ temp/ MANIFEST dist/ src/sinon.egg-info sinon/VERSION VERSION pylint.out *.egg
	find . -name \*.pyc -exec rm -f {} \;
	find . -name \*.egg-info -exec rm -rf {} \;


-include Makefile.andre

