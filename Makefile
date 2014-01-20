
.PHONY: clean

clean:
	-rm -rf build/ temp/ MANIFEST dist/ sinon/VERSION VERSION pylint.out *.egg
	find . -name \*.pyc -exec rm -f {} \;
	find . -name \*sinon.egg-info -exec rm -f {} \;


-include Makefile.andre

