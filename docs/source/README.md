# RADICAL-Pilot (RP) documentation

RP's documentation uses [Sphinx](https://www.sphinx-doc.org/en/master/index.html "Python documentation generator") and it includes [Jupyter notebooks](https://jupyter.org/ "Interactive computing") that execute when compiling the documentation.

## Compiling the documentation

1. Clone the RP's GitHub repository on a GNU/Linux system:

  ```shell
  git clone git@github.com:radical-cybertools/radical.pilot.git
  cd radical.pilot
  ```

2. Create a suitable Python virtual environment. For example:

  ```shell
  python -m venv ~/.ve/rp-docs
  . ~/.ve/rp-docs/bin/activate
  pip install --upgrade pip
  pip install -r requirements-docs.txt
  pip install jupyter
  ```

3. Clean and compile all the notebooks as Read the Docs' containers do not have enough resources to run RADICAL-Pilot

  ```shell
  cd docs/source
  for n in `find ./ -name "*.ipynb" -type f`; do find $HOME/radical.pilot.sandbox/ -type d -name 'rp.session.three.*' -prune -exec rm -rf {} +; find . -type d -name 'rp.session.three.*' -prune -exec rm -rf {} +; jupyter nbconvert --clear-output --inplace $n; jupyter nbconvert --to notebook --execute --inplace $n; done
  ```

4. Generate RP's documentation with Sphinx:

  ```shell
  cd docs
  sphinx-build source _build -b html
  ```

## Publishing on Read the Docs

- Become an admin of RP's documentation on Read the Docs
- Configure the build at [Readthedocs](https://readthedocs.org/dashboard/)

### Configuration files

- `docs/source/conf.py`: `Sphinx` configuration file
- `requirements-docs.txt`: Python environment requirements
- [`.readthedocs`](https://docs.readthedocs.io/en/stable/config-file/v2.html): Read the Docs environment configuration file

### Settings

- Name and Repository URL
- Description
- Project Homepage
- Branch name
- Requirements File: relative path to requirements file
- Documentation Type: Select `Sphinx Html`
