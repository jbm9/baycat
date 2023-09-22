init:
	./dev_env_init.sh

test:
	python3 -m unittest discover -s tests/

coverage:
	coverage run --branch -m unittest discover -s tests/
	coverage html

codestyle:
	pycodestyle . --exclude=".#*" --exclude=".venv/*" --max-line-length=120

.PHONY: init test coverage codestyle
