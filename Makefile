init:
	pip install -r requirements.txt

test:
	python -m unittest discover -s tests/

coverage:
	coverage run -m unittest discover -s tests/
	coverage html

codestyle:
	pycodestyle . --exclude=".#*" --max-line-length=120

.PHONY: init test coverage codestyle
