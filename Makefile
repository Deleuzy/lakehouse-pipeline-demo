PY ?= .venv/bin/python

setup:
	python3 -m venv .venv && $(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest -q

run:
	PYTHONPATH=src $(PY) -m lakehouse.pipeline --source sample

.PHONY: setup test run
