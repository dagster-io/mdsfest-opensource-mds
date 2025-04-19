help:
	echo "Help!"

clean:
	rm -rf ./data/db/*
	rm -rf ./data/raw/*

install:
	uv venv myenv
	. myenv/bin/activate
	uv pip install -e .'[dev]'

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run dg check defs
	uv run dg check yaml

run-dagster:
	. myenv/bin/activate
	dagster dev
