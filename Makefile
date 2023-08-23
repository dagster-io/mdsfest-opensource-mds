help:
	echo "Help!"

clean:
	rm -rf ./data/db/*
	rm -rf ./data/raw/*

install:
	python -m venv myenv
	. myenv/bin/activate
	pip install -e .'[dev]'
	cd dbt_project && npm --prefix ./reports install
	cd ..

run-dagster:
	. myenv/bin/activate
	dagster dev

run-evidence:
	cd dbt_project && npm --prefix ./reports run dev -- --port 4000
