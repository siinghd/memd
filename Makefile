.PHONY: install test bench gate docker ten-min clean

install:
	pip install -e ".[dev,mcp]"

test:
	python -m pytest tests/ -q

bench:
	python bench/slo_bench.py

gate:
	python -m memd.harness.run --suite all --gate

ten-min:
	bash scripts/ten_minute_test.sh

docker:
	docker build -t memd/memd .

clean:
	rm -rf .pytest_cache harness-results *.egg-info src/*.egg-info
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
