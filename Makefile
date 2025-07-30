install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	flake8 .

test:
	pytest

package:
	python scripts/package_submission.py --dry-run || echo "package script placeholder"

simulate-run:
	python main.py --mock-input data/mock_input.txt || echo "main script placeholder"
