first-install:
	pip3 install poetry
	export PATH="$$HOME/.local/bin:$$PATH"

	poetry config virtualenvs.create true
	poetry config virtualenvs.in-project true

install:
	poetry install

build:
	poetry build

publish:
	poetry publish --dry-run

package-install:
	python3 -m pip install --user --force-reinstall dist/*.whl

make lint:
	poetry run flake8 happy_days

init-bot:
	~/.local/bin/poetry run init-bot
