COMPOSE_BASE := docker-compose
LOG_FILE := build.log

init-venv:
	python3 -m venv .venv

pytest:
	pytest -v fastapi;

build-up:
	$(COMPOSE_BASE)	up --build --remove-orphans;

up:	
	$(COMPOSE_BASE) up;

restart:	
	$(COMPOSE_BASE) restart;

down:
	$(COMPOSE_BASE) down;

recreate:
	$(COMPOSE_BASE) up --build -d --force-recreate
