PYTHON_VERSION := 3.11
AIRFLOW_VERSION := 2.8.1
# Must be comma-separated, no spaces
AIRFLOW_EXTRAS := postgres
CONSTRAINT := https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

ENV := dev  # default environment

ifeq ($(ENV),prd)
COMPOSE_FILE := airflow/docker-compose.prd.yaml
else
COMPOSE_FILE := airflow/docker-compose.dev.yaml
endif

# Recreates your virtualenv from scratch, assuming you have python3 installed
.PHONY: venv
venv:
ifneq ($(wildcard ./venv/.*),)
	@echo "venv already exists"
else
	python3 -m virtualenv venv
	venv/bin/python3 -m pip install --upgrade pip
	venv/bin/python3 -m pip install --trusted-host pypi.python.org "apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}" --constraint ${CONSTRAINT}  --use-deprecated legacy-resolver
	venv/bin/python3 -m pip install -r airflow.requirements.txt --use-deprecated legacy-resolver
	venv/bin/python3 -m pip install -r local.requirements.txt --use-deprecated legacy-resolver
endif


# Runs all tests
.PHONY: test
test: venv
	@( \
		export AIRFLOW_HOME=${PWD}/airflow; \
		source venv/bin/activate; \
		pytest tests --log-cli-level=info --disable-warnings; \
	)

test-docker:
	@docker-compose -f $(COMPOSE_FILE) up test

# Gets rid of junk from running pytest locally
clean-pytest:
	rm -rf *.cfg airflow.db logs .pytest_cache

# Cleans your virtualenv, run make venv to recreate it
clean-venv:
	rm -rf venv plugins.egg-info

# Cleans everything
clean-all: clean-pytest clean-venv reset-airflow

# starts Postgres
start-db:
	@docker-compose -f $(COMPOSE_FILE) up -d postgres
	@docker-compose -f $(COMPOSE_FILE) up initdb

# starts Airflow
start-airflow: start-db
	@docker-compose -f $(COMPOSE_FILE) up data_processing webserver scheduler


# stops Airflow
stop-airflow:
	@docker-compose -f $(COMPOSE_FILE) down 

# Resets local Airflow, removes all docker volumes and stopped containers
reset-airflow:
	@docker-compose -f $(COMPOSE_FILE) down -v || true
	@docker-compose -f $(COMPOSE_FILE) rm -f
	rm -f webserver_config.py

# Rebuilds all docker-compose images 
rebuild-airflow:
	@docker-compose build

### DO NOT RUN THESE STEPS BY HAND
### The below steps are used inside the Dockerfile and/or docker-compose, they are not meant to be run locally

internal-install-airflow:
	pip install --no-cache-dir --trusted-host pypi.python.org "apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}" --constraint ${CONSTRAINT}

internal-install-deps: 
	pip install --no-cache-dir -r airflow.requirements.txt

internal-install-local-deps:
	pip install --no-cache-dir -r local.requirements.txt

internal-test: internal-install-local-deps
	pytest tests --log-cli-level=info --disable-warnings
