.PHONY: maintenance orchestrate cache-manage

maintenance:
	python -m platform.cli maintenance

orchestrate:
	python -m platform.cli orchestrator

cache-manage:
	python -m platform.cli cache-manage
