.PHONY: maintenance orchestrate cache-prune

maintenance:
	python -m platform.cli maintenance

orchestrate:
	python -m platform.cli orchestrate

cache-prune:
	python -m platform.cli cache-prune --dry-run
