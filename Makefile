DEFAULT_BRANCH ?= main
REMOTE ?= origin

.DEFAULT_GOAL := help

.PHONY: help version release-check check-version check-release-branch check-clean check-tag tag tag-push release

help:
	@printf '%s\n' \
		'Firecube release helpers' \
		'' \
		'Targets:' \
		'  make version                  Print pyproject.toml package version' \
		'  make release-check VERSION=X  Validate local release preconditions' \
		'  make tag VERSION=X            Create annotated tag vX' \
		'  make tag-push VERSION=X       Push existing tag vX' \
		'  make release VERSION=X        Create and push tag vX from main'

version:
	@python3 -c 'import pathlib, tomllib; print(tomllib.loads(pathlib.Path("pyproject.toml").read_text())["project"]["version"])'

release-check: check-version check-release-branch check-clean check-tag

check-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "ERROR: VERSION is required, for example VERSION=0.1.1" >&2; \
		exit 1; \
	fi
	@case "$(VERSION)" in \
		v*) echo "ERROR: VERSION must be bare package version, not v-prefixed: $(VERSION)" >&2; exit 1;; \
	esac
	@pyproject_version=$$(python3 -c 'import pathlib, tomllib; print(tomllib.loads(pathlib.Path("pyproject.toml").read_text())["project"]["version"])'); \
	if [ "$$pyproject_version" != "$(VERSION)" ]; then \
		echo "ERROR: VERSION=$(VERSION) does not match pyproject.toml version $$pyproject_version" >&2; \
		exit 1; \
	fi

check-release-branch:
	@branch=$$(git branch --show-current); \
	if [ "$$branch" != "$(DEFAULT_BRANCH)" ]; then \
		echo "ERROR: releases must be tagged from $(DEFAULT_BRANCH), current branch is $$branch" >&2; \
		exit 1; \
	fi
	@git fetch --quiet "$(REMOTE)" "+refs/heads/$(DEFAULT_BRANCH):refs/remotes/$(REMOTE)/$(DEFAULT_BRANCH)"
	@local_head=$$(git rev-parse HEAD); \
	remote_head=$$(git rev-parse "$(REMOTE)/$(DEFAULT_BRANCH)"); \
	if [ "$$local_head" != "$$remote_head" ]; then \
		echo "ERROR: local $(DEFAULT_BRANCH) is not equal to $(REMOTE)/$(DEFAULT_BRANCH)" >&2; \
		echo "Run: git pull --ff-only $(REMOTE) $(DEFAULT_BRANCH)" >&2; \
		exit 1; \
	fi

check-clean:
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "ERROR: working tree must be clean before tagging a release" >&2; \
		git status --short >&2; \
		exit 1; \
	fi

check-tag:
	@if git rev-parse -q --verify "refs/tags/v$(VERSION)" >/dev/null; then \
		echo "ERROR: local tag v$(VERSION) already exists" >&2; \
		exit 1; \
	fi
	@status=0; \
	git ls-remote --exit-code --tags "$(REMOTE)" "refs/tags/v$(VERSION)" >/dev/null 2>&1 || status=$$?; \
	if [ "$$status" -eq 0 ]; then \
		echo "ERROR: remote tag v$(VERSION) already exists on $(REMOTE)" >&2; \
		exit 1; \
	fi; \
	if [ "$$status" -ne 2 ]; then \
		echo "ERROR: could not query remote tags from $(REMOTE)" >&2; \
		exit "$$status"; \
	fi

tag: release-check
	@git tag -a "v$(VERSION)" -m "Release v$(VERSION)"
	@echo "Created tag v$(VERSION)"

tag-push: check-version check-release-branch
	@if ! git rev-parse -q --verify "refs/tags/v$(VERSION)" >/dev/null; then \
		echo "ERROR: local tag v$(VERSION) not found. Run: make tag VERSION=$(VERSION)" >&2; \
		exit 1; \
	fi
	@git push "$(REMOTE)" "v$(VERSION)"

release:
	@$(MAKE) tag VERSION="$(VERSION)" DEFAULT_BRANCH="$(DEFAULT_BRANCH)" REMOTE="$(REMOTE)"
	@$(MAKE) tag-push VERSION="$(VERSION)" DEFAULT_BRANCH="$(DEFAULT_BRANCH)" REMOTE="$(REMOTE)"
