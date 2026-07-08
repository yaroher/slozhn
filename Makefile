SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

# Publish order matters: dependencies first.
CRATES := slozhn-proto slozhn-frame slozhn-ws slozhn-client slozhn-session slozhn-server slozhn

.PHONY: help gen test test-wasm test-browser doc release publish-crates

help:
	@echo "make gen           - regenerate crates/slozhn-proto via easyp"
	@echo "make test          - cargo test + clippy (native)"
	@echo "make test-wasm     - wasm32 build + clippy"
	@echo "make test-browser  - browser e2e (wasm-pack, headless chrome)"
	@echo "make doc           - cargo doc"
	@echo "make release       - interactive tag-driven release (crates.io + npm share one version)"
	@echo "make publish-crates- publish all crates to crates.io in dependency order (used by CI)"

# --- codegen -----------------------------------------------------------------

gen:
	easyp generate

# --- test --------------------------------------------------------------------

test:
	cargo test --workspace
	cargo clippy --workspace --all-targets -- -D warnings

test-wasm:
	cargo build --target wasm32-unknown-unknown -p slozhn -p echo-wasm -p browser-app-core
	cargo clippy --target wasm32-unknown-unknown -p slozhn -p slozhn-ws -p slozhn-client -p slozhn-session -p echo-wasm -- -D warnings

test-browser:
	./examples/echo-wasm/run-browser-tests.sh

doc:
	cargo doc --workspace --no-deps

# --- publish (CI) ------------------------------------------------------------
# cargo publish waits for crates.io index propagation since 1.66, so a plain
# sequential loop is enough. Requires CARGO_REGISTRY_TOKEN in the environment.

publish-crates:
	@for crate in $(CRATES); do
	  echo "── publishing $$crate ──"
	  cargo publish -p "$$crate" --no-verify
	done

# --- release -----------------------------------------------------------------
# One version drives everything: the workspace is tagged vX.Y.Z; all internal
# crate versions (and npm workspaces, once they exist) are bumped to the same
# X.Y.Z. Pushing the tag triggers .github/workflows/release.yml, which re-runs
# CI, creates a GitHub release, and publishes to crates.io (and npm).

release:
	@set -euo pipefail
	cd "$$(git rev-parse --show-toplevel)"

	if [ -n "$$(git status --porcelain)" ]; then
	  echo "✗ Working tree is not clean — commit or stash first:"
	  git status --short
	  exit 1
	fi

	cur="$$(git tag -l 'v[0-9]*.[0-9]*.[0-9]*' | sed 's/^v//' | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)"
	cur="$${cur:-0.0.0}"
	manifest_cur="$$(sed -n 's/^version = "\(.*\)"$$/\1/p' Cargo.toml | head -1)"
	head="$$(git rev-parse --short HEAD)"
	echo "Latest release: v$$cur    manifest: $$manifest_cur    HEAD: $$head"
	echo
	echo "  1) bump version"
	echo "  2) recreate last tag (v$$cur) on HEAD   [force]"
	echo "  3) cancel"
	read -r -p "> " action

	set_version() { # $$1 = new version (no v prefix); syncs workspace + internal dep reqs + npm workspaces
	  new="$$1"
	  old="$$manifest_cur"
	  sed -i "0,/^version = \"$$old\"$$/s//version = \"$$new\"/" Cargo.toml
	  sed -i "/slozhn/s/version = \"$$old\"/version = \"$$new\"/g" crates/*/Cargo.toml
	  easyp generate  # regenerate slozhn-proto Cargo.toml from the template
	  if [ -f package.json ] && grep -q '"workspaces"' package.json; then
	    npm version "$$new" --no-git-tag-version --workspaces >/dev/null
	  fi
	  cargo check --workspace --quiet  # sanity: versions consistent
	}

	case "$$action" in
	1)
	  IFS=. read -r MA MI PA <<< "$$cur"
	  echo
	  echo "  1) major  -> v$$((MA+1)).0.0"
	  echo "  2) minor  -> v$$MA.$$((MI+1)).0"
	  echo "  3) patch  -> v$$MA.$$MI.$$((PA+1))"
	  read -r -p "> " comp
	  case "$$comp" in
	    1) MA=$$((MA+1)); MI=0; PA=0 ;;
	    2) MI=$$((MI+1)); PA=0 ;;
	    3) PA=$$((PA+1)) ;;
	    *) echo "Aborted."; exit 0 ;;
	  esac
	  new="$$MA.$$MI.$$PA"
	  echo
	  echo "Release v$$new — will:"
	  echo "  - set version $$new across the cargo workspace (and npm workspaces, if any)"
	  echo "  - commit 'release v$$new'"
	  echo "  - create tag v$$new and push HEAD + tag (triggers CI release)"
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }

	  set_version "$$new"
	  git add -A
	  git diff --cached --quiet || git commit -m "release v$$new"
	  git tag -a "v$$new" -m "v$$new"
	  git push origin HEAD
	  git push origin "v$$new"
	  echo "✓ Released v$$new."
	  ;;
	2)
	  if [ "$$cur" = "0.0.0" ] && ! git tag -l 'v0.0.0' | grep -q .; then
	    echo "✗ No release tag to recreate."; exit 1
	  fi
	  echo
	  echo "Will DELETE and recreate tag v$$cur on $$head, then force-push."
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }
	  git tag -d "v$$cur" 2>/dev/null || true
	  git push origin ":refs/tags/v$$cur" 2>/dev/null || true
	  git tag -a "v$$cur" -m "v$$cur"
	  git push origin --force "v$$cur"
	  echo "✓ Recreated v$$cur on $$head."
	  ;;
	*)
	  echo "Cancelled."
	  ;;
	esac
