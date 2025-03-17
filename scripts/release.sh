#!/usr/bin/env bash

set -e

REPOSITORY="${REPOSITORY:-git@github.com:etcd-io/etcd-operator.git}"
REGISTRY="${REGISTRY:-gcr.io/etcd-development/etcd-operator}"
CONTAINER_TOOL="${CONTAINER_TOOL:-docker}"

function main {
  if [ -z "$1" ]; then
    cat <<EOF
Usage: $0 [release_version]

  Arguments:
    release_version can be specified with our without the "v" prefix (i.e., v0.1.0 or 0.1.0)
EOF
    exit
  fi

  local version="v${1#v}"
  local image="${REGISTRY}:${version}"

  build_and_push_image "${image}"
  build_and_commit_dist_files "${version}" "${image}"

  if ! tag_repository "$version"; then
    echo "Failed to tag repository"
    exit 1
  fi
}

function build_and_push_image {
  if "${CONTAINER_TOOL}" manifest inspect "${image}" &>/dev/null; then
    echo "Skipping pushing container image. Image ${image} already exists."
    return
  fi
  echo "Building and pushing container image ${image}"
  make docker-buildx IMG="${image}"
}

function build_and_commit_dist_files {
  local version="$1"
  local image="$2"

  echo "Building distribution YAML and committing to repository"
  make build-installer IMG="${image}" VERSION_SUFFIX="-${version}"
  git checkout -- config/manager/kustomization.yaml
  git add "dist/install-${version}.yaml"
  if git diff --cached --exit-code; then
    echo "Skipping commiting distribution files. No changes to commit."
    return
  fi
  git commit --signoff --message "Release ${version} distribution files"
  git push -f "${REPOSITORY}"
}

function get_gpg_key {
  local git_email
  local key_id

  git_email=$(git config --get user.email)
  key_id=$(gpg --list-keys --with-colons "${git_email}" | \
    awk -F: '/^pub:/ { print $5 }')
  if [[ -z "${key_id}" ]]; then
    echo "Failed to load gpg key. Is gpg set up correctly for etcd releases?"
    return 2
  fi
  echo "${key_id}"
}

function tag_repository {
  local version="$1"

  if [ "$(git tag --list | grep -c "${version}")" -gt 0 ]; then
    echo "Skipping tag step. git tag ${RELEASE_VERSION} already exists."
  else
    echo "Tagging release..."
    local key_id
    key_id=$(get_gpg_key) || return 2
    git tag --local-user "${key_id}" --sign "${version}" --message "${version}"
    git push -f "${REPOSITORY}" "${version}"
  fi
}

main "$@"
