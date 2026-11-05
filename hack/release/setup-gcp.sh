#!/usr/bin/env bash

# Sets up the Google Cloud resources that allow the Release workflow
# (.github/workflows/release.yaml) to push container images, using Workload
# Identity Federation through a Service Account. See RELEASE.md for details.
#
# It must be run by a user with permissions to manage IAM, Workload Identity
# Pools, and Artifact Registry in the Google Cloud project. It can be run
# multiple times, existing resources are updated or left unchanged.

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-etcd-development}"
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-etcd-io/etcd-operator}"
GITHUB_ENVIRONMENT="${GITHUB_ENVIRONMENT:-release}"
POOL_ID="${POOL_ID:-github}"
PROVIDER_ID="${PROVIDER_ID:-etcd-operator-release}"
SERVICE_ACCOUNT_ID="${SERVICE_ACCOUNT_ID:-etcd-operator-release}"
# Artifact Registry repository serving the image registry. The gcr.io
# repository, in the us location, serves gcr.io/${PROJECT_ID}.
REGISTRY_REPOSITORY="${REGISTRY_REPOSITORY:-gcr.io}"
REGISTRY_LOCATION="${REGISTRY_LOCATION:-us}"

function main {
  if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit
  fi

  local project_number
  project_number=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')

  local subject="repo:${GITHUB_REPOSITORY}:environment:${GITHUB_ENVIRONMENT}"
  local service_account="${SERVICE_ACCOUNT_ID}@${PROJECT_ID}.iam.gserviceaccount.com"
  local pool="projects/${project_number}/locations/global/workloadIdentityPools/${POOL_ID}"

  check_registry
  enable_services
  ensure_pool
  ensure_provider "${subject}"
  ensure_service_account
  grant_workload_identity_user "${service_account}" "principal://iam.googleapis.com/${pool}/subject/${subject}"
  grant_registry_writer "${service_account}"

  cat <<EOF

Done. Set the following variables in the ${GITHUB_REPOSITORY} repository:

  gh variable set GCP_WORKLOAD_IDENTITY_PROVIDER --repo "${GITHUB_REPOSITORY}" --body "${pool}/providers/${PROVIDER_ID}"
  gh variable set GCP_SERVICE_ACCOUNT --repo "${GITHUB_REPOSITORY}" --body "${service_account}"

And restrict the "${GITHUB_ENVIRONMENT}" environment to deployments from tags matching v*.
EOF
}

function usage {
  cat <<EOF
Usage: $0

  Sets up the Google Cloud resources used by the Release workflow to push
  container images. Configure it with the following environment variables:

    PROJECT_ID           Google Cloud project (default: ${PROJECT_ID})
    GITHUB_REPOSITORY    GitHub repository running the workflow (default: ${GITHUB_REPOSITORY})
    GITHUB_ENVIRONMENT   GitHub environment of the release job (default: ${GITHUB_ENVIRONMENT})
    POOL_ID              Workload Identity Pool (default: ${POOL_ID})
    PROVIDER_ID          Workload Identity Pool Provider (default: ${PROVIDER_ID})
    SERVICE_ACCOUNT_ID   Service Account (default: ${SERVICE_ACCOUNT_ID})
    REGISTRY_REPOSITORY  Artifact Registry repository (default: ${REGISTRY_REPOSITORY})
    REGISTRY_LOCATION    Artifact Registry repository location (default: ${REGISTRY_LOCATION})
EOF
}

function check_registry {
  if ! gcloud artifacts repositories describe "${REGISTRY_REPOSITORY}" \
      --project="${PROJECT_ID}" --location="${REGISTRY_LOCATION}" &>/dev/null; then
    echo "Artifact Registry repository ${REGISTRY_REPOSITORY} (${REGISTRY_LOCATION}) not found in project ${PROJECT_ID}"
    return 1
  fi
}

function enable_services {
  echo "Enabling required services"
  gcloud services enable \
    iam.googleapis.com \
    sts.googleapis.com \
    iamcredentials.googleapis.com \
    --project="${PROJECT_ID}"
}

function ensure_pool {
  if gcloud iam workload-identity-pools describe "${POOL_ID}" \
      --project="${PROJECT_ID}" --location=global &>/dev/null; then
    echo "Skipping creating Workload Identity Pool ${POOL_ID}. It already exists."
    return
  fi
  echo "Creating Workload Identity Pool ${POOL_ID}"
  gcloud iam workload-identity-pools create "${POOL_ID}" --project="${PROJECT_ID}" \
    --location=global --display-name="GitHub Actions"
}

function ensure_provider {
  local subject="$1"
  local action="create-oidc"

  # Only accept tokens from the release job, which runs in the GitHub
  # environment. Updating an existing provider keeps the condition in sync.
  if gcloud iam workload-identity-pools providers describe "${PROVIDER_ID}" \
      --project="${PROJECT_ID}" --location=global --workload-identity-pool="${POOL_ID}" &>/dev/null; then
    echo "Updating Workload Identity Pool Provider ${PROVIDER_ID}"
    action="update-oidc"
  else
    echo "Creating Workload Identity Pool Provider ${PROVIDER_ID}"
  fi
  gcloud iam workload-identity-pools providers "${action}" "${PROVIDER_ID}" \
    --project="${PROJECT_ID}" --location=global --workload-identity-pool="${POOL_ID}" \
    --display-name="${PROVIDER_ID}" --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
    --attribute-condition="assertion.sub == '${subject}'"
}

function ensure_service_account {
  if gcloud iam service-accounts describe "${SERVICE_ACCOUNT_ID}@${PROJECT_ID}.iam.gserviceaccount.com" \
      --project="${PROJECT_ID}" &>/dev/null; then
    echo "Skipping creating Service Account ${SERVICE_ACCOUNT_ID}. It already exists."
    return
  fi
  echo "Creating Service Account ${SERVICE_ACCOUNT_ID}"
  gcloud iam service-accounts create "${SERVICE_ACCOUNT_ID}" --project="${PROJECT_ID}" \
    --display-name="etcd-operator release"
}

function grant_workload_identity_user {
  local service_account="$1"
  local principal="$2"

  echo "Allowing ${principal} to impersonate ${service_account}"
  gcloud iam service-accounts add-iam-policy-binding "${service_account}" --project="${PROJECT_ID}" \
    --role=roles/iam.workloadIdentityUser --member="${principal}" --condition=None --format=none
}

function grant_registry_writer {
  local service_account="$1"

  echo "Allowing ${service_account} to push to ${REGISTRY_REPOSITORY} (${REGISTRY_LOCATION})"
  gcloud artifacts repositories add-iam-policy-binding "${REGISTRY_REPOSITORY}" \
    --project="${PROJECT_ID}" \
    --location="${REGISTRY_LOCATION}" \
    --role=roles/artifactregistry.writer \
    --member="serviceAccount:${service_account}" \
    --condition=None \
    --format=none
}

main "$@"
