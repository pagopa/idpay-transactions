# Implementation plan: release-gated PostgreSQL migrations on AKS

## Status

Proposed.

## Objective

Add a production-capable PostgreSQL schema migration mechanism without
coupling the running `idpay-transactions` Spring application to Flyway, JDBC,
or schema-changing startup logic.

The migration must:

- run exactly once per ArgoCD synchronization, with database locking as a
  second line of defence;
- run before the new application Deployment is promoted;
- use a migration image built from the same application commit;
- use a dedicated migration database identity and Key Vault-backed secrets;
- fail the `idpay-transactions` deployment when the migration fails;
- preserve compatibility with the old pods that remain during a rolling
  Deployment update;
- support the existing migration audit/cutover gate around migrations `003`
  and `004`;
- leave the application runtime R2DBC-only and without DDL privileges.

## Actual deployment topology

The deployment is not rendered from the `helm/` directory in this repository.
The ArgoCD Application defined in
`cstar-securehub-infra/src/70_domains/idpay_app/30_argocd_domain.tf` renders
the chart from `pagopa/idpay-deploy-aks`:

```text
helm/${environment}/top/idpay-transactions
```

It also loads:

```text
helm/_global/idpay-transactions.yaml
```

The application uses `microservice-chart` version `8.1.1`. The Azure DevOps
deployment pipeline updates the ArgoCD revision, runs `argocd app sync`, waits
for health, and then restarts Deployments.

`idpay-transactions` is not configured for ArgoCD automated sync in
`30_argocd_domain.tf`; the pipeline explicitly invokes synchronization.

## Repository ownership

| Repository | Responsibility |
| --- | --- |
| `pagopa/idpay-transactions` | Canonical SQL files, migration image, migration-image build/publish, SQL validation, migration documentation, and application runtime configuration |
| `pagopa/idpay-deploy-aks` | ArgoCD PreSync Job, Key Vault `SecretProviderClass`, environment values, image references, Helm chart versions, and strict migration-aware deployment synchronization |
| `pagopa/cstar-securehub-infra` | PostgreSQL database/roles, Key Vault secrets, grants, network access, and existing Workload Identity wiring |
| `pagopa/aks-microservice-chart-blueprint` | No change initially; the local application chart adds the migration resources beside the existing dependency |

The application repository must not receive the migration Job template:
ArgoCD does not render that chart.

## Design decisions

### Migration execution

Use a dedicated Flyway CLI image in an ArgoCD `PreSync` Kubernetes Job.

The Job is part of the same ArgoCD Application as the Deployment:

```text
ArgoCD sync
  -> PreSync migration Job
       -> failure: sync fails and new Deployment is not promoted
       -> success: Deployment rolls out
```

The Job is not a CronJob and must not be added to
`cstar-securehub-infra/src/70_domains/idpay_app/09_k8s_cronjobs.tf`.

### Migration history and locking

Use Flyway's `flyway_schema_history` table and migration lock. Do not create a
custom history table or custom SQL parser in the backend.

The migration image must configure:

- validation on migrate;
- `cleanDisabled=true`;
- out-of-order migrations disabled;
- a stable schema and history-table name;
- a connection/lock timeout appropriate for the environment.

PostgreSQL locking remains the database safety mechanism if two deployment
operations overlap. ArgoCD synchronization must also be serialized for this
Application.

### Migration naming

Before any environment applies the files, rename the current numeric files to
the selected migration-tool convention:

```text
V001__create_reward_batches.sql
V002__create_reward_transactions.sql
V003__create_reconciliation_views.sql
V004__derive_reward_batch_aggregates.sql
V005__add_payment_batch_impact_revisions.sql
```

After the first environment applies them, filenames and checksums are
forward-only and must not be rewritten.

### Runtime database access

The application remains R2DBC-only. No Flyway dependency, JDBC migration
driver, `CommandLineRunner`, or startup migration configuration is added to
the Spring application.

The application database identity receives only runtime DML permissions.
The migration identity receives schema-changing permissions and is used only
by the Kubernetes Job.

## Phase 0: resolve production database contracts

### `cstar-securehub-infra`

Update the PostgreSQL provisioning in
`src/70_domains/idpay_common/03_postgres.tf` to make the database contract
explicit.

1. Confirm the canonical database name. The current Key Vault connection
   string uses `idpay-database`, while the application local default uses
   `idpay-transactions`. Production must use one name consistently.
2. Create an application role, for example
   `idpay_transactions_app`, with:
   - `CONNECT` on the database;
   - schema usage;
   - DML permissions on application tables and sequences;
   - no schema ownership or DDL permissions.
3. Create a migration role, for example
   `idpay_transactions_migrator`, with the permissions needed to create and
   alter the application schema.
4. Store the application and migration credentials in the environment Key
   Vault.
5. Store separate JDBC and R2DBC connection values if the existing
   `idpay-postgres-connection-string` cannot be reused.
6. Ensure the AKS subnet/workload can reach the private PostgreSQL endpoint.

Prefer Terraform-managed roles and grants if the infrastructure runner can
reach the private PostgreSQL server. If it cannot, implement a one-time,
audited bootstrap operation using the existing administrator credentials, then
remove administrator credentials from workload configuration.

Required Key Vault objects should be named consistently, for example:

```text
idpay-postgres-r2dbc-url
idpay-postgres-app-user
idpay-postgres-app-password
idpay-postgres-jdbc-url
idpay-postgres-migration-user
idpay-postgres-migration-password
```

The existing administrator secrets remain infrastructure bootstrap secrets and
must not be mounted into the application Deployment.

### `cstar-securehub-infra/src/70_domains/idpay_app/30_argocd_domain.tf`

No new ArgoCD Application is required. Keep `idpay-transactions` in the
existing `top` application set and continue to pass:

- `microservice-chart.azure.workloadIdentityClientId`;
- `microservice-chart.serviceAccount.name`.

The custom migration Job will reuse these values for Azure Workload Identity.
Only change this Terraform file if the platform team chooses to create a
separate migration Application instead of a Job inside the existing
Application. That is not the default design.

## Phase 1: prepare migration artifacts

### `pagopa/idpay-transactions`

1. Rename the SQL files under
   `src/main/resources/db/migration/` to the Flyway convention.
2. Update migration documentation and any file-order assumptions in tests.
3. Keep the existing Testcontainers fixture for empty-schema SQL tests, but
   add a separate migration-runner validation path that exercises the actual
   Flyway image or CLI.
4. Add `Dockerfile.migration`:

   ```dockerfile
   FROM flyway/flyway:<pinned-version>

   COPY src/main/resources/db/migration/ /flyway/sql/
   ```

5. Pin the Flyway base image by digest.
6. Ensure the migration image:
   - contains only the migration files and migration executable;
   - does not start Spring, Kafka, WebFlux, or application consumers;
   - runs as the same non-root UID expected by the AKS security policy;
   - has a writable temporary directory if required by the Flyway image.

### Build and publish workflows

Update:

```text
.github/workflows/flow-docker-snapshot.yml
.github/workflows/release.yml
```

The current shared Docker action publishes the application image. Extend the
build flow, or add a dedicated authenticated Docker build/push step, to publish
both images from the same commit:

```text
ghcr.io/pagopa/idpay-transactions
ghcr.io/pagopa/idpay-transactions-migrations
```

The migration image tag and digest must be independently recorded but tied to
the application release. A deployment must never select a migration image from
an unrelated commit.

The Anchore workflow must scan the migration image as well as the application
image, or a dedicated equivalent scan must be added.

## Phase 2: add deployment configuration

### Common values

Update:

```text
pagopa/idpay-deploy-aks/helm/_global/idpay-transactions.yaml
```

Add top-level values beside `microservice-chart`:

```yaml
schemaMigration:
  enabled: false

  image:
    repository: ghcr.io/pagopa/idpay-transactions-migrations
    tag: ""
    pullPolicy: IfNotPresent

  keyvault:
    enabled: true
    secretProviderClassName: idpay-transactions-schema-migration
    jdbcUrlSecret: idpay-postgres-jdbc-url
    usernameSecret: idpay-postgres-migration-user
    passwordSecret: idpay-postgres-migration-password

  activeDeadlineSeconds: 900
  backoffLimit: 1
  ttlSecondsAfterFinished: 86400

  resources:
    requests:
      cpu: 50m
      memory: 128Mi
    limits:
      cpu: 250m
      memory: 256Mi
```

The migration Job must have its own Key Vault object list. Do not rely on the
`microservice-chart` SecretProviderClass: the existing Deployment may not have
mounted it yet when a PreSync Job runs for the first time.

The Job must also receive the same scheduling constraints as the application:

- `idpayOnly` toleration;
- `domain=idpay` node affinity;
- non-root security context;
- image pull secret if the migration registry is private.

### Environment values

Update:

```text
helm/dev/top/idpay-transactions/values.yaml
helm/uat/top/idpay-transactions/values.yaml
helm/prod/top/idpay-transactions/values.yaml
```

For each environment, set:

- `schemaMigration.enabled`;
- migration image tag and digest;
- migration deadline;
- any environment-specific Key Vault or scheduling overrides.

The production image must be digest-pinned, following the existing pattern:

```yaml
microservice-chart:
  image:
    tag: v1.18.2@sha256:<application-digest>

schemaMigration:
  enabled: true
  image:
    tag: v1.18.2@sha256:<migration-image-digest>
```

The application `microservice-chart.envSecret` must be extended with the
runtime R2DBC URL, username, and password Key Vault object names once the
infrastructure secrets are available.

### Helm chart resources

Add a local template to each environment chart:

```text
helm/dev/top/idpay-transactions/templates/schema-migration-secrets.yaml
helm/dev/top/idpay-transactions/templates/schema-migration-job.yaml
helm/uat/top/idpay-transactions/templates/schema-migration-secrets.yaml
helm/uat/top/idpay-transactions/templates/schema-migration-job.yaml
helm/prod/top/idpay-transactions/templates/schema-migration-secrets.yaml
helm/prod/top/idpay-transactions/templates/schema-migration-job.yaml
```

The templates may later be extracted into a reusable deployment chart. For the
first implementation, keeping the resources local avoids changing
`microservice-chart` version `8.1.1`.

Add a `SecretProviderClass` equivalent to the mechanism used by the blueprint
chart:

```yaml
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: idpay-transactions-schema-migration
  namespace: idpay
spec:
  provider: azure
  secretObjects:
    - secretName: idpay-transactions-schema-migration
      type: Opaque
      data:
        - objectName: idpay-postgres-jdbc-url
          key: FLYWAY_URL
        - objectName: idpay-postgres-migration-user
          key: FLYWAY_USER
        - objectName: idpay-postgres-migration-password
          key: FLYWAY_PASSWORD
  parameters:
    usePodIdentity: "false"
    clientID: <microservice-chart workload identity client id>
    keyvaultName: <microservice-chart key vault name>
    tenantId: <microservice-chart key vault tenant id>
    objects: |
      array:
        - |
          objectName: idpay-postgres-jdbc-url
          objectType: secret
        - |
          objectName: idpay-postgres-migration-user
          objectType: secret
        - |
          objectName: idpay-postgres-migration-password
          objectType: secret
```

The Job must mount the CSI volume and read the files from
`/mnt/secrets-store`. Do not use `secretKeyRef` from the synced Kubernetes
Secret as the only mechanism: the SecretProviderClass creates/synchronizes the
Secret when the volume is mounted, which can race with environment-variable
initialization.

The Job command should load the mounted values and invoke Flyway:

```yaml
command:
  - /bin/sh
  - -ec
args:
  - |
    export FLYWAY_URL="$(cat /mnt/secrets-store/FLYWAY_URL)"
    export FLYWAY_USER="$(cat /mnt/secrets-store/FLYWAY_USER)"
    export FLYWAY_PASSWORD="$(cat /mnt/secrets-store/FLYWAY_PASSWORD)"
    exec /flyway/flyway migrate
```

The Job must include:

- `argocd.argoproj.io/hook: PreSync`;
- `argocd.argoproj.io/sync-wave: "-1"`;
- `argocd.argoproj.io/hook-delete-policy: BeforeHookCreation`;
- `restartPolicy: Never`;
- `backoffLimit`;
- `activeDeadlineSeconds`;
- resources;
- Workload Identity label
  `azure.workload.identity/use: "true"`;
- the existing Workload Identity service account;
- the CSI volume and mount;
- `FLYWAY_VALIDATE_ON_MIGRATE=true`;
- `FLYWAY_CLEAN_DISABLED=true`;
- `FLYWAY_OUT_OF_ORDER=false`;
- a stable history table/schema configuration.

Use a stable Job name with `BeforeHookCreation` so a new sync removes the
previous completed or failed Job before creating the next one. Retain failed
Job logs until the next synchronization or an agreed TTL.

### Chart metadata

Bump the chart version in:

```text
helm/dev/top/idpay-transactions/Chart.yaml
helm/uat/top/idpay-transactions/Chart.yaml
helm/prod/top/idpay-transactions/Chart.yaml
```

The dependency on `microservice-chart` remains at version `8.1.1`.

## Phase 3: make ArgoCD failure observable and blocking

### `pagopa/idpay-deploy-aks/.devops/templates/deploy-argo-template.yml`

The current template:

- uses `set +e`;
- converts sync/health failures into warnings;
- uses `continueOnError: true`;
- has a default sync timeout of 180 seconds.

That behavior would allow a failed migration hook to produce a misleading
successful deployment pipeline. Change the template to support strict
applications:

1. Add a `strictApplications` parameter, defaulting to an empty list.
2. Add a migration-capable sync timeout, at least 900 seconds.
3. Determine strict mode from the matrix `appName`.
4. For strict applications:
   - return a non-zero status from `argocd app sync` failures;
   - return a non-zero status from health-check failures;
   - do not convert failures to `SucceededWithIssues`;
   - do not execute the post-sync Deployment restart.
5. Preserve the current warning behavior for applications not in the strict
   list until a broader pipeline policy is approved.
6. Remove unconditional `continueOnError: true`, or make the script return
   zero only for explicitly non-strict warning paths.

The sync timeout must exceed the maximum migration Job deadline. The current
180-second default is insufficient for a 900-second migration Job.

### `pagopa/idpay-deploy-aks/.devops/deploy-argocd-apps.yml`

Pass `idpay-transactions` as a strict application to the deployment template.
Keep it in the existing `APPS_TOP` list so it is still deployed by the
current GitHub-to-Azure-DevOps trigger:

```yaml
strictApplications:
  - idpay-transactions
```

Ensure the Azure DevOps pipeline waits for the ArgoCD sync result and does not
continue to Postman tests when the strict migration deployment fails.

## Phase 4: migration lifecycle and cutover

### Normal releases after cutover

For migrations that are backward-compatible with the current running pods:

```text
commit SQL and application changes
build application and migration images
update deploy-aks image references
ArgoCD sync
  -> migration Job applies pending versions
  -> Deployment rolls out
  -> Argo health check completes
  -> deployment restart action runs
```

The old pods remain active while the PreSync Job runs. All normal migrations
must therefore use expand/contract sequencing.

### Initial SQL cutover

Do not enable an unconditional migrate-to-latest Job before the external data
cutover is complete.

Use the following controlled sequence:

```text
1. Provision PostgreSQL, roles, grants, and Key Vault secrets.
2. Apply schema/audit migrations through V003.
3. Load the legacy data externally.
4. Run reconciliation and quarantine invalid records.
5. Require reward_batch_counter_mismatches to be empty.
6. Apply the contract/cutover migration V004.
7. Apply V005 and any final schema changes.
8. Enable the SQL application configuration.
9. Deploy the final SQL-capable application.
10. Enable steady-state PreSync migrations.
```

Migration `V004` must be a separately approved cutover operation because it
drops audit views and legacy counter columns. It must not be applied blindly
as a normal pod deployment migration.

If V004 is incompatible with the currently running application, quiesce all
writers and scheduled callers before applying it. This includes relevant
application pods and the reward-batch CronJobs defined in
`cstar-securehub-infra/src/70_domains/idpay_app/09_k8s_cronjobs.tf`.

## Phase 5: compatibility and failure policy

### Compatibility rules

- Additive tables and nullable columns may run in a PreSync Job.
- Renames require dual-read/dual-write or a compatibility view.
- Tightened constraints require backfill and validation before enforcement.
- Large indexes should use online PostgreSQL procedures where applicable.
- Dropping columns is a later contract release only.
- New application code must not require a schema change that breaks old pods.

The migration Job does not remove the need for compatibility with the old
Deployment.

### Failure behavior

| Failure | Expected behavior |
| --- | --- |
| Database unavailable | PreSync Job retries, then Argo sync fails; old pods remain |
| Migration SQL failure | Job fails; strict Azure DevOps deployment fails |
| Flyway checksum mismatch | Job fails and requires operator remediation |
| Migration timeout | Job fails; no new Deployment promotion |
| Application rollout failure after migration | Roll back application code only if schema remains compatible; do not automatically run down migrations |
| Partial non-transactional migration | Use Flyway repair/forward migration under operator control |

No automatic database down-migration is part of this feature.

## Phase 6: validation and tests

### `pagopa/idpay-transactions`

Add or update validation for:

- migration file naming and ordering;
- all SQL migrations applied to an empty PostgreSQL Testcontainer;
- Flyway history creation and checksum validation;
- repeated migration execution being a no-op;
- concurrent migration attempts being serialized;
- V003 reconciliation views;
- the V004 precondition/cutover procedure;
- generated jOOQ sources remaining synchronized with the final schema;
- the application remaining free of Flyway/JDBC runtime dependencies.

### `pagopa/idpay-deploy-aks`

Run Helm validation for every environment:

```text
helm dependency build helm/<env>/top/idpay-transactions
helm lint helm/<env>/top/idpay-transactions \
  -f helm/_global/idpay-transactions.yaml \
  -f helm/<env>/top/idpay-transactions/values.yaml
helm template ...
```

Verify the rendered resources include:

- the SecretProviderClass;
- the PreSync Job;
- the Workload Identity label/service account;
- the correct image digest;
- the correct environment Key Vault name;
- the correct database URL secret;
- the idpay node toleration and affinity.

Add a CI check that fails if the Job is accidentally absent from any
environment chart.

### `pagopa/cstar-securehub-infra`

Validate:

- PostgreSQL role and grant creation;
- Key Vault secret creation;
- Workload Identity access to the new secret objects;
- private DNS/network reachability from AKS;
- database name consistency with the deployment values.

The existing ArgoCD Terraform should continue to plan without creating a
second `idpay-transactions` Application.

### Integration verification

In a non-production environment:

1. Start from an empty PostgreSQL database.
2. Trigger an Argo sync.
3. Confirm the Job completes before the Deployment changes.
4. Trigger a second sync and confirm no SQL migration is reapplied.
5. Run two sync attempts or two Job instances and confirm Flyway locking.
6. Inject invalid credentials and confirm:
   - the Job fails;
   - the strict pipeline fails;
   - the old application remains deployed.
7. Confirm migration logs do not contain database passwords.
8. Confirm a compatible rolling deployment keeps the old pods healthy.

## Delivery sequence

### Pull request 1: migration artifact

Repository: `pagopa/idpay-transactions`

- rename migration files;
- add migration image;
- add image build/publish workflow;
- add migration-image security scan;
- update migration documentation and tests.

### Pull request 2: infrastructure contract

Repository: `pagopa/cstar-securehub-infra`

- align database name;
- provision application and migration roles;
- provision grants;
- add Key Vault secrets;
- verify Workload Identity access and network reachability.

### Pull request 3: deployment resources

Repository: `pagopa/idpay-deploy-aks`

- add global migration values;
- add environment image references;
- add SecretProviderClass and PreSync Job templates;
- bump environment chart versions;
- add Helm rendering/lint validation.

### Pull request 4: deployment failure gate

Repository: `pagopa/idpay-deploy-aks`

- add strict application support to the Argo deployment template;
- increase the effective timeout for `idpay-transactions`;
- pass `idpay-transactions` as a strict application;
- prevent Postman execution after a strict migration failure.

### Operational cutover

- apply V001-V003;
- perform external backfill and reconciliation;
- approve and apply V004/V005;
- deploy the final SQL application;
- enable normal PreSync migration execution.

## Completion criteria

The feature is complete when:

- both images are published from the same application commit;
- each environment renders a migration SecretProviderClass and PreSync Job;
- the Job obtains secrets through the existing Workload Identity model;
- the migration runs before the Deployment and is serialized by Flyway;
- the migration role is separate from the application runtime role;
- a failed migration fails the Argo/Azure DevOps deployment;
- the old application remains available for compatible migrations;
- `V004` cannot be applied without the approved cutover procedure;
- the application contains no startup migration dependency;
- the deployment repository, infrastructure repository, and application
  repository each own only the changes listed above;
- empty-database, repeat, concurrent, failure, and rolling-compatibility
  scenarios are verified.
