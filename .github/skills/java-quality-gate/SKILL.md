---
name: java-quality-gate
description: Applies the idpay-transactions Java, test-coverage, JaCoCo, generated-jOOQ, and SonarCloud quality workflow. Use for any non-documentation change to Java, SQL, Maven, or GitHub Actions files, and when investigating test coverage or SonarCloud findings.
---

# Java quality gate

Use this workflow before reporting a Java, SQL, Maven, or workflow change as complete.

1. Read `.github/workflows/code-review.yml` before selecting validation commands. It is the source of truth for the CI build, JaCoCo report, generated-jOOQ verification, SonarCloud scan, and exclusions. Do not invent thresholds or substitute a different static-analysis configuration.
2. Inspect the changed production behavior and its existing tests. Write a short test matrix that covers the changed success path, each changed validation or error path, and relevant state, retry, idempotency, or concurrency behavior.
3. Use the smallest targeted test command first. Add or update tests before running the full suite. Reuse the repository's established JUnit, StepVerifier, Mockito, and Testcontainers patterns rather than introducing a new test framework.
4. Run the full repository test suite. When a schema migration changed, also run the repository build path that regenerates jOOQ and confirm that `src/main/generated/jooq` is current.
5. For coverage evidence, mirror the JaCoCo steps in `.github/workflows/code-review.yml` and inspect the generated XML report when available. Focus on changed, Sonar-included production classes; do not count excluded DTO, model, configuration, or generated code as a coverage gap.
6. Run the SonarCloud scanner only when the required local configuration and token are already available. Never print, copy, or synthesize a token. If the scanner cannot run locally, state that SonarCloud remains a required CI check rather than claiming it passed.
7. Run the `sonar-quality-reviewer` agent or `/review` on the final diff. Resolve high-confidence findings, or identify a concrete external blocker.

Report the changed behavior, the test matrix, commands run and their outcomes, generated-source status, and whether SonarCloud was run locally or remains pending in CI. Do not claim a clean quality gate without evidence from the applicable checks.
