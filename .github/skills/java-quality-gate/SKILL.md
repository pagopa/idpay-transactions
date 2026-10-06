---
name: java-quality-gate
description: Autonomously orchestrates the idpay-transactions Java, test-coverage, JaCoCo, generated-jOOQ, and SonarCloud quality workflow. Use automatically for any non-documentation implementation change to Java, SQL, Maven, or GitHub Actions files, and when investigating test coverage or SonarCloud findings.
---

# Java quality gate

Use this workflow automatically for a Java, SQL, Maven, or workflow implementation request. It owns the quality flow: do not ask the user to invoke supporting agents or to request routine validation.

1. Read `.github/workflows/code-review.yml` before selecting validation commands. It is the source of truth for the CI build, JaCoCo report, generated-jOOQ verification, SonarCloud scan, and exclusions. Do not invent thresholds or substitute a different static-analysis configuration.
2. Inspect the changed production behavior and its existing tests. Create a concise test matrix covering the changed success path, each changed validation or error path, and relevant state, retry, idempotency, or concurrency behavior.
3. Invoke the `java-test-specialist` agent with the implementation scope and test matrix. It owns test-gap analysis and test additions. Integrate its result before final validation; if it reports a blocker, resolve it or surface the concrete blocker.
4. Use the smallest targeted test command first. Add or update tests before running the full suite. Reuse the repository's established JUnit, StepVerifier, Mockito, and Testcontainers patterns rather than introducing a new test framework.
5. Run the full repository test suite. When a schema migration changed, also run the repository build path that regenerates jOOQ and confirm that `src/main/generated/jooq` is current.
6. For coverage evidence, mirror the JaCoCo steps in `.github/workflows/code-review.yml` and inspect the generated XML report when available. Focus on changed, Sonar-included production classes; do not count excluded DTO, model, configuration, or generated code as a coverage gap.
7. Run the SonarCloud scanner only when the required local configuration and token are already available. Never print, copy, or synthesize a token. If the scanner cannot run locally, state that SonarCloud remains a required CI check rather than claiming it passed.
8. Invoke the read-only `sonar-quality-reviewer` agent on the final diff. Resolve its high-confidence findings, rerunning the reviewer after material changes. Use `/review` as an additional independent pass when appropriate.

Report the changed behavior, the test matrix, commands run and their outcomes, generated-source status, and whether SonarCloud was run locally or remains pending in CI. Do not claim a clean quality gate without evidence from the applicable checks.

Proceed without routine follow-up questions. Stop only when a business requirement is genuinely ambiguous, an explicitly destructive action needs approval, or required local infrastructure is unavailable and cannot be resolved safely.
