---
applyTo: "src/main/**/*.java,src/test/**/*.java,src/main/resources/**/*.sql,pom.xml"
---

# Java and SQL quality workflow

For a production behavior change, identify the affected branches, error paths, and state invariants before editing. Add focused tests for every changed branch and externally observable error path; do not inflate coverage with tests of trivial accessors or implementation details.

Use `StepVerifier` for reactive unit tests. For R2DBC, jOOQ, schema, or transaction behavior, add focused PostgreSQL Testcontainers integration coverage that verifies persisted rows and derived projections. Cover retry, idempotency, locking, or concurrency when the behavior can be invoked more than once or by concurrent callers.

Treat `.github/workflows/code-review.yml` as the canonical source for the Maven, JaCoCo, generated-jOOQ, and SonarCloud checks. Run the `/java-quality-gate` skill before completing work. The skill automatically invokes `java-test-specialist` and `sonar-quality-reviewer`; do not ask the user to run those agents separately.

Never edit `src/main/generated/jooq` by hand. When a migration changes the schema, regenerate the sources using the repository build and verify that the generated source diff is intentional.
