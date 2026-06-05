# Basic CI Pipeline

## Rationale

The project needs a GitHub Actions workflow that validates changes before they reach `main`. The workflow should restore packages, build the whole solution, run the full test suite with Docker available for Testcontainers-based integration tests, publish merged Cobertura coverage output, and comment on pull requests with a useful coverage summary.

## Acceptance Criteria

- [x] A GitHub Actions workflow exists under `.github/workflows/` and runs for pushes to `main` and pull requests targeting `main`.
- [x] The workflow grants only the permissions it needs: repository contents read access, plus the narrow write permission required by the chosen PR coverage comment action.
- [x] Superseded runs for the same branch or pull request are cancelled automatically.
- [x] The workflow installs and uses the .NET 10 SDK requested by `global.json`.
- [x] NuGet packages are cached between workflow runs without depending on missing lock files.
- [x] The workflow restores all NuGet packages for `USF.slnx`.
- [x] The workflow builds the entire solution in Release configuration without restoring again.
- [x] The workflow runs every test project and collects code coverage in Cobertura format.
- [x] The workflow runs with Docker available so Testcontainers integration tests can start their required containers.
- [x] The workflow merges all test coverage outputs and converts the result to Cobertura format.
- [x] Pull request runs create or update a single PR comment summarizing the coverage results for same-repository PRs.
- [x] Forked pull requests are handled safely: either they skip the write-back comment and still upload coverage artifacts, or a separate trusted comment workflow reads the completed run's artifacts and comments without executing untrusted PR code.
- [x] Coverage artifacts are uploaded for later inspection.

## Technical Details

Add a workflow such as `.github/workflows/ci.yml` with `push` limited to `main` and `pull_request` limited to `main`. Use the `pull_request` event, not `pull_request_target`, for restore/build/test because the workflow executes untrusted PR code. Add top-level `permissions` with `contents: read`, keep the build/test job read-only, and grant only the write permissions required by the PR coverage comment job (`issues: write` plus `pull-requests: write` for the sticky PR comment action). Add `concurrency` using the workflow name plus `github.ref` or PR number so pushes to the same branch cancel stale runs.

Use `actions/checkout` and `actions/setup-dotnet` with `global-json-file: global.json`. Cache NuGet packages with either checked-in `packages.lock.json` files and `setup-dotnet` caching, or with `actions/cache` over `~/.nuget/packages` keyed from `global.json`, `Directory.Packages.props`, `Directory.Build.props`, and `**/*.csproj`. Since this repo currently has no `packages.lock.json` files, prefer the explicit `actions/cache` approach unless lock files are added as part of the implementation. Then run `dotnet restore USF.slnx /p:ContinuousIntegrationBuild=true` and `dotnet build USF.slnx --configuration Release --no-restore /p:ContinuousIntegrationBuild=true`.

GitHub-hosted Ubuntu runners already include Docker, but the workflow should verify Docker is usable before test execution, for example with `docker version`. Keep the tests on Ubuntu unless the project later needs a matrix; the Testcontainers RabbitMQ tests only require a working Docker daemon.

Because the test suite uses the Microsoft Testing Platform runner, use the Microsoft Testing Platform code coverage extension instead of the VSTest `coverlet.collector` package. Test projects reference `Microsoft.Testing.Extensions.CodeCoverage`, which lets the workflow run the whole solution with `dotnet test` and MTP-native coverage:

```bash
dotnet test USF.slnx --configuration Release --no-build --no-restore --results-directory artifacts/test-results --coverage --coverage-output-format cobertura /p:ContinuousIntegrationBuild=true
```

Use a pinned `dotnet-reportgenerator-globaltool` to merge every generated `*.cobertura.xml` into one stable output such as `artifacts/coverage/Cobertura.xml`, with `-reporttypes:Cobertura;MarkdownSummaryGithub` so the same step also produces a Markdown summary. The important CI behavior is to merge the per-project files into one stable Cobertura report and normalize the output path. Upload both the raw test result directories and merged coverage directory with `actions/upload-artifact`, using `if: always()` so failures still leave diagnostics behind.

For PR comments, prefer `irongut/CodeCoverageSummary` if it can consume the merged Cobertura file and produce the desired summary/comment behavior without duplicating coverage calculations. Otherwise, use the ReportGenerator Markdown summary plus an action such as `marocchino/sticky-pull-request-comment` to create or update a single sticky coverage comment on `pull_request` events only. The comment step should be skipped for `push` events. For same-repository PRs, the comment can run in a separate job with write permissions after tests complete. If forked PR comments are required, split commenting into a separate `workflow_run`-based workflow that reads artifacts from the completed untrusted run and comments with a write-capable token. If that extra workflow is not worth the complexity, skip the fork comment and rely on uploaded artifacts. Do not switch the build/test workflow to `pull_request_target`.
