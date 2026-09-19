# Releasing `loac`

This repository publishes two crates:

- `loac-macros`, the procedural macros;
- `loac`, the actor runtime.

They use one version. Publish the macro crate first.

## Prepare a release

Work on a release commit that will land on `main`.

1. Update `version` in both package manifests:
   `crates/loac-macros/Cargo.toml` and `crates/loac/Cargo.toml`.
2. Update the runtime's `loac-macros` requirement to that version.
   Keep its `path` entry for workspace development.
3. Update the examples link in `crates/loac/src/lib.rs`.
   It must use `loac-v<VERSION>`.
4. Run `cargo check --workspace` to refresh `Cargo.lock`.
5. Review the generated package metadata and commit the release.

Macro UI tests live in the private `loac-macro-tests` package.
The macro package therefore has no circular runtime dev-dependency.
This keeps its archive independently testable before publishing the runtime.

Before pushing the release commit, run the local source check:

```bash
bash scripts/release-loac.sh check
bash scripts/release-loac.sh verify-package loac-macros
```

The second crate needs the macro version on crates.io first.

## Configure GitHub

The workflow is `.github/workflows/release-loac.yml`.

Create a GitHub environment named `release`. Add this environment secret:

```text
CARGO_REGISTRY_TOKEN=<crates.io API token>
```

The token needs permission to publish both crates. Keep it in the environment.
Do not put it in the repository or workflow file.

GitHub exposes manual dispatch only from the default branch.
Keep the workflow and release commit on `main`.
That checkout must be clean.
The workflow also needs permission to push tags.

## Run the release

Open **Actions**, select **Release loac**, and choose **Run workflow**.
The job checks out `main` and performs these steps:

1. Run formatting, tests, and documentation checks.
2. Package and test `loac-macros` from its archive.
3. Publish `loac-macros` when that exact archive is absent.
4. Wait until the macro archive is visible on crates.io.
5. Package and test `loac` from its archive.
6. Create and push the annotated immutable tag `loac-v<VERSION>`.
7. Publish `loac` when that exact archive is absent.

The script compares archive checksums with crates.io. An existing version with
different contents stops the workflow. Published versions are never replaced.

## Retry a failed run

Retry the same workflow after fixing a transient failure.

Already completed steps are idempotent:

- a matching published archive is skipped;
- a matching local or remote tag is reused;
- a different archive or tag target fails closed.

Do not change source files after the first crate is published. A new release
needs a new version and a new release commit.

## Verify the release

After the workflow succeeds, verify the tag and both registry entries:

```bash
VERSION=0.3.0
git fetch origin --tags
git show --stat "loac-v$VERSION"
cargo info "loac@$VERSION"
cargo info "loac-macros@$VERSION"
```

Then check the published docs and run a small consumer build. The examples
index in those docs must resolve through the immutable release tag.
