# Releasing

## When to release

Akka Persistence DynamoDB is released when there is a need for it.

If you want to test an improvement that is not yet released, you can use a
snapshot version. We publish snapshot versions for every commit to the `main` branch.
Snapshot builds are available at https://repo.akka.io/snapshots.

## How to release

Create a new issue from the [Release Train Issue Template](docs/release-train-issue-template.md):

```
./scripts/create-release-issue.sh 0.x.y
```

### Releasing only updated docs

It is possible to release a revised documentation to the already existing release.

1. Create a new branch from a release tag. If a revised documentation is for the `v0.1` release, then the name of the new branch should be `docs/v0.1`.
1. Add and commit `version.sbt` file that pins the version to the one that is being revised. Also set `isSnapshot` to `false` for the stable documentation links. For example:
    ```scala
    ThisBuild / version := "0.1.2"
    ThisBuild / isSnapshot := false
    ```
1. Make all of the required changes to the documentation.
1. Build documentation locally with:
    ```sh
    sbt docs/paradoxBrowse
    ```
1. If the generated documentation looks good, send it to Gustav:
    ```sh
    sbt docs/publishRsync
    ```
1. Do not forget to push the new branch back to GitHub.
