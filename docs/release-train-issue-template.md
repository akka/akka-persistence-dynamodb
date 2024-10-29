Release Akka Persistence DynamoDB $VERSION$

<!--
# Release Train Issue Template for Akka Persistence DynamoDB

For every release, use the `scripts/create-release-issue.sh` to make a copy of this file named after the release, and expand the variables.

Variables to be expanded in this template:
- $VERSION$=???

Key links:
  - akka/akka-persistence-dynamodb milestone: https://github.com/akka/akka-persistence-dynamodb/milestone/?
-->

### Cutting the release

- [ ] Check that open PRs and issues assigned to the milestone are reasonable
<!-- - [ ] If PRs were merged after EU midnight, trigger the [native-image tests](https://github.com/akka/akka-persistence-dynamodb/actions/workflows/native-image-tests.yml) and see that they are green. -->
- [ ] Update the Change date and version in the LICENSE file.
- [ ] Create a new milestone for the [next version](https://github.com/akka/akka-persistence-dynamodb/milestones)
- [ ] Close the [$VERSION$ milestone](https://github.com/akka/akka-persistence-dynamodb/milestones?direction=asc&sort=due_date)
- [ ] Make sure all important PRs have been merged
- [ ] Update the revision in Fossa in the Akka Group for the Akka umbrella version, e.g. `22.10`. Note that the revisions for the release is udpated by Akka Group > Projects > Edit. For recent dependency updates the Fossa validation can be triggered from the GitHub actions "Dependency License Scanning".
- [ ] Wait until [main build finished](https://github.com/akka/akka-persistence-dynamodb/actions) after merging the latest PR
- [ ] Update the [draft release](https://github.com/akka/akka-persistence-dynamodb/releases) with the next tag version `v$VERSION$`, title and release description. Use the `Publish release` button, which will create the tag.
- [ ] Check that GitHub Actions release build has executed successfully (GitHub Actions will start a [CI build](https://github.com/akka/akka-persistence-dynamodb/actions) for the new tag and publish artifacts to https://repo.akka.io/maven)

### Check availability

- [ ] Check [API](https://doc.akka.io/api/akka-persistence-dynamodb/$VERSION$/) documentation
- [ ] Check [reference](https://doc.akka.io/libraries/akka-persistence-dynamodb/$VERSION$/) documentation. Check that the reference docs were deployed and show a version warning (see section below on how to fix the version warning).
- [ ] Check the release on https://repo.akka.io/maven/com/lightbend/akka/akka-persistence-dynamodb_2.13/$VERSION$/akka-persistence-dynamodb_2.13-$VERSION$.pom

### When everything is on https://repo.akka.io/maven
  - [ ] Log into `gustav.akka.io` as `akkarepo` 
    - [ ] If this updates the `current` version, run `./update-akka-persistence-dynamodb-current-version.sh $VERSION$`
    - [ ] otherwise check changes and commit the new version to the local git repository
         ```
         cd ~/www
         git status
         git add docs/akka-persistence-dynamodb/current docs/akka-persistence-dynamodb/$VERSION$
         git add api/akka-persistence-dynamodb/current api/akka-persistence-dynamodb/$VERSION$
         git commit -m "Akka Persistence DynamoDB $VERSION$"
         ```

### Announcements

For important patch releases, and only if critical issues have been fixed:

- [ ] Send a release notification to [Lightbend discuss](https://discuss.akka.io)
- [ ] Tweet using the [@akkateam](https://twitter.com/akkateam/) account (or ask someone to) about the new release
- [ ] Announce internally (with links to Tweet, discuss)

For minor or major releases:

- [ ] Include noteworthy features and improvements in Akka umbrella release announcement at akka.io. Coordinate with PM and marketing.

### Afterwards

- [ ] Update [akka-dependencies bom](https://github.com/lightbend/akka-dependencies) and version for [Akka module versions](https://doc.akka.io/docs/akka-dependencies/current/) in [akka-dependencies repo](https://github.com/akka/akka-dependencies)
- Close this issue
