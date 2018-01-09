#!/bin/bash

# PERSONALIZE GITHUB_USERNAME AND GITHUB_TOKEN!

GITHUB_USERNAME=""
GITHUB_TOKEN="" # https://github.com/settings/tokens (at least repo scope)

# set prefix, timestamp, and branch

PREFIX="desq"
BRANCH=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
TIMESTAMP=$(date +"%Y%m%d")

# packaging

echo "PACKAGING ..."
echo

# creaate the base jar and the -full jar

mvn clean package

# create the -no-spark jar without cleaning the old jars

mvn package -Pprovided

# creating tag

echo
echo "CREATING TAG ..."
echo

# create tag

git tag "$DESQ-$BRANCH-$TIMESTAMP" "$BRANCH"

# push tag

git push origin "$DESQ-$BRANCH-$TIMESTAMP"

# creating release

echo
echo "CREATING RELEASE ..."
echo

# switch to target folder

cd target

# create release (https://developer.github.com/v3/repos/releases/#list-releases-for-a-repository)

UPLOAD_ID=$(curl --user $GITHUB_USERNAME:$GITHUB_TOKEN -X POST -d '{
  "tag_name": "'$PREFIX'-'$BRANCH'-'$TIMESTAMP'",
  "target_commitish": "'$BRANCH'",
  "name": "'$PREFIX'-'$BRANCH'-'$TIMESTAMP'",
  "body": "'$PREFIX'-'$BRANCH'-'$TIMESTAMP'",
  "draft": false,
  "prerelease": false
}' https://api.github.com/repos/rgemulla/desq/releases | grep upload_url | sed "s/[^0-9]//g")

# upload release assets (https://developer.github.com/v3/repos/releases/#upload-a-release-asset)

curl --user $GITHUB_USERNAME:$GITHUB_TOKEN -H "Content-Type: application/java-archive" --data-binary @$PREFIX-$BRANCH-$TIMESTAMP.jar -X POST "https://uploads.github.com/repos/rgemulla/desq/releases/$UPLOAD_ID/assets?name=$PREFIX-$BRANCH-$TIMESTAMP.jar"
curl --user $GITHUB_USERNAME:$GITHUB_TOKEN -H "Content-Type: application/java-archive" --data-binary @$PREFIX-$BRANCH-$TIMESTAMP-no-spark.jar -X POST "https://uploads.github.com/repos/rgemulla/desq/releases/$UPLOAD_ID/assets?name=$PREFIX-$BRANCH-$TIMESTAMP-no-spark.jar"
curl --user $GITHUB_USERNAME:$GITHUB_TOKEN -H "Content-Type: application/java-archive" --data-binary @$PREFIX-$BRANCH-$TIMESTAMP-full.jar -X POST "https://uploads.github.com/repos/rgemulla/desq/releases/$UPLOAD_ID/assets?name=$PREFIX-$BRANCH-$TIMESTAMP-full.jar"
