# Release Instructions

* Create your personal access token for the GitHub API and make sure to select at least repo scope: [https://github.com/settings/tokens](https://github.com/settings/tokens)
* Set `GITHUB_USERNAME` and `GITHUB_TOKEN` in `release.sh`
* Release from your current branch with `./release.sh` (it might take a while to upload the release assets)