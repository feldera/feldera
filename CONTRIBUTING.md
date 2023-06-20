# Contributing to DBSP

The DBSP project team welcomes contributions from the community. Before you start working with DBSP, please
read our [Developer Certificate of Origin (DCO)](https://developercertificate.org/).
To acknowledge the DCO, sign your commits by adding `Signed-off-by: Your Name <your@email.com>` to the last
line of each Git commit message.  Your signature certifies that you wrote the patch or have the right to pass
it on as an open-source patch.  The e-mail address used to sign must match the e-mail address of the Git
author. If you set your `user.name` and `user.email` git config values, you can sign your commit automatically
with `git commit -s`.

## Contribution Flow

### Forking

We recommend forking the dbsp repository and contributing from a fork.
This [page](https://docs.github.com/en/get-started/quickstart/fork-a-repo)
has instructions on how to fork a repository.  After forking do not
forget to add dbsp as a remote repository:

```shell
git remote add upstream https://github.com/feldera/dbsp.git
```

### Workflow

This is a rough outline of what a contributor's workflow looks like:

- Create a topic branch from where you want to base your work
- Make commits of logical units
- Make sure your commit messages are in the proper format (see below)
- Push your changes to a topic branch in the repository (push to your fork if
  you don't have commit access to the dbsp repository --- pushing directly
  to the repo is preferred because then CI will be able to add benchmark
  results to the PR in the comments).
- Submit a pull request

Example:

``` shell
git checkout -b my-new-feature main
git commit -a
git push origin my-new-feature
```

### Staying In Sync With Upstream

When your branch gets out of sync with the feldera/main branch, use the following to update:

``` shell
git checkout my-new-feature
git fetch -a
git pull --rebase upstream main
git push --force-with-lease upstream my-new-feature
```

If you don't have permissions replace the last command with

```
git push --force-with-lease origin my-new-feature
```

### Updating pull requests

If your PR fails to pass CI or needs changes based on code review, you'll most likely want to squash these changes into
existing commits.

If your pull request contains a single commit or your changes are related to the most recent commit, you can simply
amend the commit.

``` shell
git add <files to add>
git commit --amend
git push --force-with-lease origin my-new-feature
```

If you need to squash changes into an earlier commit, you can use:

``` shell
git add <files to add>
git commit --fixup <commit>
git rebase -i --autosquash main
git push --force-with-lease origin my-new-feature
```

Be sure to add a comment to the PR indicating your new changes are ready to review, as GitHub does not generate a
notification when you git push.

### Merging a pull request

Since we run benchmarks as part of CI it's good practice to preserve the commit IDs of the feature branch
we've worked on (and benchmarked). Unfortunately, [the github UI does not have support for this](https://github.com/community/community/discussions/4618)
(it only allows rebase, squash and merge commits to close PRs).
Therefore, it's recommended to merge PRs using the following git CLI invocation:

```shell
git checkout main
git merge --ff-only feature-branch-name
git push upstream main
```

### Code Style

### Formatting Commit Messages

We follow the conventions on [How to Write a Git Commit Message](http://chris.beams.io/posts/git-commit/).

Be sure to include any related GitHub issue references in the commit message.  See
[GFM syntax](https://guides.github.com/features/mastering-markdown/#GitHub-flavored-markdown) for referencing issues
and commits.

## Reporting Bugs and Creating Issues

When opening a new issue, try to roughly follow the commit message format conventions above.
