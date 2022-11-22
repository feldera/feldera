# Contributing to database-stream-processor

The database-stream-processor project team welcomes contributions from the community. Before you start working with database-stream-processor, please
read our [Developer Certificate of Origin](https://cla.vmware.com/dco). All contributions to this repository must be
signed as described on that page. Your signature certifies that you wrote the patch or have the right to pass it on
as an open-source patch.

## Contribution Flow

This is a rough outline of what a contributor's workflow looks like:

- Create a topic branch from where you want to base your work
- Make commits of logical units
- Make sure your commit messages are in the proper format (see below)
- Push your changes to a topic branch in the vmware repository or a fork if you don't have commit access to vmware (the reason that pushing directly to the vmware repo is preferred is because then CI will be able to add benchmark results to the PR in the comments).
- Submit a pull request

Example:

``` shell
git remote add upstream https://github.com/vmware/database-stream-processor.git
git checkout -b my-new-feature main
git commit -a
git push origin my-new-feature
```

### Staying In Sync With Upstream

When your branch gets out of sync with the vmware/main branch, use the following to update:

``` shell
git checkout my-new-feature
git fetch -a
git pull --rebase upstream main
git push --force-with-lease origin my-new-feature
```

### Updating pull requests

If your PR fails to pass CI or needs changes based on code review, you'll most likely want to squash these changes into
existing commits.

If your pull request contains a single commit or your changes are related to the most recent commit, you can simply
amend the commit.

``` shell
git add .
git commit --amend
git push --force-with-lease origin my-new-feature
```

If you need to squash changes into an earlier commit, you can use:

``` shell
git add .
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
git push vmware main
```

### Code Style

### Formatting Commit Messages

We follow the conventions on [How to Write a Git Commit Message](http://chris.beams.io/posts/git-commit/).

Be sure to include any related GitHub issue references in the commit message.  See
[GFM syntax](https://guides.github.com/features/mastering-markdown/#GitHub-flavored-markdown) for referencing issues
and commits.

## Reporting Bugs and Creating Issues

When opening a new issue, try to roughly follow the commit message format conventions above.
