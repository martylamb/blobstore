# Project Admonitions and Notices

## Repository History Change - 2023-11-12

On 2023-11-12, I cleaned up some incorrect contact information (user.name and user.email) in the commit history via [git-filter-repo](https://github.com/newren/git-filter-repo).

### For Clones Made Before 2023-11-12
1. Fetch the updated history and tags with `git fetch --force --all --tags`
2. For each local branch, rebase onto the updated remote branch with `git checkout <branch-name>; git rebase origin/<branch-name>` (Replace `branch-name` with the name of each branch you have checked out.)

### For New Clones (After 2023-11-12):
- No action is needed.  You can clone the repository as usual.

**Note:** These steps are necessary to align your local repository with the rewritten history. Failing to do so might result in conflicts and inconsistencies. If you encounter any issues, please feel free to raise an issue.
