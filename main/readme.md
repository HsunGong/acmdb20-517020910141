Some Hint to merge a submodule into origin-main module?

REF: https://stackoverflow.com/questions/23327701/git-merge-submodule-into-parent-tree-cleanly-and-preserving-commit-history
 - need to record all history, and use this submodule as part of the main
```
git subtree add --prefix=$SUBMODULE_DIR git@$SUBMODULE_REMOTE_REPO_URL $SUBMODULE_WANTED_BRANCH
git add .
git commit -m ""
```
 - just do not want another remote repo
```
git remote add $SUB_REMOTE_NAME git@$SUBMODULE_REMOTE_REPO_URL # sub remote name != origin !!!!!
git fetch $SUB_REMOTE_NAME
git checkout -b $SUB_LOCAL_NAME ${SUB_REMOTE_NAME}/$SUBMODULE_WANTED_BRANCH
git push -u origin $SUB_LOCAL_NAME
git remote rm $SUB_REMOTE_NAME
```

