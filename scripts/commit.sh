#!/bin/sh
# bash script to commit in accordance with the branch name
# usage: ./scripts/commit.sh
branch=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
branch="${branch##*feat/}"
branch="${branch//\_/ }"
issue_number="${branch//Issue\-/}"
title=$( cut -d ' ' -f 2- <<< "$issue_number" )
issue_number=$( cut -d ' ' -f 1 <<< "$issue_number" )
commit_message="Closes #$issue_number $title"
git commit -m "$commit_message"