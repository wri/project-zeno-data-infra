# Frequently Used Act Commands

https://nektosact.com/usage

## Performance Testing Workflow
```shell
act workflow_dispatch \
-e act_helpers/event.json \
-j performance-test \
--env ACT=true \
--container-architecture linux/amd64 \
-P ubuntu-latest=catthehacker/ubuntu:act-latest
```