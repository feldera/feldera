# Skills

## `ralph-feldera-fabric`

Run pyfeldera dbt regression tests against Feldera on Microsoft Fabric. Iteratively fixes failures, rebuilds the wheel, uploads, restarts the notebook, and re-tests until green.

```bash
cd /workspaces/feldera
scripts/ralph.sh .github/skills/ralph-feldera-fabric/skill.md -n 10
```
