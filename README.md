# CRAN Poland Mirror Analytics

This project proposes an analytics system for Poland's CRAN mirror (`cran.m2.ai`). It correlates package downloads with GitHub releases to track R package demand and adoption across the Polish data science ecosystem.

## Planned capabilities

- Monitor popular packages, download surges, and mirror health, including HTTP errors and bot/CI traffic.
- Measure the time between GitHub releases and download peaks, and track version migration.
- Match CRAN packages to their upstream repositories using the `r-universe` `crantogit` mapping.

See [Milestone 1](deliverables/Milestone_1.pdf) for the project proposal and architecture.
