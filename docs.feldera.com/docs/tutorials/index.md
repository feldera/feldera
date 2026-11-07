import {HubGrid, UseCaseGrid} from '@site/src/components/UseCases';

# Guides

Learn Feldera step by step, or start from what you want to build.

<UseCaseGrid items={[
  {
    kind: 'Guide',
    title: 'Feldera Basics',
    description: 'Write your first SQL pipeline, connect inputs and outputs, and generate test data. Four parts.',
    href: '/tutorials/basics/',
  },
]} />

## Use cases

Guides and runnable demos, sorted by what you want to build. Every use case has the same shape: see [How to build a use case](/use_cases/how-to-build) for the steps and a starting point for each.

<HubGrid />

## More examples

<UseCaseGrid items={[
  {
    kind: 'Guide',
    title: 'OpenTelemetry analysis',
    description: 'Analyze logs, traces and metrics from the OTel Collector with SQL, and show them in Grafana.',
    href: '/use_cases/otel/intro',
  },
  {
    kind: 'Guide',
    title: 'Live web applications',
    description: 'Build a collaborative spreadsheet that stays incremental for many concurrent users.',
    href: '/use_cases/real_time_apps/part1',
  },
  {
    kind: 'Demo',
    title: 'Recommender system features',
    description: 'Compute rolling per-user and per-video aggregates from Kafka for a TikTok-style recommender.',
    href: 'https://github.com/feldera/feldera-demos/tree/main/tik-tok-recommender-system',
  },
]} />
