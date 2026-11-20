# Architectural Overview

## C4 Container Diagram for Global Nature Watch (GNW) Analytics Pipeline

### Intent

The Container diagram shows the high-level shape of the software architecture and how
responsibilities are distributed across it. It also shows the major technology choices,
how they are used, and how containers communicate with each other. It’s a high-level
**technology focussed** diagram that is useful for software developers and
support/operations staff alike.

A container diagram helps you answer the following questions:
1. What is the overall shape of the software system?
1. What are the high-level technology decisions?
1. How are responsibilities distributed across the system?
1. How do containers communicate with one another?
1. As a developer, where do I need to write code in order to implement features?

### Motivation

Where a System Context diagram shows your software system as a single box,
a Container diagram opens this box up to show what’s inside it.

This is useful because:
- It makes the high-level technology choices explicit.
- It shows the relationships between containers, and how those containers communicate.

### Audience

Technical people inside and outside of the immediate software development
team; including everybody from software developers through to operational
and support staff.

```mermaid
---
title: "GNW Analytics Pipeline C4 Model: Container Diagram"
---
flowchart LR
subgraph DataApiTeam["Data API Team"]
  PrefectCron["Prefect Cron Job
    [Software System]
    <br/>
    Nightly Cron job that submits
    jobs to its internal 'ECS work pool'"]

  ExtSystemPrefectUI["Prefect UI
    [Software System]
    <br>
    Web interface to monitor and manage Prefect flows"]
end

subgraph SystemAnalytics["GNW Analytics System<br>[Software System]"]
      ContainerWorker["Worker
      [Container: Python]
      <br>
      Continuously polls for jobs that were
      added to the Prefect work pool"]
      
      ContainerFlowRunner["Prefect Flow Runner<br>[Container: Python]
      [<b>Ephemeral</b>]
      <br>
      One or more ECS tasks that are spawned
      by the Worker to execute the workload
      and report status/logs/metrics"]

      ExtSystemAmazonS3AnalyticsZonalStats["AWS S3 Bucket<br>lcl-analytics
        [Data Storage]
        <br>
        Stores the precalc Parquet files for admin AOIs"]
    end

    ExtSystemAmazonS3RasterData["AWS S3 Bucket<br>gfw-data-lake
      [GFW Data Lake]
      <br>
      Stores the raw raster datasets for non-admin AOI analytics requests"]

    ExtSystemPrefectServer["Prefect Server
      [Software System]
      <br>
      Orchestrates the Prefect flow execution"]
      

  PrefectCron      -- "<b>(1)</b> Submits jobs" --> PrefectCron
  PrefectCron <-- "<b>(2)</b> Polls for work" --> ContainerWorker
  ContainerWorker -- "<b>(3)</b> Spawns ephemeral ECS tasks to run Prefect flows" --> ContainerFlowRunner
  ContainerFlowRunner -- "<b>(4)</b> Reports status" --> ExtSystemPrefectServer
  ContainerFlowRunner -- "Fetches raw raster data (zarr)" --> ExtSystemAmazonS3RasterData
  ContainerFlowRunner -- "Writes precalc Parquet files for admin AOIs" --> ExtSystemAmazonS3AnalyticsZonalStats
  ExtSystemPrefectUI -- "fetches metrics, logs, and run histories" --> ExtSystemPrefectServer
    
  click ExtSystemPrefectServer "https://data-api.globalforestwatch.org/" _blank
  click PrefectCron "https://docs.prefect.io/v3/concepts/schedules#cron" _blank
  click PrefectUI "https://app.prefect.cloud/auth/sign-in" _blank
  
  classDef focusSystem fill:#1168bd,stroke:#0b4884,color:#ffffff
  classDef supportingSystem fill:#666,stroke:#0b4884,color:#ffffff
  classDef person fill:#08427b,stroke:#052e56,color:#ffffff
  
  class ContainerWorker,ContainerFlowRunner focusSystem
  class ExtSystemAmazonS3AnalyticsZonalStats focusSystem
  class ExtSystemPrefectServer,ExtSystemAmazonS3RasterData supportingSystem
  class PrefectCron,ExtSystemPrefectUI person
  
  style SystemAnalytics fill:none,stroke:#CCC,stroke-width:2px,stroke-dasharray: 5 5
  style DataApiTeam fill:none,stroke:#CCC,stroke-width:2px,stroke-dasharray: 5 5
```

## Notes
Right-click linked nodes in the diagram when viewing in Github due to security issues.