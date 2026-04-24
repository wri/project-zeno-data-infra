# Architectural Overview

## C4 Container Diagram for Global Nature Watch (GNW) Analytics API

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
title: "GNW Analytics API C4 Model: Container Diagram"
---
flowchart LR
  GnwBackend["Global Nature Watch (GNW) Backend
    [Software System]
    <br/>
    Supports an agentic AI frontend by translating
    user prompts into API calls related to specific datasets
    and focused on specific areas of interest (AOIs)"]
  
    subgraph SystemAnalytics["GNW Analytics System<br>[Software System]"]
      ContainerRestApi["Rest API 
      [Container: Python and FastAPI]
      <br>
      Creates and fetches analytics resources for requested datasets and AOIs"]
      
      ContainerDaskCluster["Dask Cluster<br>[Container: Python and Dask]
      <br>
      Orchestrates distributed computing tasks for OTF analytics processing"]
    
      ExtSystemAmazonS3AnalyticsResults["AWS S3 Bucket<br>gnw-analytics-api-analysis-results
        [Data Storage]
        <br>
        Stores the analytics resource results"]

      ExtSystemAmazonS3AnalyticsZonalStats["AWS S3 Bucket<br>lcl-analytics
        [Data Storage]
        <br>
        Stores the precalc Parquet files for admin AOIs"]

      ExtSystemAwsDynamoDbAnalyticsMetadata["AWS DynamoDB Table<br>Analyses
        [Document Database]
        <br>
        Stores metadata passed from the
        GNW backend during the POST request
        (AOI IDs, filters, dates, etc.)"]
    end

    ExtSystemAmazonS3RasterData["AWS S3 Bucket<br>gfw-data-lake
      [GFW Data Lake]
      <br>
      Stores the raw raster datasets for non-admin AOI analytics requests"]

    ExtSystemGfwDataAPI["GFW Data API
      [Software System]
      <br>
      Provides geojson of non-admin AOIs:
      <code>birdlife_key_biodiversity_areas</code>
      <code>wdpa_protected_areas</code>
      <code>landmark_ip_lc_and_indicative_poly</code>"]


  GnwBackend       -- "POSTs analytics requests to<br><code>analytics.globalnaturewatch.org/v0/land_change/:dataset/analytics</code><br>using<br>[HTTP]" --> ContainerRestApi
  ContainerRestApi -- "Runs zonal statistics for OTF requests" --> ContainerDaskCluster
  ContainerRestApi -- "Queries zonal statistics data for GADM admin AOIs <br>using<br>[DuckDB (SQL)]" --> ExtSystemAmazonS3AnalyticsZonalStats
  ContainerRestApi -- "Fetches non-admin AOI <code>geojson</code> from <br><code>data-api.globalforestwatch.org/</code><br>using<br>[HTTPS]" --> ExtSystemGfwDataAPI
  ContainerRestApi -- "Fetches raw raster data (zarr)" --> ExtSystemAmazonS3RasterData
  ContainerRestApi -- "Stores and retrieves zonal stats results at<br><code>s3://gnw-analytics-api-analysis-results/*</code><br>using<br>[aiboto3]" --> ExtSystemAmazonS3AnalyticsResults
  ContainerRestApi -- "Stores and retrieves analytics metadata POSTed by GNW client requests <br>using<br>[aiboto3]" --> ExtSystemAwsDynamoDbAnalyticsMetadata
    
  click ExtSystemGfwDataAPI "https://data-api.globalforestwatch.org/" _blank
  
  classDef focusSystem fill:#1168bd,stroke:#0b4884,color:#ffffff
  classDef supportingSystem fill:#666,stroke:#0b4884,color:#ffffff
  classDef person fill:#08427b,stroke:#052e56,color:#ffffff
  
  class ContainerRestApi,ContainerRestApi,ContainerDaskCluster focusSystem
  class ExtSystemAmazonS3AnalyticsResults,ExtSystemAmazonS3AnalyticsZonalStats focusSystem
  class ExtSystemAwsDynamoDbAnalyticsMetadata focusSystem
  class ExtSystemGfwDataAPI,ExtSystemAmazonS3RasterData supportingSystem
  class GnwBackend person
  
  style SystemAnalytics fill:none,stroke:#CCC,stroke-width:2px,stroke-dasharray: 5 5
```

## Notes
Right-click linked nodes in the diagram when viewing in Github due to security issues.