# Architectural Overview

## C4 System Context Diagram for Global Nature Watch (GNW) Analytics Pipeline

### Intent

A System Context diagram helps you to answer the following questions.
1. What is the software system that we are building (or have built)?
1. Who is using it?
1. How does it fit in with the existing environment?

### Motivation

- It makes the context and scope of the software system explicit so that there are no assumptions.
- It shows what is being added (from a high-level) to an existing environment.
- It’s a high-level diagram that technical and non-technical people can use as a starting
  point for discussions.
- It provides a starting point for identifying who you potentially need to go and talk to
  as far as understanding inter-system interfaces is concerned.

### Audience

Technical and non-technical people, inside and outside of the immediate software development team.


```mermaid
---
title: "Analytics Pipeline C4 Model: System Context"
---
flowchart TD
  subgraph BoundaryWRI["World Resources Institute (WRI) [Organization]"]
    DataApiEngineer["Data API Engineer
      [User]
      <br/>
      Updates precalc datasets based on new data releases"]
    subgraph BoundaryLCLProgram["Land and Carbon Lab (LCL) [Program]"]
      
      SystemAnalyticsPipeline["Analytics Precalc Pipeline
        [Software System]
        <br>
        Generates pre-calculated (admin) analytics for supported datasets"]
        ExtSystemLclDataRepository["LCL Data Repository
        [Data Repository]
        <br>
        Provides access to pre-processed 
        datasets for administrative AOIs"]
    end
    subgraph BoundaryDataLab["Data Lab [Data Innovation and Product Delivery]"]
      ExtSystemGfwDataLake["Global Forest Watch (GFW) Data Lake
      [Data Lake]
      <br>
      Provides access to raw datasets for on-the-fly (OTF) 
      analysis of non-admin AOIs"]
    end
  end

  DataApiEngineer -- "Executes a dataset flow for" --> SystemAnalyticsPipeline
  SystemAnalyticsPipeline -- "Generates precalc datasets<br>from raw datasets residing in" --> ExtSystemGfwDataLake
  SystemAnalyticsPipeline -- "Copies pre-processed datasets to<br>satisfy admin (precalc) requests to" --> ExtSystemLclDataRepository
  
  classDef focusSystem fill:#1168bd,stroke:#0b4884,color:#ffffff
  classDef supportingSystem fill:#666,stroke:#0b4884,color:#ffffff
  classDef clientSystem fill:#08427b,stroke:#052e56,color:#ffffff
  
  class SystemAnalyticsPipeline focusSystem
  class ExtSystemGfwDataLake,ExtSystemLclDataRepository supportingSystem
  class DataApiEngineer clientSystem
  
  style BoundaryWRI fill:none,stroke:#CCC,stroke-width:2px,stroke-dasharray: 5 5
  style BoundaryDataLab fill:none,stroke:#CCC,stroke-width:2px,stroke-dasharray: 5 5
  style BoundaryLCLProgram fill:none,stroke:#CCC,stroke-width:2px,stroke-dasharray: 5 5
```

## Notes
Right-click linked nodes in the diagram when viewing in Github due to security issues.