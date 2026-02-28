# Keboola Cross-Project Data Lineage

> Generated from `kbagent --json lineage` on 2026-02-26.
> 34 projects, 107 unique data links, 332 shared buckets.

## Architecture Overview

The projects follow a 3-layer architecture:

- **L0 (Data Sources)** -- Raw data extraction from external systems (Salesforce, Jira, Zendesk, BambooHR, etc.)
- **L1 (Data Processes)** -- Business logic transformations, enrichment, ML automations
- **L2 (Output / Consumption)** -- BI dashboards, data shares, marketing outputs
- **Standalone** -- Cross-cutting projects (AI, KIDS App Factory, Cloud Costs, PS Discovery)

## Mermaid Diagram

```mermaid
graph LR

    subgraph L0 - Data Sources
        IR_L0___Finance["Finance"]
        IR_L0___KBC_Telemetry_to_Catalog["KBC Telemetry to Catalog"]
        IR_L0___Marketing["Marketing"]
        IR_L0___People["People"]
        IR_L0___Product["Product"]
        IR_L0___Professional_Services["Professional Services"]
        IR_L0___Sales_and_Marketing["Sales & Marketing"]
        IR_L0___Snowflake_Usage_Data["Snowflake Usage Data"]
        IR_L0___Support["Support"]
    end

    subgraph L1 - Data Processes
        IR_L1_Data_Processes_Customer_Success["Customer Success"]
        IR_L1_Data_Processes_Digital_Sales_Machine["Digital Sales Machine"]
        IR_L1_Data_Processes_Engg["Engg"]
        IR_L1_Data_Processes_KIDS["KIDS"]
        IR_L1_Data_Processes_ML_Automations["ML Automations"]
        IR_L1_Data_Processes_Marketing["Marketing"]
        IR_L1_Data_Processes_Marketing_CDP["Marketing CDP"]
        IR_L1_Data_Processes_Product["Product"]
        IR_L1_Data_Processes_Sales["Sales"]
        IR_L1_Data_Processes_Support["Support"]
    end

    subgraph L2 - Output / Consumption
        IR_L2___3rd_Party_Data_Share["3rd Party Data Share"]
        IR_L2___AI_Exploration["AI Exploration"]
        IR_L2___Data_Processes___UX_Design["UX/Design"]
        IR_L2___GD_Telemetry_Output["GD Telemetry Output"]
        IR_L2___Internal_BI_Output["Internal BI Output"]
        IR_L2___Sales_and_Marketing_Output["Sales & Marketing Output"]
        IR_L2___Telemety_Extractor["Telemetry Extractor"]
        IR_L2___VC_Output["VC Output"]
    end

    subgraph Standalone Projects
        ENGG___Cloud_Costs["ENGG - Cloud Costs"]
        Internal_AI_Data_Analyst["Internal AI Data Analyst"]
        KIDS_App_Factory["KIDS App Factory"]
        Keboola_AI["Keboola AI"]
        PS_Team_Data_Discovery["PS Team Data Discovery"]
    end

    IR_L0___Finance --> ENGG___Cloud_Costs
    IR_L0___Finance --> IR_L0___Professional_Services
    IR_L0___Finance --> IR_L0___Sales_and_Marketing
    IR_L0___Finance --> IR_L0___Snowflake_Usage_Data
    IR_L0___Finance --> IR_L2___Internal_BI_Output
    IR_L0___Finance --> IR_L2___Sales_and_Marketing_Output
    IR_L0___Finance --> IR_L2___VC_Output
    IR_L0___Finance -->|2 buckets| Internal_AI_Data_Analyst

    IR_L0___KBC_Telemetry_to_Catalog --> IR_L0___Finance
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L0___Product
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L0___Sales_and_Marketing
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L0___Support
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L1_Data_Processes_Customer_Success
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L1_Data_Processes_Digital_Sales_Machine
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L1_Data_Processes_Engg
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L1_Data_Processes_Marketing
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L1_Data_Processes_Product
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L1_Data_Processes_Sales
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L1_Data_Processes_Support
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L2___3rd_Party_Data_Share
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L2___AI_Exploration
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L2___Data_Processes___UX_Design
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L2___GD_Telemetry_Output
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L2___Internal_BI_Output
    IR_L0___KBC_Telemetry_to_Catalog --> IR_L2___Sales_and_Marketing_Output
    IR_L0___KBC_Telemetry_to_Catalog --> Internal_AI_Data_Analyst
    IR_L0___KBC_Telemetry_to_Catalog --> KIDS_App_Factory
    IR_L0___KBC_Telemetry_to_Catalog --> Keboola_AI
    IR_L0___KBC_Telemetry_to_Catalog --> PS_Team_Data_Discovery

    IR_L0___Marketing --> IR_L1_Data_Processes_Marketing
    IR_L0___Marketing -->|2 buckets| IR_L1_Data_Processes_Marketing_CDP
    IR_L0___Marketing --> IR_L2___Internal_BI_Output

    IR_L0___People --> IR_L0___Finance
    IR_L0___People --> IR_L0___Professional_Services
    IR_L0___People --> IR_L0___Sales_and_Marketing
    IR_L0___People --> IR_L0___Support
    IR_L0___People --> IR_L1_Data_Processes_ML_Automations
    IR_L0___People --> IR_L2___Internal_BI_Output
    IR_L0___People --> IR_L2___Sales_and_Marketing_Output
    IR_L0___People --> Internal_AI_Data_Analyst
    IR_L0___People --> KIDS_App_Factory
    IR_L0___People --> Keboola_AI

    IR_L0___Product --> IR_L0___Support
    IR_L0___Product --> IR_L1_Data_Processes_Product
    IR_L0___Product -->|2 buckets| IR_L2___Internal_BI_Output
    IR_L0___Product --> Keboola_AI

    IR_L0___Professional_Services --> IR_L0___Marketing
    IR_L0___Professional_Services -->|2 buckets| IR_L0___Product
    IR_L0___Professional_Services -->|2 buckets| IR_L0___Sales_and_Marketing
    IR_L0___Professional_Services -->|2 buckets| IR_L0___Support
    IR_L0___Professional_Services --> IR_L1_Data_Processes_Product
    IR_L0___Professional_Services --> IR_L1_Data_Processes_Support
    IR_L0___Professional_Services -->|4 buckets| IR_L2___Internal_BI_Output
    IR_L0___Professional_Services --> IR_L2___Sales_and_Marketing_Output
    IR_L0___Professional_Services -->|3 buckets| Keboola_AI
    IR_L0___Professional_Services --> PS_Team_Data_Discovery

    IR_L0___Sales_and_Marketing -->|2 buckets| IR_L0___Finance
    IR_L0___Sales_and_Marketing -->|5 buckets| IR_L0___Marketing
    IR_L0___Sales_and_Marketing --> IR_L0___People
    IR_L0___Sales_and_Marketing --> IR_L0___Product
    IR_L0___Sales_and_Marketing --> IR_L0___Professional_Services
    IR_L0___Sales_and_Marketing --> IR_L0___Snowflake_Usage_Data
    IR_L0___Sales_and_Marketing -->|2 buckets| IR_L0___Support
    IR_L0___Sales_and_Marketing -->|2 buckets| IR_L1_Data_Processes_Customer_Success
    IR_L0___Sales_and_Marketing --> IR_L1_Data_Processes_Digital_Sales_Machine
    IR_L0___Sales_and_Marketing --> IR_L1_Data_Processes_KIDS
    IR_L0___Sales_and_Marketing --> IR_L1_Data_Processes_ML_Automations
    IR_L0___Sales_and_Marketing -->|2 buckets| IR_L1_Data_Processes_Marketing
    IR_L0___Sales_and_Marketing -->|4 buckets| IR_L1_Data_Processes_Marketing_CDP
    IR_L0___Sales_and_Marketing -->|2 buckets| IR_L1_Data_Processes_Product
    IR_L0___Sales_and_Marketing --> IR_L1_Data_Processes_Sales
    IR_L0___Sales_and_Marketing --> IR_L1_Data_Processes_Support
    IR_L0___Sales_and_Marketing -->|2 buckets| IR_L2___3rd_Party_Data_Share
    IR_L0___Sales_and_Marketing --> IR_L2___AI_Exploration
    IR_L0___Sales_and_Marketing --> IR_L2___GD_Telemetry_Output
    IR_L0___Sales_and_Marketing -->|4 buckets| IR_L2___Internal_BI_Output
    IR_L0___Sales_and_Marketing -->|16 buckets| IR_L2___Sales_and_Marketing_Output
    IR_L0___Sales_and_Marketing --> IR_L2___Telemety_Extractor
    IR_L0___Sales_and_Marketing --> IR_L2___VC_Output
    IR_L0___Sales_and_Marketing --> Internal_AI_Data_Analyst
    IR_L0___Sales_and_Marketing -->|3 buckets| KIDS_App_Factory
    IR_L0___Sales_and_Marketing --> Keboola_AI
    IR_L0___Sales_and_Marketing -->|2 buckets| PS_Team_Data_Discovery

    IR_L0___Snowflake_Usage_Data -->|3 buckets| IR_L0___Finance
    IR_L0___Snowflake_Usage_Data --> IR_L2___Internal_BI_Output
    IR_L0___Snowflake_Usage_Data --> IR_L2___Telemety_Extractor
    IR_L0___Snowflake_Usage_Data --> KIDS_App_Factory

    IR_L0___Support --> IR_L0___Professional_Services
    IR_L0___Support --> IR_L0___Sales_and_Marketing
    IR_L0___Support -->|4 buckets| IR_L1_Data_Processes_Support
    IR_L0___Support --> IR_L2___Internal_BI_Output
    IR_L0___Support --> KIDS_App_Factory
    IR_L0___Support -->|2 buckets| Keboola_AI

    IR_L1_Data_Processes_Digital_Sales_Machine --> IR_L2___Internal_BI_Output
    IR_L1_Data_Processes_Engg --> IR_L2___Internal_BI_Output
    IR_L1_Data_Processes_Marketing_CDP --> IR_L2___Sales_and_Marketing_Output
    IR_L1_Data_Processes_Product -->|4 buckets| IR_L2___Internal_BI_Output

    IR_L2___Sales_and_Marketing_Output --> IR_L0___Marketing
    IR_L2___Sales_and_Marketing_Output -->|4 buckets| IR_L0___Sales_and_Marketing
    IR_L2___Sales_and_Marketing_Output -->|2 buckets| IR_L1_Data_Processes_Marketing
    IR_L2___Sales_and_Marketing_Output --> IR_L2___Internal_BI_Output
    IR_L2___Sales_and_Marketing_Output --> KIDS_App_Factory

    Keboola_AI -->|2 buckets| IR_L0___Sales_and_Marketing
    Keboola_AI --> IR_L1_Data_Processes_Product
    Keboola_AI --> IR_L1_Data_Processes_Support
    Keboola_AI -->|4 buckets| IR_L2___AI_Exploration
    Keboola_AI --> IR_L2___Sales_and_Marketing_Output
```
