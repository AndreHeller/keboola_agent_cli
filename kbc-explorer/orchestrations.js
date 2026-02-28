const ORCHESTRATIONS = {
  "ir-l0-finance|01k4mx8hxg189wym57dthffr31": {
    "project_alias": "ir-l0-finance",
    "config_id": "01k4mx8hxg189wym57dthffr31",
    "name": "Infrastructure Cost Monthly",
    "description": "This flows runs once a month as we only download cost for Azure and GCP/BigQuery once a month running in [this flow](https://connection.us-east4.gcp.keboola.com/admin/projects/337/flows/01k4sr773gnfm5gsz84gmkepmv). AWS is  updated daily in separate flow - [AWS](https://connection.us-east4.gcp.keboola.com/admin/projects/337/flows-v2/01k4q5yst44v9xcxdr2ejkey8e) and Snowflake in separate project - [L0 Keboola Snowflake Usage](https://connection.us-east4.gcp.keboola.com/admin/projects/106/dashboard) and [IR L0 - Reseller Data](https://connection.us-east4.gcp.keboola.com/admin/projects/343/dashboard).",
    "is_disabled": false,
    "version": 11,
    "last_modified": "2026-02-04T13:20:28+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 68783,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Infrastructure Cost",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01k4a9nndp3a65cw0s7qncccet",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 89968,
        "name": "Step 2",
        "depends_on": [
          68783
        ],
        "tasks": [
          {
            "name": "Cloud Cost, Snowflake MT Cost",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k47s99vr3qhp0xcgx2f870dr",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Cloud Cost, Snowflake MT Cost Updated",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kgjv9zb6qq1gm2shw47ax4w9",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 15926,
        "name": "Step 3",
        "depends_on": [
          89968
        ],
        "tasks": [
          {
            "name": "Infrastructure Costs",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1152612505",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P68783[\"Step 1<br/>[no enabled tasks]\"]\n    P89968[\"Step 2<br/>---<br/>TR: Cloud Cost, Snowflake MT Cost<br/>TR: Cloud Cost, Snowflake MT Cost Updated\"]\n    P68783 --> P89968\n    P15926[\"Step 3<br/>---<br/>TR: Infrastructure Costs\"]\n    P89968 --> P15926\n\n    style P68783 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P89968 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P15926 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-finance|01k4sr773gnfm5gsz84gmkepmv": {
    "project_alias": "ir-l0-finance",
    "config_id": "01k4sr773gnfm5gsz84gmkepmv",
    "name": "Keboola Azure & GCP Cost Usage Reports",
    "description": "https://keboola.atlassian.net/wiki/spaces/ENGG/pages/4269113349/Azure+Cost+Exports",
    "is_disabled": false,
    "version": 8,
    "last_modified": "2026-02-04T13:19:58+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 82668,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Azure Cost & Usage Download",
            "component_id": "kds-team.ex-azure-blob-storage-v2",
            "component_short": "ex-azure-blob-storage-v2",
            "config_id": "01k4pv171bv6jxtrr6dgm45g4v",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "GCP Cost Usage report (monthly)",
            "component_id": "keboola.ex-google-bigquery-v2",
            "component_short": "ex-google-bigquery-v2",
            "config_id": "01k4vs3rx4dqy7c7jz7cdyemss",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 834,
        "name": "Step 2",
        "depends_on": [
          82668
        ],
        "tasks": [
          {
            "name": "GCP Cost Monthly - Incremental Merge",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kgm8cs5vrqq2sg9jnatbb065",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 2,
    "mermaid": "graph LR\n    P82668[\"Step 1<br/>---<br/>EX: Azure Cost and Usage Download<br/>EX: GCP Cost Usage report [monthly]\"]\n    P834[\"Step 2<br/>---<br/>TR: GCP Cost Monthly - Incremental Merge\"]\n    P82668 --> P834\n\n    style P82668 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P834 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-finance|01k7sva4d5f9q6bt6j99r5kjcs": {
    "project_alias": "ir-l0-finance",
    "config_id": "01k7sva4d5f9q6bt6j99r5kjcs",
    "name": "Weekly Leadership Update",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2026-01-09T17:54:24+0100",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 84559,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Weekly Leadership",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01k7s3wjcrwqvek8kgvpxyh0ba",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 55917,
        "name": "Step 2",
        "depends_on": [
          84559
        ],
        "tasks": [
          {
            "name": "Leadership Weekly Metrics",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k7pm4yp9pfbtcexkd22mx6xk",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P84559[\"Step 1<br/>---<br/>EX: Weekly Leadership\"]\n    P55917[\"Step 2<br/>---<br/>TR: Leadership Weekly Metrics\"]\n    P84559 --> P55917\n\n    style P84559 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P55917 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-finance|01kg1vahtkc838ejr1ts5gg5kz": {
    "project_alias": "ir-l0-finance",
    "config_id": "01kg1vahtkc838ejr1ts5gg5kz",
    "name": "Keboola AWS Cost Usage Reports",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2026-01-28T09:26:08+0100",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 18095,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keboola AWS cost usage reports",
            "component_id": "keboola.ex-aws-s3",
            "component_short": "ex-aws-s3",
            "config_id": "01k4q3yrdm72ej7jyezj6y61yf",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P18095[\"Step 1<br/>---<br/>EX: Keboola AWS cost usage reports\"]\n\n    style P18095 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb"
  },
  "ir-l0-finance|1081504538": {
    "project_alias": "ir-l0-finance",
    "config_id": "1081504538",
    "name": "Invoicing",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2026-01-06T12:15:31+0100",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 89766,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Sales Data & Entity Bank Accounts",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1081500772",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "CashFlow - Payment Delays",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1081891676",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Keboola",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "1081487990",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Accounting Systems Data Update",
            "component_id": "keboola.orchestrator",
            "component_short": "orchestrator",
            "config_id": "842058213",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "FL"
          }
        ]
      },
      {
        "id": 96549,
        "name": "Transformation 1",
        "depends_on": [
          89766
        ],
        "tasks": [
          {
            "name": "01 Invoices",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1081483776",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 24113,
        "name": "Transformation 02",
        "depends_on": [
          96549
        ],
        "tasks": [
          {
            "name": "02 Invoices Snapshots",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1081960078",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 44014,
        "name": "Transformation 03",
        "depends_on": [
          24113
        ],
        "tasks": [
          {
            "name": " 03 Invoices Changelog",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1081985754",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "03 Invoices Mailgun Triggers",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "1081974348",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 79331,
        "name": "Hide Sensitive Data Outside",
        "depends_on": [
          44014
        ],
        "tasks": [
          {
            "name": "GoodData Data Preparation",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1185545045",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 85062,
        "name": "Update list of Xero invoices",
        "depends_on": [
          79331
        ],
        "tasks": [
          {
            "name": "Accounting Files Xero",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "01ke9frdqkq0enrw52bmmnw1t5",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 26417,
        "name": "Upload the records into Flexibee",
        "depends_on": [
          85062
        ],
        "tasks": [
          {
            "name": "Flexibee Writer",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1095592140",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 88877,
        "name": "Save the written records",
        "depends_on": [
          26417
        ],
        "tasks": [
          {
            "name": "04 Loaded Records",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1101012565",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 12,
    "total_phases": 8,
    "mermaid": "graph LR\n    P89766[\"Extraction<br/>---<br/>EX: Sales Data and Entity Bank Accounts *<br/>EX: CashFlow - Payment Delays *<br/>EX: Keboola<br/>FL: Accounting Systems Data Update *\"]\n    P96549[\"Transformation 1<br/>---<br/>TR: 01 Invoices\"]\n    P89766 --> P96549\n    P24113[\"Transformation 02<br/>---<br/>TR: 02 Invoices Snapshots\"]\n    P96549 --> P24113\n    P44014[\"Transformation 03<br/>---<br/>TR:  03 Invoices Changelog<br/>TR: 03 Invoices Mailgun Triggers\"]\n    P24113 --> P44014\n    P79331[\"Hide Sensitive Data Outside<br/>---<br/>TR: GoodData Data Preparation\"]\n    P44014 --> P79331\n    P85062[\"Update list of Xero invoices<br/>---<br/>WR: Accounting Files Xero\"]\n    P79331 --> P85062\n    P26417[\"Upload the records into Flexibee<br/>---<br/>WR: Flexibee Writer\"]\n    P85062 --> P26417\n    P88877[\"Save the written records<br/>---<br/>TR: 04 Loaded Records\"]\n    P26417 --> P88877\n\n    style P89766 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P96549 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P24113 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P44014 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P79331 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P85062 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P26417 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P88877 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-finance|1082120393": {
    "project_alias": "ir-l0-finance",
    "config_id": "1082120393",
    "name": "Send Xero CA File",
    "description": "This is set up with the flow to run",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:10:00+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 75149,
        "name": "Get Current Recipients",
        "depends_on": [],
        "tasks": [
          {
            "name": "Invoice Mailing Lists",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1081987319",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 74235,
        "name": "Send File with Invoices",
        "depends_on": [
          75149
        ],
        "tasks": [
          {
            "name": "Xero CA Invoice File",
            "component_id": "kds-team.app-mailgun-v2",
            "component_short": "app-mailgun-v2",
            "config_id": "1082124121",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P75149[\"Get Current Recipients<br/>---<br/>EX: Invoice Mailing Lists\"]\n    P74235[\"Send File with Invoices<br/>---<br/>AP: Xero CA Invoice File\"]\n    P75149 --> P74235\n\n    style P75149 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P74235 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l0-finance|1082122935": {
    "project_alias": "ir-l0-finance",
    "config_id": "1082122935",
    "name": "Send Xero US File",
    "description": "Created from Send Xero CA File version #4\n\nThis is set up with the flow to run",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:10:00+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 75149,
        "name": "Get Current Recipients",
        "depends_on": [],
        "tasks": [
          {
            "name": "Invoice Mailing Lists",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1081987319",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 74235,
        "name": "Send File with Invoices",
        "depends_on": [
          75149
        ],
        "tasks": [
          {
            "name": "Xero US Invoice File",
            "component_id": "kds-team.app-mailgun-v2",
            "component_short": "app-mailgun-v2",
            "config_id": "1082116296",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P75149[\"Get Current Recipients<br/>---<br/>EX: Invoice Mailing Lists\"]\n    P74235[\"Send File with Invoices<br/>---<br/>AP: Xero US Invoice File\"]\n    P75149 --> P74235\n\n    style P75149 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P74235 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l0-finance|1082125550": {
    "project_alias": "ir-l0-finance",
    "config_id": "1082125550",
    "name": "Send Flexibee CZ File",
    "description": "Created from Send Xero US File version #1\n\nCreated from Send Xero CA File version #4\n\nThis is set up with the flow to run",
    "is_disabled": true,
    "version": 1,
    "last_modified": "2025-02-12T17:19:58+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 75149,
        "name": "Get Current Recipients",
        "depends_on": [],
        "tasks": [
          {
            "name": "Invoice Mailing Lists",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1081987319",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 74235,
        "name": "Send File with Invoices",
        "depends_on": [
          75149
        ],
        "tasks": [
          {
            "name": "Flexibee CZ Invoice File",
            "component_id": "kds-team.app-mailgun-v2",
            "component_short": "app-mailgun-v2",
            "config_id": "1082124581",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P75149[\"Get Current Recipients<br/>---<br/>EX: Invoice Mailing Lists\"]\n    P74235[\"Send File with Invoices<br/>---<br/>AP: Flexibee CZ Invoice File\"]\n    P75149 --> P74235\n\n    style P75149 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P74235 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l0-finance|1084869979": {
    "project_alias": "ir-l0-finance",
    "config_id": "1084869979",
    "name": "Send Xero GB File",
    "description": "Created from Send Xero CA File version #2\n\nThis is set up with the flow to run",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:10:01+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 75149,
        "name": "Get Current Recipients",
        "depends_on": [],
        "tasks": [
          {
            "name": "Invoice Mailing Lists",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1081987319",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 74235,
        "name": "Send File with Invoices",
        "depends_on": [
          75149
        ],
        "tasks": [
          {
            "name": "Xero GB Invoice File",
            "component_id": "kds-team.app-mailgun-v2",
            "component_short": "app-mailgun-v2",
            "config_id": "1084872229",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P75149[\"Get Current Recipients<br/>---<br/>EX: Invoice Mailing Lists\"]\n    P74235[\"Send File with Invoices<br/>---<br/>AP: Xero GB Invoice File\"]\n    P75149 --> P74235\n\n    style P75149 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P74235 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l0-finance|1089905479": {
    "project_alias": "ir-l0-finance",
    "config_id": "1089905479",
    "name": "SFDC Invoice update",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:10:01+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 92669,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keboola - specific tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "1089788223",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 64855,
        "name": "Step 2",
        "depends_on": [
          92669
        ],
        "tasks": [
          {
            "name": "Salesforce - Invoice Update",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1089790317",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 86723,
        "name": "Step 3",
        "depends_on": [
          64855
        ],
        "tasks": [
          {
            "name": "Invoice Update",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1089917412",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P92669[\"Step 1<br/>---<br/>EX: Keboola - specific tables\"]\n    P64855[\"Step 2<br/>---<br/>TR: Salesforce - Invoice Update\"]\n    P92669 --> P64855\n    P86723[\"Step 3<br/>---<br/>WR: Invoice Update\"]\n    P64855 --> P86723\n\n    style P92669 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P64855 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P86723 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-finance|1093287107": {
    "project_alias": "ir-l0-finance",
    "config_id": "1093287107",
    "name": "Periodic update of Flexibee",
    "description": "This is to capture any difference happening more than 60 days ago (this is the period set up for the daily updates for flexibee). ",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-02-18T06:37:22+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 30862,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Flexibee Czech Runner FL periodic",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1093284022",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Flexibee Europe Runner FL periodic",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1093284556",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Flexibee Industries Runner FL periodic",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1093283150",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 20024,
        "name": "Step 2",
        "depends_on": [
          30862
        ],
        "tasks": [
          {
            "name": "Journal periodic update",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1092065331",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 4,
    "total_phases": 2,
    "mermaid": "graph LR\n    P30862[\"Extraction<br/>---<br/>AP: Flexibee Czech Runner FL periodic<br/>AP: Flexibee Europe Runner FL periodic<br/>AP: Flexibee Industries Runner FL periodic\"]\n    P20024[\"Step 2<br/>---<br/>TR: Journal periodic update\"]\n    P30862 --> P20024\n\n    style P30862 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P20024 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-finance|1196639363": {
    "project_alias": "ir-l0-finance",
    "config_id": "1196639363",
    "name": "Exchange Rates Update",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:10:01+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 40825,
        "name": "Download Exchange Rates from both CNB and ECB",
        "depends_on": [],
        "tasks": [
          {
            "name": "CNB Exchange Rates",
            "component_id": "keboola.ex-cnb-exchange-rates",
            "component_short": "ex-cnb-exchange-rates",
            "config_id": "1195911936",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "USD Exchange Rates",
            "component_id": "keboola.ex-currency",
            "component_short": "ex-currency",
            "config_id": "460644953",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 87820,
        "name": "Create Exchange Rates Tables",
        "depends_on": [
          40825
        ],
        "tasks": [
          {
            "name": "Exchange Rates CNB",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1195911181",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Exchange Rates",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836620379",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 4,
    "total_phases": 2,
    "mermaid": "graph LR\n    P40825[\"Download Exchange Rates from both CNB and ECB<br/>---<br/>EX: CNB Exchange Rates<br/>EX: USD Exchange Rates\"]\n    P87820[\"Create Exchange Rates Tables<br/>---<br/>TR: Exchange Rates CNB<br/>TR: Exchange Rates\"]\n    P40825 --> P87820\n\n    style P40825 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P87820 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-finance|20244021": {
    "project_alias": "ir-l0-finance",
    "config_id": "20244021",
    "name": "Leadership Overview Daily Update",
    "description": "",
    "is_disabled": false,
    "version": 6,
    "last_modified": "2026-01-18T23:51:34+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 66322,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Files for Leadership",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "20412584",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Budget",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1107554849",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 65810,
        "name": "Step 2",
        "depends_on": [
          66322
        ],
        "tasks": [
          {
            "name": "Leadership KPIs, Budget and P&L",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "20213963",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 98804,
        "name": "Step 3",
        "depends_on": [
          65810
        ],
        "tasks": [
          {
            "name": "Leadership KPIs Snapshot",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "20218457",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 10893,
        "name": "Step 4",
        "depends_on": [
          98804
        ],
        "tasks": [
          {
            "name": "Leadership AE Cohorts",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k56293q89dvtb2w3jdadj7qe",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 4,
    "mermaid": "graph LR\n    P66322[\"Step 1<br/>---<br/>EX: Files for Leadership<br/>EX: Budget\"]\n    P65810[\"Step 2<br/>---<br/>TR: Leadership KPIs, Budget and PandL\"]\n    P66322 --> P65810\n    P98804[\"Step 3<br/>---<br/>TR: Leadership KPIs Snapshot\"]\n    P65810 --> P98804\n    P10893[\"Step 4<br/>---<br/>TR: Leadership AE Cohorts\"]\n    P98804 --> P10893\n\n    style P66322 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P65810 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P98804 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P10893 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-finance|842058210": {
    "project_alias": "ir-l0-finance",
    "config_id": "842058210",
    "name": "Financial Reporting",
    "description": "",
    "is_disabled": false,
    "version": 6,
    "last_modified": "2025-12-09T11:28:23+0100",
    "last_modified_by": "regina.dzhuraeva@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Data Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "USD Exchange Rates",
            "component_id": "keboola.ex-currency",
            "component_short": "ex-currency",
            "config_id": "460644953",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "CEB statements",
            "component_id": "kds-team.ex-csob-ceb",
            "component_short": "ex-csob-ceb",
            "config_id": "461429923",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Fin Master Sheets Data",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "573640538",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Budget",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1107554849",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Snowflake KBC Stack Overview",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1159681817",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 2,
        "name": "Transformation 2",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Finance Main",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836621576",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 23409,
        "name": "Journal Snapshots creation",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "Journal Snapshots",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1093288469",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 61543,
        "name": "Mask Sensitive Infromation",
        "depends_on": [
          23409
        ],
        "tasks": [
          {
            "name": "Journal Line GoodData",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1189816724",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 8,
    "total_phases": 4,
    "mermaid": "graph LR\n    P0[\"Data Extraction<br/>---<br/>EX: USD Exchange Rates<br/>EX: CEB statements *<br/>EX: Fin Master Sheets Data *<br/>EX: Budget<br/>EX: Snowflake KBC Stack Overview\"]\n    P2[\"Transformation 2<br/>---<br/>TR: Finance Main\"]\n    P0 --> P2\n    P23409[\"Journal Snapshots creation<br/>---<br/>TR: Journal Snapshots\"]\n    P2 --> P23409\n    P61543[\"Mask Sensitive Infromation<br/>---<br/>TR: Journal Line GoodData\"]\n    P23409 --> P61543\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P2 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P23409 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P61543 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-finance|842058213": {
    "project_alias": "ir-l0-finance",
    "config_id": "842058213",
    "name": "Accounting Systems Data Update",
    "description": "",
    "is_disabled": false,
    "version": 6,
    "last_modified": "2025-02-18T06:37:02+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 24627,
        "name": "Update period for Flexibee",
        "depends_on": [],
        "tasks": [
          {
            "name": "Flexibee Dates",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1071072452",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [
          24627
        ],
        "tasks": [
          {
            "name": "Keboola",
            "component_id": "keboola.ex-stripe",
            "component_short": "ex-stripe",
            "config_id": "772961316",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Xero LLC",
            "component_id": "kds-team.ex-xero-accounting-v2",
            "component_short": "ex-xero-accounting-v2",
            "config_id": "979132428",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Xero KDS",
            "component_id": "kds-team.ex-xero-accounting-v2",
            "component_short": "ex-xero-accounting-v2",
            "config_id": "989232853",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Flexibee Czech Runner",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1072545677",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "AP"
          },
          {
            "name": "Flexibee Europe Runner",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1072526486",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "AP"
          },
          {
            "name": "Flexibee Industries Runner",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1071073288",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "AP"
          },
          {
            "name": "Xero LTD",
            "component_id": "kds-team.ex-xero-accounting-v2",
            "component_short": "ex-xero-accounting-v2",
            "config_id": "1083157414",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Flexibee Czech Runner FL daily",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1093283484",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "AP"
          },
          {
            "name": "Flexibee Europe Runner FL daily",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1093284257",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "AP"
          },
          {
            "name": "Flexibee Industries Runner FL daily",
            "component_id": "kds-team.app-component-runner",
            "component_short": "app-component-runner",
            "config_id": "1093283044",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 11,
    "total_phases": 2,
    "mermaid": "graph LR\n    P24627[\"Update period for Flexibee<br/>---<br/>TR: Flexibee Dates\"]\n    P0[\"Extraction<br/>---<br/>EX: Keboola *<br/>EX: Xero LLC *<br/>EX: Xero KDS *<br/>AP: Flexibee Czech Runner *<br/>AP: Flexibee Europe Runner *<br/>AP: Flexibee Industries Runner *<br/>EX: Xero LTD *<br/>AP: Flexibee Czech Runner FL daily *<br/>AP: Flexibee Europe Runner FL daily *<br/>AP: Flexibee Industries Runner FL daily *\"]\n    P24627 --> P0\n\n    style P24627 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb"
  },
  "ir-l0-marketing|01k1rqbpe4pwf5aj0eyxp98d60": {
    "project_alias": "ir-l0-marketing",
    "config_id": "01k1rqbpe4pwf5aj0eyxp98d60",
    "name": "Daily Website Analytics Dashboard",
    "description": "Automated daily processing of website analytics data to generate engagement dashboard metrics, user behavior insights, and page performance tracking.",
    "is_disabled": false,
    "version": 1,
    "last_modified": "2025-08-03T21:43:25+0200",
    "last_modified_by": "martin.lepka@keboola.com",
    "phases": [
      {
        "id": "data_processing",
        "name": "Process Website Analytics",
        "depends_on": [],
        "tasks": [
          {
            "name": "Website Engagement Dashboard",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k1rqafvx3rx7qk39gkgbhqr2",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    Pdata_processing[\"Process Website Analytics<br/>---<br/>TR: Website Engagement Dashboard\"]\n\n    style Pdata_processing fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-marketing|1208157016": {
    "project_alias": "ir-l0-marketing",
    "config_id": "1208157016",
    "name": "GA Events/Sessions table",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:10:33+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 97946,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keboola Web Analytics",
            "component_id": "keboola.ex-google-bigquery-v2",
            "component_short": "ex-google-bigquery-v2",
            "config_id": "1204843281",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P97946[\"Step 1<br/>---<br/>EX: Keboola Web Analytics\"]\n\n    style P97946 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb"
  },
  "ir-l0-marketing|1211717440": {
    "project_alias": "ir-l0-marketing",
    "config_id": "1211717440",
    "name": "Marketing_Customer_io",
    "description": "Daily Schedule for Customer.io data",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:10:34+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 458,
        "name": "New Phase",
        "depends_on": [],
        "tasks": [
          {
            "name": "Customer.io",
            "component_id": "kds-team.ex-customer-io",
            "component_short": "ex-customer-io",
            "config_id": "1183468941",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P458[\"New Phase<br/>---<br/>EX: Customer.io\"]\n\n    style P458 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb"
  },
  "ir-l0-people|1156596942": {
    "project_alias": "ir-l0-people",
    "config_id": "1156596942",
    "name": "Main",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:10:46+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 95947,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Humaans",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "1156509383",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 83759,
        "name": "Transformation",
        "depends_on": [
          95947
        ],
        "tasks": [
          {
            "name": "Humaans",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1156625872",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P95947[\"Extraction<br/>---<br/>EX: Humaans\"]\n    P83759[\"Transformation<br/>---<br/>TR: Humaans\"]\n    P95947 --> P83759\n\n    style P95947 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P83759 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-people|1207867530": {
    "project_alias": "ir-l0-people",
    "config_id": "1207867530",
    "name": "BambooHR",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-05-03T23:30:04+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 26858,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "BambooHR [DEV]",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "1207176594",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 48098,
        "name": "Step 2",
        "depends_on": [
          26858
        ],
        "tasks": [
          {
            "name": "BambooHR",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1207866928",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 49537,
        "name": "Step 3",
        "depends_on": [
          48098
        ],
        "tasks": [
          {
            "name": "Snapshots",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "20168736",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P26858[\"Step 1<br/>---<br/>EX: BambooHR [DEV]\"]\n    P48098[\"Step 2<br/>---<br/>TR: BambooHR\"]\n    P26858 --> P48098\n    P49537[\"Step 3<br/>---<br/>TR: Snapshots\"]\n    P48098 --> P49537\n\n    style P26858 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P48098 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P49537 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-product|1162839131": {
    "project_alias": "ir-l0-product",
    "config_id": "1162839131",
    "name": " NPS Flow",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-02-13T00:10:58+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 27518,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "NPS feedback transformation only real data",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1092068160",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P27518[\"Step 1<br/>---<br/>TR: NPS feedback transformation only real data\"]\n\n    style P27518 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-product|1178935891": {
    "project_alias": "ir-l0-product",
    "config_id": "1178935891",
    "name": "Heap Info Update",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-02-13T00:10:58+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 86395,
        "name": "Data Preparation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Heap - Add Account Property",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1178923981",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Heap - Track (identity)",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1178924967",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Heap - Add User Property",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1182728506",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 22136,
        "name": "Upload into Heap",
        "depends_on": [
          86395
        ],
        "tasks": [
          {
            "name": "Heap API - Add Account Properties",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1178925508",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Heap API - Track - Errors",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1178925904",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Heap API - Add User Properties",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1182727902",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 2,
    "mermaid": "graph LR\n    P86395[\"Data Preparation<br/>---<br/>TR: Heap - Add Account Property<br/>TR: Heap - Track [identity]<br/>TR: Heap - Add User Property\"]\n    P22136[\"Upload into Heap<br/>---<br/>WR: Heap API - Add Account Properties<br/>WR: Heap API - Add User Properties\"]\n    P86395 --> P22136\n\n    style P86395 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P22136 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-product|1183033654": {
    "project_alias": "ir-l0-product",
    "config_id": "1183033654",
    "name": "Product Features",
    "description": "Automated workflow to synchronize product features and feedback categories from Google Sheets to Jira custom field options. This flow extracts product data, transforms it for Jira integration, and updates CI custom field options through API calls.",
    "is_disabled": false,
    "version": 10,
    "last_modified": "2025-12-16T08:10:02+0100",
    "last_modified_by": "david@keboola.com",
    "phases": [
      {
        "id": 4590,
        "name": "Data Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Jira - CI field options",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "1183327081",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Product Features",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1182952381",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 9158,
        "name": "Data Transformation",
        "depends_on": [
          4590
        ],
        "tasks": [
          {
            "name": "Product Feature - Configuration IDs",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1182959644",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Product Features - BDM table",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1217956879",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 71075,
        "name": "Update labels in linear",
        "depends_on": [
          9158
        ],
        "tasks": [
          {
            "name": "Sync Component Labels to Linear",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01kcjyjxx8wt5xqh4sgzz4z6vv",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 85685,
        "name": "Jira Field Deletions",
        "depends_on": [
          71075
        ],
        "tasks": [
          {
            "name": "[Product] Update Jira CI field [DELETE]",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1217945402",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 58390,
        "name": "Jira Field Updates",
        "depends_on": [
          85685
        ],
        "tasks": [
          {
            "name": "[Product] Update Jira CI field [UPDATE]",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1191781320",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 72170,
        "name": "Jira Field Additions",
        "depends_on": [
          58390
        ],
        "tasks": [
          {
            "name": "[Product] Update Jira CI field [ADD]",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1183021449",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 8,
    "total_phases": 6,
    "mermaid": "graph LR\n    P4590[\"Data Extraction<br/>---<br/>EX: Jira - CI field options<br/>EX: Product Features\"]\n    P9158[\"Data Transformation<br/>---<br/>TR: Product Feature - Configuration IDs<br/>TR: Product Features - BDM table\"]\n    P4590 --> P9158\n    P71075[\"Update labels in linear<br/>---<br/>AP: Sync Component Labels to Linear\"]\n    P9158 --> P71075\n    P85685[\"Jira Field Deletions<br/>---<br/>WR: [Product] Update Jira CI field [DELETE]\"]\n    P71075 --> P85685\n    P58390[\"Jira Field Updates<br/>---<br/>WR: [Product] Update Jira CI field [UPDATE]\"]\n    P85685 --> P58390\n    P72170[\"Jira Field Additions<br/>---<br/>WR: [Product] Update Jira CI field [ADD]\"]\n    P58390 --> P72170\n\n    style P4590 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P9158 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P71075 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P85685 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P58390 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P72170 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-product|1217528907": {
    "project_alias": "ir-l0-product",
    "config_id": "1217528907",
    "name": "Time to Value",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-02-13T00:10:59+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 20823,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "30 Days User",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1193773615",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P20823[\"Step 1<br/>---<br/>TR: 30 Days User\"]\n\n    style P20823 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-product|15975867": {
    "project_alias": "ir-l0-product",
    "config_id": "15975867",
    "name": "Raw data processing- AI flow builder",
    "description": "",
    "is_disabled": true,
    "version": 8,
    "last_modified": "2025-08-28T08:15:35+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 40493,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "azure_north_east_raw_data",
            "component_id": "keboola.ex-storage",
            "component_short": "ex-storage",
            "config_id": "15941961",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "gcp_us_4_raw_data",
            "component_id": "keboola.ex-storage",
            "component_short": "ex-storage",
            "config_id": "16015240",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 43554,
        "name": "Step 2",
        "depends_on": [
          40493
        ],
        "tasks": [
          {
            "name": "raw data processing Azure-northe-east",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "15943706",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "raw data processing GCP-us-4",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "16016628",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P40493[\"Step 1<br/>[no enabled tasks]\"]\n    P43554[\"Step 2<br/>---<br/>TR: raw data processing Azure-northe-east<br/>TR: raw data processing GCP-us-4\"]\n    P40493 --> P43554\n\n    style P40493 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P43554 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-product|842025284": {
    "project_alias": "ir-l0-product",
    "config_id": "842025284",
    "name": "Main",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-02-13T00:10:59+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 3,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Product Jira Data",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "750353525",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P3[\"Transformation<br/>---<br/>TR: Product Jira Data\"]\n\n    style P3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-product|842025288": {
    "project_alias": "ir-l0-product",
    "config_id": "842025288",
    "name": "Component Requests Processing",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-02-13T00:10:59+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 83430,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "KCF Priority Sheet",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "948117175",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 81540,
        "name": "Step 2",
        "depends_on": [
          83430
        ],
        "tasks": [
          {
            "name": "Component Requests",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "773049313",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 9862,
        "name": "Collect and calculate score",
        "depends_on": [
          81540
        ],
        "tasks": [
          {
            "name": "Process Component Requests",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "946373552",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 74184,
        "name": "Update score in Jira",
        "depends_on": [
          9862
        ],
        "tasks": [
          {
            "name": "[KCF] Update Jira Epic Scores",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "946577682",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "[KCF] Update Jira Feature Scores",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "982079286",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 4,
    "mermaid": "graph LR\n    P83430[\"Step 1<br/>---<br/>WR: KCF Priority Sheet\"]\n    P81540[\"Step 2<br/>---<br/>EX: Component Requests\"]\n    P83430 --> P81540\n    P9862[\"Collect and calculate score<br/>---<br/>TR: Process Component Requests\"]\n    P81540 --> P9862\n    P74184[\"Update score in Jira<br/>---<br/>WR: [KCF] Update Jira Epic Scores<br/>WR: [KCF] Update Jira Feature Scores\"]\n    P9862 --> P74184\n\n    style P83430 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P81540 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P9862 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P74184 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-product|939738634": {
    "project_alias": "ir-l0-product",
    "config_id": "939738634",
    "name": "Product Analysis",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-02-13T00:11:00+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 84606,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "KBC Component Configurations Analysis",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "939094686",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 58691,
        "name": "Disabled load to Looker",
        "depends_on": [
          84606
        ],
        "tasks": [
          {
            "name": "Looker",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "939110992",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Looker [PROD]",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1168574313",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 2,
    "mermaid": "graph LR\n    P84606[\"Step 1<br/>---<br/>TR: KBC Component Configurations Analysis\"]\n    P58691[\"Disabled load to Looker<br/>[no enabled tasks]\"]\n    P84606 --> P58691\n\n    style P84606 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P58691 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l0-professional-services|14669904": {
    "project_alias": "ir-l0-professional-services",
    "config_id": "14669904",
    "name": "KIT SFDC mapping",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-02-14T16:39:43+0100",
    "last_modified_by": "adam.zetocha@keboola.com",
    "phases": [
      {
        "id": 27105,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "KIT account allocation",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1223964966",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 48726,
        "name": "Step 2",
        "depends_on": [
          27105
        ],
        "tasks": [
          {
            "name": "KIT - SFDC IDs mapping",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "14662292",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 60293,
        "name": "Step 3",
        "depends_on": [
          48726
        ],
        "tasks": [
          {
            "name": "KIT - account allocation SFDC IDs",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "14659905",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P27105[\"Step 1<br/>---<br/>EX: KIT account allocation\"]\n    P48726[\"Step 2<br/>---<br/>TR: KIT - SFDC IDs mapping\"]\n    P27105 --> P48726\n    P60293[\"Step 3<br/>---<br/>WR: KIT - account allocation SFDC IDs\"]\n    P48726 --> P60293\n\n    style P27105 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P48726 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P60293 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-professional-services|842058227": {
    "project_alias": "ir-l0-professional-services",
    "config_id": "842058227",
    "name": "Main",
    "description": "",
    "is_disabled": false,
    "version": 5,
    "last_modified": "2025-07-30T13:02:13+0200",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Calendarific",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "500466927",
            "enabled": false,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Operations",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "519880342",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "KDS",
            "component_id": "kds-team.ex-jira",
            "component_short": "ex-jira",
            "config_id": "603990443",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "KDS FULL Issues",
            "component_id": "kds-team.ex-jira",
            "component_short": "ex-jira",
            "config_id": "725092312",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "KDS Team Dev Portal",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "897784880",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformations 01",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Jira - Main",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836636119",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Work Calendar",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836639046",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "01 Team Planning - Preparation",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "836638725",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "KDS Team Component Versions",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "897815509",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Transformations 02",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "02 Team Planning - Finalization",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836639044",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 3,
        "name": "Load",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "JIRA Users",
            "component_id": "keboola.wr-google-drive",
            "component_short": "wr-google-drive",
            "config_id": "1092448527",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 10,
    "total_phases": 4,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: Operations<br/>EX: KDS<br/>EX: KDS FULL Issues<br/>EX: KDS Team Dev Portal *\"]\n    P1[\"Transformations 01<br/>---<br/>TR: Jira - Main<br/>TR: Work Calendar<br/>TR: 01 Team Planning - Preparation<br/>TR: KDS Team Component Versions\"]\n    P0 --> P1\n    P2[\"Transformations 02<br/>---<br/>TR: 02 Team Planning - Finalization\"]\n    P1 --> P2\n    P3[\"Load<br/>---<br/>WR: JIRA Users\"]\n    P2 --> P3\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P3 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-professional-services|842058230": {
    "project_alias": "ir-l0-professional-services",
    "config_id": "842058230",
    "name": "Academy",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:11:07+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Thinkific",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "681176146",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation 01",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "01 Academy - Main",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836627919",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Transformation 02",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "02 Academy - Snapshot",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836627925",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: Thinkific\"]\n    P1[\"Transformation 01<br/>---<br/>TR: 01 Academy - Main\"]\n    P0 --> P1\n    P2[\"Transformation 02<br/>---<br/>TR: 02 Academy - Snapshot\"]\n    P1 --> P2\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|01k7175kfg6kecxd2prbxgrce4": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "01k7175kfg6kecxd2prbxgrce4",
    "name": "Salesforce Monitor weekly",
    "description": "Created from Salesforce Monitor monthly version #3",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-10-08T08:33:16+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 89019,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce check - churned",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k71774y7vj2qyjqm3p1ape34",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 26875,
        "name": "Step 2",
        "depends_on": [
          89019
        ],
        "tasks": [
          {
            "name": "Salesforce monitor - churned customers",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01k5mgd936k0aa7g1fxtmkm8gr",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P89019[\"Step 1<br/>---<br/>TR: Salesforce check - churned\"]\n    P26875[\"Step 2<br/>---<br/>AP: Salesforce monitor - churned customers\"]\n    P89019 --> P26875\n\n    style P89019 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P26875 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|01k88q07m55mqs01xjfgcgahze": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "01k88q07m55mqs01xjfgcgahze",
    "name": "Forms - Booking System",
    "description": "Process the booking system leads",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-10-23T16:19:48+0200",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 72996,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Forms - Booking System",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k88q1b2bn2kempt90dpky4bz",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P72996[\"Step 1<br/>---<br/>TR: Forms - Booking System\"]\n\n    style P72996 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|01kas4yyyek0x1zm6q1ht6563b": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "01kas4yyyek0x1zm6q1ht6563b",
    "name": "RB2B",
    "description": "This flow prepare data from Make.com for L2 on who spent time on our pages using RB2B",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-11-23T21:02:42+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 69744,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "RB2B",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kas4zfmntc60ncxpwm04h15t",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P69744[\"Step 1<br/>---<br/>TR: RB2B\"]\n\n    style P69744 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|1154397732": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "1154397732",
    "name": "Scheduled CRM Data Filtered",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:11:36+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [],
        "tasks": [
          {
            "name": "CRM Data Filtered",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1154389927",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Scheduled configuration<br/>---<br/>TR: CRM Data Filtered\"]\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|1173310163": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "1173310163",
    "name": "Scheduled Telemetry for CDP",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:11:36+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [],
        "tasks": [
          {
            "name": "Telemetry for CDP",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1166253034",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Scheduled configuration<br/>---<br/>TR: Telemetry for CDP\"]\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|1188764902": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "1188764902",
    "name": "Forms - Event Signup",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:11:36+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [],
        "tasks": [
          {
            "name": "Forms - Events Signup",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1188503986",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Scheduled configuration<br/>---<br/>TR: Forms - Events Signup\"]\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|1199983571": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "1199983571",
    "name": "Update Organization ID in Salesforce",
    "description": "This flow prepares data for update of Organization ID in Salesforce, this is connected with new way of maintenance of CRM ID",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:11:37+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 38796,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "[DEV] Salesforce Org ID Preparation",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1199931148",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P38796[\"Step 1<br/>---<br/>TR: [DEV] Salesforce Org ID Preparation\"]\n\n    style P38796 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|15136355": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "15136355",
    "name": "Forms - Google Marketplace",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-21T14:25:18+0100",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 81768,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Forms - Google Marketplace",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "15136357",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P81768[\"Step 1<br/>---<br/>TR: Forms - Google Marketplace\"]\n\n    style P81768 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|18426551": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "18426551",
    "name": "Salesforce Chatter",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-04-08T08:45:59+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 56686,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Chatter formatting",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "16972899",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 81535,
        "name": "Step 2",
        "depends_on": [
          56686
        ],
        "tasks": [
          {
            "name": "HTML replacement",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "17016939",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P56686[\"Step 1<br/>---<br/>TR: Chatter formatting\"]\n    P81535[\"Step 2<br/>---<br/>TR: HTML replacement\"]\n    P56686 --> P81535\n\n    style P56686 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P81535 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|18426909": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "18426909",
    "name": "Trigger Salesforce Chatter",
    "description": "This flow make sure the main flow Salesforce feedback runs only every other week (on odd weeks).",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-04-08T08:46:52+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 58309,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce Trigger (Bi-weekly)",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "18426934",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P58309[\"Step 1<br/>---<br/>TR: Salesforce Trigger [Bi-weekly]\"]\n\n    style P58309 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|21065367": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "21065367",
    "name": "Intent Data",
    "description": "Extracts and process data from Leady.cz, G2 intent and Qualified",
    "is_disabled": false,
    "version": 23,
    "last_modified": "2025-12-17T05:40:08+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 73871,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Leady",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "22921814",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "G2",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "22918453",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Qualified",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "23204598",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Qualified Intent Data",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01k0bnqy07nyske5c161nbv124",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 89176,
        "name": "Transformation",
        "depends_on": [
          73871
        ],
        "tasks": [
          {
            "name": "Leady",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "22935419",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "G2",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "22997822",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Qualified",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "23213956",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 2,
    "mermaid": "graph LR\n    P73871[\"Extraction<br/>---<br/>EX: Leady<br/>EX: G2<br/>EX: Qualified Intent Data\"]\n    P89176[\"Transformation<br/>---<br/>TR: Leady<br/>TR: G2\"]\n    P73871 --> P89176\n\n    style P73871 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P89176 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|21955258": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "21955258",
    "name": "Salesforce Monitor daily",
    "description": "",
    "is_disabled": false,
    "version": 8,
    "last_modified": "2025-09-15T07:45:34+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 28500,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce check",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "21869188",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 32864,
        "name": "Step 2",
        "depends_on": [
          28500
        ],
        "tasks": [
          {
            "name": "Salesforce monitor",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "21952179",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Salesforce monitor - activated order",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "22097814",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 32192,
        "name": "Step 3",
        "depends_on": [
          32864
        ],
        "tasks": [
          {
            "name": "Salesforce monitor -orderitems date",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "23638271",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 88178,
        "name": "Step 4",
        "depends_on": [
          32192
        ],
        "tasks": [
          {
            "name": "Salesforce monitor - Snowflake reseller",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01k4wbzqa6wpbtbd01dak3qsga",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 4,
    "total_phases": 4,
    "mermaid": "graph LR\n    P28500[\"Step 1<br/>---<br/>TR: Salesforce check\"]\n    P32864[\"Step 2<br/>---<br/>AP: Salesforce monitor\"]\n    P28500 --> P32864\n    P32192[\"Step 3<br/>---<br/>AP: Salesforce monitor -orderitems date\"]\n    P32864 --> P32192\n    P88178[\"Step 4<br/>---<br/>AP: Salesforce monitor - Snowflake reseller\"]\n    P32192 --> P88178\n\n    style P28500 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P32864 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P32192 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P88178 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|22288157": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "22288157",
    "name": "Salesforce Monitor monthly",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-06-09T12:12:32+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 89019,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce check - organizations",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "22110980",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 26875,
        "name": "Step 2",
        "depends_on": [
          89019
        ],
        "tasks": [
          {
            "name": "Salesforce monitor - organization spend",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "22287588",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 19395,
        "name": "Step 3",
        "depends_on": [
          26875
        ],
        "tasks": [
          {
            "name": "Not billed organizations",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "22782144",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P89019[\"Step 1<br/>---<br/>TR: Salesforce check - organizations\"]\n    P26875[\"Step 2<br/>---<br/>AP: Salesforce monitor - organization spend\"]\n    P89019 --> P26875\n    P19395[\"Step 3<br/>---<br/>WR: Not billed organizations\"]\n    P26875 --> P19395\n\n    style P89019 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P26875 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P19395 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|842027364": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "842027364",
    "name": "Main",
    "description": "",
    "is_disabled": false,
    "version": 20,
    "last_modified": "2026-02-27T20:49:16+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Sales Data & Entity Bank Accounts",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "501029757",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Emails and newsletters",
            "component_id": "kds-team.ex-customer-io",
            "component_short": "ex-customer-io",
            "config_id": "610452216",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Developer Portal",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "697610579",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Developer Portal (FULL)",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "697625194",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Keboola",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723966296",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Keboola Google Analytics",
            "component_id": "keboola.ex-google-bigquery-v2",
            "component_short": "ex-google-bigquery-v2",
            "config_id": "01jykgnyqgxkrf9x0rp41c2wdk",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "GTM Sheets",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "18510245",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Developer Portal",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "833554831",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          },
          {
            "name": "Customer.io Prep",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "833555254",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          },
          {
            "name": "CRM Model - Main",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "833574433",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Public Telemetry Update (Salesforce Data)",
            "component_id": "kds-team.app-orchestration-trigger-queue-v2",
            "component_short": "app-orchestration-trigger-queue-v2",
            "config_id": "1223257696",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "FL"
          },
          {
            "name": "Google Analytics",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k06z80mxf3xr71q9w663nzbr",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          },
          {
            "name": "Marketing Targets",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k1aqk3fpphq916qrfy0ydtnc",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Transformation 2",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "CRM Model - Snapshots Processing & Tables for Public Telemetry",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "833574437",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "KBC Data Enrichment",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k1jewzva5m87dr9h4pdxpjsh",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "CRM Model - PAYG MRR",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "841635303",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "PAYG Project Activity",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k1z8zcm168dv130nf1twpsdx",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "CRM Model - Snapshots Processing & Tables for Public Telemetry (Copy)",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kjg4zqt41eqw241gb5ptyycd",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 3,
        "name": "Load",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "Telemetry Data Extractor (DEV)",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "673065819",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Telemetry Data Extractor (PROD)",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1085259970",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Lead Scoring & Enrichment Trigger",
            "component_id": "kds-team.app-orchestration-trigger-queue-v2",
            "component_short": "app-orchestration-trigger-queue-v2",
            "config_id": "01kbqtb9d4e9qgy99vg1p6ys4n",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "FL"
          }
        ]
      }
    ],
    "total_tasks": 20,
    "total_phases": 4,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: Sales Data and Entity Bank Accounts *<br/>EX: Emails and newsletters *<br/>EX: Developer Portal *<br/>EX: Developer Portal [FULL] *<br/>EX: Keboola<br/>EX: Keboola Google Analytics *<br/>EX: GTM Sheets\"]\n    P1[\"Transformation<br/>---<br/>TR: Developer Portal *<br/>TR: Customer.io Prep *<br/>TR: CRM Model - Main<br/>FL: Public Telemetry Update [Salesforce Data]<br/>TR: Google Analytics *<br/>TR: Marketing Targets *\"]\n    P0 --> P1\n    P2[\"Transformation 2<br/>---<br/>TR: CRM Model - Snapshots Processing and Tables for...<br/>TR: KBC Data Enrichment<br/>TR: CRM Model - PAYG MRR<br/>TR: PAYG Project Activity<br/>TR: CRM Model - Snapshots Processing and Tables for...\"]\n    P1 --> P2\n    P3[\"Load<br/>---<br/>WR: Telemetry Data Extractor [PROD]<br/>FL: Lead Scoring and Enrichment Trigger *\"]\n    P2 --> P3\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P3 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|842027430": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "842027430",
    "name": "Webinars",
    "description": "",
    "is_disabled": true,
    "version": 1,
    "last_modified": "2025-02-12T19:19:58+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Webinars",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "833553821",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Transformation<br/>---<br/>TR: Webinars\"]\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|842027537": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "842027537",
    "name": "Forms - Gated Content",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:11:39+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Forms - Gated Content Requests",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "833505599",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P0[\"Transformation<br/>---<br/>TR: Forms - Gated Content Requests\"]\n\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|842027544": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "842027544",
    "name": "Forms - Newsletter Sign Up",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:11:39+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Forms - Newsletter Sign Up",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "833504897",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P0[\"Transformation<br/>---<br/>TR: Forms - Newsletter Sign Up\"]\n\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|842027566": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "842027566",
    "name": "Forms - Webinar Registrations",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-03-17T21:29:03+0100",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Forms - Webinar Registrations",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "833506952",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Transformation<br/>---<br/>TR: Forms - Webinar Registrations\"]\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-sales-marketing|951536779": {
    "project_alias": "ir-l0-sales-marketing",
    "config_id": "951536779",
    "name": "KBC Data for Web Analytics",
    "description": "Sharing data to externally managed marketing project.",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:11:39+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 58145,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "KBC Data for Web Analytics",
            "component_id": "keboola.wr-storage",
            "component_short": "wr-storage",
            "config_id": "951535645",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P58145[\"Step 1<br/>---<br/>WR: KBC Data for Web Analytics\"]\n\n    style P58145 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-snowflake-usage-data|1028456626": {
    "project_alias": "ir-l0-snowflake-usage-data",
    "config_id": "1028456626",
    "name": "Reseller Usage Data",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-09-08T18:50:11+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Reseller Snowflake BYODB",
            "component_id": "keboola.ex-db-snowflake",
            "component_short": "ex-db-snowflake",
            "config_id": "1028181349",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 52426,
        "name": "Step 2",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Snowflake Contracts Usage",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1030695161",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 98327,
        "name": "Step 3",
        "depends_on": [
          52426
        ],
        "tasks": [
          {
            "name": "L2 [PROD] - KBC Billing Writer",
            "component_id": "keboola.wr-storage",
            "component_short": "wr-storage",
            "config_id": "20917696",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P1[\"Step 1<br/>---<br/>EX: Reseller Snowflake BYODB\"]\n    P52426[\"Step 2<br/>---<br/>TR: Snowflake Contracts Usage\"]\n    P1 --> P52426\n    P98327[\"Step 3<br/>---<br/>WR: L2 [PROD] - KBC Billing Writer\"]\n    P52426 --> P98327\n\n    style P1 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P52426 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P98327 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-support|22445236": {
    "project_alias": "ir-l0-support",
    "config_id": "22445236",
    "name": "Jira Context enrichment",
    "description": "This was moved to IR L1 [Data Processes] Support project.",
    "is_disabled": true,
    "version": 5,
    "last_modified": "2025-06-23T09:13:20+0200",
    "last_modified_by": "jakub.sochan@keboola.com",
    "phases": [
      {
        "id": 38032,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "snowflake-transformation (22409193)",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "22409193",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 38867,
        "name": "Step 2",
        "depends_on": [
          38032
        ],
        "tasks": [
          {
            "name": "Jira Context enrichment",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "22420472",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 2,
    "mermaid": "graph LR\n    P38032[\"Step 1<br/>[no enabled tasks]\"]\n    P38867[\"Step 2<br/>---<br/>WR: Jira Context enrichment\"]\n    P38032 --> P38867\n\n    style P38032 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P38867 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l0-support|41231054": {
    "project_alias": "ir-l0-support",
    "config_id": "41231054",
    "name": "Jira org pull",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-06-15T21:25:58+0200",
    "last_modified_by": "jakub.sochan@keboola.com",
    "phases": [
      {
        "id": 49801,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Get JSM Organization objects",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "22714479",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 13775,
        "name": "Step 2",
        "depends_on": [
          49801
        ],
        "tasks": [
          {
            "name": "JIRA Organizations complete",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "22851706",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 34897,
        "name": "Step 3",
        "depends_on": [
          13775
        ],
        "tasks": [
          {
            "name": "SFDC to JIRA Organizations Mapping",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "22901820",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P49801[\"Step 1<br/>---<br/>EX: Get JSM Organization objects\"]\n    P13775[\"Step 2<br/>---<br/>TR: JIRA Organizations complete\"]\n    P49801 --> P13775\n    P34897[\"Step 3<br/>---<br/>TR: SFDC to JIRA Organizations Mapping\"]\n    P13775 --> P34897\n\n    style P49801 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P13775 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P34897 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-support|842058240": {
    "project_alias": "ir-l0-support",
    "config_id": "842058240",
    "name": "Support",
    "description": "Jira data are extracted in Operations project and Zendesk data are no longer accessible.",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-06-11T12:34:56+0200",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Transformation 01",
        "depends_on": [],
        "tasks": [
          {
            "name": "Jira Support",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "998312384",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Transformation 01<br/>---<br/>TR: Jira Support\"]\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l0-support|842058253": {
    "project_alias": "ir-l0-support",
    "config_id": "842058253",
    "name": "Telemetry & Metadata (Data Discovery)",
    "description": "",
    "is_disabled": true,
    "version": 4,
    "last_modified": "2025-02-19T09:29:30+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Support Telemetry",
            "component_id": "keboola.ex-telemetry-data",
            "component_short": "ex-telemetry-data",
            "config_id": "708529106",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Support Metadata",
            "component_id": "kds-team.ex-kbc-project-metadata-v2",
            "component_short": "ex-kbc-project-metadata-v2",
            "config_id": "708528395",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Load",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Data Discovery - Telemetry",
            "component_id": "keboola.wr-storage",
            "component_short": "wr-storage",
            "config_id": "708528007",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Data Discovery - Metadata",
            "component_id": "keboola.wr-storage",
            "component_short": "wr-storage",
            "config_id": "708527966",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: Support Telemetry\"]\n    P1[\"Load<br/>---<br/>WR: Data Discovery - Metadata\"]\n    P0 --> P1\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-customer-success|988967780": {
    "project_alias": "ir-l1-data-processes-customer-success",
    "config_id": "988967780",
    "name": "Triggered Notifications - Overconsumption",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:12:08+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 81341,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "CE - Triggered Notifications",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "986923051",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 93898,
        "name": "Transformation 01 - Create events",
        "depends_on": [
          81341
        ],
        "tasks": [
          {
            "name": "Customer.io - Overconsumption Events - 01 Create Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "986925031",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 78554,
        "name": "Load 01 - Add missing customers",
        "depends_on": [
          93898
        ],
        "tasks": [
          {
            "name": "Customer.io - Missing Customers",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "987229508",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 3034,
        "name": "Load 02 - Upload events",
        "depends_on": [
          78554
        ],
        "tasks": [
          {
            "name": "Customer.io - Triggered Notifications - Overconsumption",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "987218474",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 60201,
        "name": "Transformation 02 - Store events",
        "depends_on": [
          3034
        ],
        "tasks": [
          {
            "name": "Customer.io - Overconsumption Events - 02 Store Sent Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "988936806",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 5,
    "mermaid": "graph LR\n    P81341[\"Extraction<br/>---<br/>EX: CE - Triggered Notifications\"]\n    P93898[\"Transformation 01 - Create events<br/>---<br/>TR: Customer.io - Overconsumption Events - 01 Creat...\"]\n    P81341 --> P93898\n    P78554[\"Load 01 - Add missing customers<br/>---<br/>WR: Customer.io - Missing Customers\"]\n    P93898 --> P78554\n    P3034[\"Load 02 - Upload events<br/>---<br/>WR: Customer.io - Triggered Notifications - Overcon...\"]\n    P78554 --> P3034\n    P60201[\"Transformation 02 - Store events<br/>---<br/>TR: Customer.io - Overconsumption Events - 02 Store...\"]\n    P3034 --> P60201\n\n    style P81341 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P93898 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P78554 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P3034 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P60201 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l1-data-processes-digital-sales-machine|01jzwjbdb4fhqx8r2yvmkcqta4": {
    "project_alias": "ir-l1-data-processes-digital-sales-machine",
    "config_id": "01jzwjbdb4fhqx8r2yvmkcqta4",
    "name": "PAYG Pricing Research with AI",
    "description": "Complete PAYG pricing research flow with AI-powered language detection and vocative generation for personalized outreach",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-07-11T14:07:12+0200",
    "last_modified_by": "matej.novak@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Data Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "PAYG Projects with Active Users",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01jzwc23gz9tcaqs3z5zs4negy",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Google Sheet Import",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01jzwdrjegqhwg9fkwx9sv5nej",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 2,
        "name": "AI Analysis",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Language and Vocative Analysis",
            "component_id": "kds-team.app-generative-ai",
            "component_short": "app-generative-ai",
            "config_id": "01jzwfnf05nn76q3kbrqtbfyx8",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 3,
        "name": "Final Processing",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "PAYG Final Data with AI Analysis",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01jzwjamfn4ffce262fad0sn6n",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 4,
        "name": "Output",
        "depends_on": [
          3
        ],
        "tasks": [
          {
            "name": "PAYG Pricing Research Results Writer - Clean",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "01jzwnvb2ddj65b188jvb21qtg",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 4,
    "mermaid": "graph LR\n    P1[\"Data Extraction<br/>---<br/>TR: PAYG Projects with Active Users<br/>EX: Google Sheet Import\"]\n    P2[\"AI Analysis<br/>---<br/>AP: Language and Vocative Analysis\"]\n    P1 --> P2\n    P3[\"Final Processing<br/>---<br/>TR: PAYG Final Data with AI Analysis\"]\n    P2 --> P3\n    P4[\"Output<br/>---<br/>WR: PAYG Pricing Research Results Writer - Clean\"]\n    P3 --> P4\n\n    style P1 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P2 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P4 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-digital-sales-machine|01k18sn1pa26k495njc0wam9zq": {
    "project_alias": "ir-l1-data-processes-digital-sales-machine",
    "config_id": "01k18sn1pa26k495njc0wam9zq",
    "name": "PAYG Pricing Analysis Pipeline",
    "description": "Streamlined end-to-end flow for PAYG pricing analysis including: 1) Base PAYG projects analysis with maturity filtering and metrics calculation, 2) Pricing structure setup, 3) Tier eligibility and pricing calculations for all tiers with optimization scenarios, 4) Optimal tier recommendations, and 5) Export to Google Sheets. This flow processes mature PAYG projects to provide comprehensive pricing strategy recommendations.",
    "is_disabled": false,
    "version": 8,
    "last_modified": "2025-07-28T18:42:00+0200",
    "last_modified_by": "matej.novak@keboola.com",
    "phases": [
      {
        "id": "phase_1",
        "name": "Data Foundation",
        "depends_on": [],
        "tasks": [
          {
            "name": "PAYG Projects Analysis - Maturity 3 & 4",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k18j4yjw65sbwwqcngb316gf",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Pricing Structure Table",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k18q7v9rtakc9mkbxa90aj90",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": "phase_2",
        "name": "Tier Analysis",
        "depends_on": [
          "phase_1"
        ],
        "tasks": [
          {
            "name": "Project Tier Eligibility and Pricing Analysis",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k18qpcf722xypr07brkgat1h",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": "phase_3",
        "name": "Optimal Pricing",
        "depends_on": [
          "phase_2"
        ],
        "tasks": [
          {
            "name": "PAYG Projects with Optimal Pricing",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k18rvsgvkcta6464cz3q5vre",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": "phase_4",
        "name": "Export Results",
        "depends_on": [
          "phase_3"
        ],
        "tasks": [
          {
            "name": "PAYG Projects Optimal Pricing Export",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "01k18s7ykk5ry9gktg8ycmanag",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 4,
    "mermaid": "graph LR\n    Pphase_1[\"Data Foundation<br/>---<br/>TR: PAYG Projects Analysis - Maturity 3 and 4<br/>TR: Pricing Structure Table\"]\n    Pphase_2[\"Tier Analysis<br/>---<br/>TR: Project Tier Eligibility and Pricing Analysis\"]\n    Pphase_1 --> Pphase_2\n    Pphase_3[\"Optimal Pricing<br/>---<br/>TR: PAYG Projects with Optimal Pricing\"]\n    Pphase_2 --> Pphase_3\n    Pphase_4[\"Export Results<br/>---<br/>WR: PAYG Projects Optimal Pricing Export\"]\n    Pphase_3 --> Pphase_4\n\n    style Pphase_1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style Pphase_2 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style Pphase_3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style Pphase_4 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-digital-sales-machine|1218822141": {
    "project_alias": "ir-l1-data-processes-digital-sales-machine",
    "config_id": "1218822141",
    "name": "> GET SERP (With Categorization)",
    "description": "Extracts search engine result pages for keywords specified in https://docs.google.com/spreadsheets/d/13LjVwNEFOSILTpTcL3tyjbj7E08NfSjomL-gc-8adqw/edit?gid=0#gid=0 and populates the spreadsheet with results.",
    "is_disabled": false,
    "version": 27,
    "last_modified": "2025-11-06T12:49:53+0100",
    "last_modified_by": "matej.novak@keboola.com",
    "phases": [
      {
        "id": 99170,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keywords to-do",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1218770349",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 95748,
        "name": "Step 2",
        "depends_on": [
          99170
        ],
        "tasks": [
          {
            "name": "Google – Apify",
            "component_id": "apify.apify",
            "component_short": "apify.apify",
            "config_id": "1219630268",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "OT"
          },
          {
            "name": "Bing with Input Mapping – tri_angle/bing-search-scraper",
            "component_id": "apify.apify",
            "component_short": "apify.apify",
            "config_id": "1218773744",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "OT"
          }
        ]
      },
      {
        "id": 75094,
        "name": "Step 3",
        "depends_on": [
          95748
        ],
        "tasks": [
          {
            "name": "Merge SERP",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1219643978",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 49018,
        "name": "Step 4",
        "depends_on": [
          75094
        ],
        "tasks": [
          {
            "name": "Resolve redirects and add Structured data",
            "component_id": "apify.apify",
            "component_short": "apify.apify",
            "config_id": "15479021",
            "enabled": false,
            "continue_on_failure": true,
            "type_icon": "OT"
          }
        ]
      },
      {
        "id": 55991,
        "name": "Step 5",
        "depends_on": [
          49018
        ],
        "tasks": [
          {
            "name": "Structured Data Pre-categorization",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "15571205",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 72854,
        "name": "Step 6",
        "depends_on": [
          55991
        ],
        "tasks": [
          {
            "name": "Find Ad Networks",
            "component_id": "apify.apify",
            "component_short": "apify.apify",
            "config_id": "23617987",
            "enabled": false,
            "continue_on_failure": true,
            "type_icon": "OT"
          }
        ]
      },
      {
        "id": 10364,
        "name": "Step 7",
        "depends_on": [
          72854
        ],
        "tasks": [
          {
            "name": "Enhance data with Ad Networks",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "23621652",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 49644,
        "name": "Step 8",
        "depends_on": [
          10364
        ],
        "tasks": [
          {
            "name": "Categorization",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "17485310",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 52299,
        "name": "Step 9",
        "depends_on": [
          49644
        ],
        "tasks": [
          {
            "name": "Merge categories",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "17493861",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 62966,
        "name": "Step 10",
        "depends_on": [
          52299
        ],
        "tasks": [
          {
            "name": "Add domain info",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "17498136",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 70655,
        "name": "Step 11",
        "depends_on": [
          62966
        ],
        "tasks": [
          {
            "name": "Write categorized keyword analysis results",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "17496930",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 85559,
        "name": "Step 12",
        "depends_on": [
          70655
        ],
        "tasks": [
          {
            "name": "Clean up keywords",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1219655292",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 22692,
        "name": "Step 13",
        "depends_on": [
          85559
        ],
        "tasks": [
          {
            "name": "Move keywords",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "1219657962",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 11,
    "total_phases": 13,
    "mermaid": "graph LR\n    P99170[\"Step 1<br/>---<br/>EX: Keywords to-do\"]\n    P95748[\"Step 2<br/>---<br/>OT: Google – Apify\"]\n    P99170 --> P95748\n    P75094[\"Step 3<br/>---<br/>TR: Merge SERP\"]\n    P95748 --> P75094\n    P49018[\"Step 4<br/>[no enabled tasks]\"]\n    P75094 --> P49018\n    P55991[\"Step 5<br/>---<br/>TR: Structured Data Pre-categorization\"]\n    P49018 --> P55991\n    P72854[\"Step 6<br/>[no enabled tasks]\"]\n    P55991 --> P72854\n    P10364[\"Step 7<br/>---<br/>TR: Enhance data with Ad Networks\"]\n    P72854 --> P10364\n    P49644[\"Step 8<br/>---<br/>TR: Categorization\"]\n    P10364 --> P49644\n    P52299[\"Step 9<br/>---<br/>TR: Merge categories\"]\n    P49644 --> P52299\n    P62966[\"Step 10<br/>---<br/>TR: Add domain info\"]\n    P52299 --> P62966\n    P70655[\"Step 11<br/>---<br/>WR: Write categorized keyword analysis results\"]\n    P62966 --> P70655\n    P85559[\"Step 12<br/>---<br/>TR: Clean up keywords\"]\n    P70655 --> P85559\n    P22692[\"Step 13<br/>---<br/>WR: Move keywords\"]\n    P85559 --> P22692\n\n    style P99170 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P95748 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P75094 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P49018 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P55991 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P72854 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P10364 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P49644 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P52299 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P62966 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P70655 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P85559 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P22692 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-digital-sales-machine|1219671054": {
    "project_alias": "ir-l1-data-processes-digital-sales-machine",
    "config_id": "1219671054",
    "name": "Download domain data",
    "description": "Downloads data from Adform and WhoTracksMe. Only needs to be run once.",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:12:20+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 56167,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Adform domains",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1219669304",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "AWS WhotracksMe sites_trackers",
            "component_id": "keboola.ex-http",
            "component_short": "ex-http",
            "config_id": "1219669535",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 1,
    "mermaid": "graph LR\n    P56167[\"Step 1<br/>---<br/>EX: Adform domains<br/>EX: AWS WhotracksMe sites_trackers\"]\n\n    style P56167 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb"
  },
  "ir-l1-data-processes-digital-sales-machine|1221094109": {
    "project_alias": "ir-l1-data-processes-digital-sales-machine",
    "config_id": "1221094109",
    "name": "Update Adform only",
    "description": "Downloads data on Adform domains, updates the serp tables and writes the result to G Drive.",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:12:20+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 18730,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Adform domains",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1219669304",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 87476,
        "name": "Step 2",
        "depends_on": [
          18730
        ],
        "tasks": [
          {
            "name": "Add domain info to SERP merged",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1219674424",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 85549,
        "name": "Step 3",
        "depends_on": [
          87476
        ],
        "tasks": [
          {
            "name": "Write keyword analysis results to G Sheet",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "1219659705",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P18730[\"Step 1<br/>---<br/>EX: Adform domains\"]\n    P87476[\"Step 2<br/>---<br/>TR: Add domain info to SERP merged\"]\n    P18730 --> P87476\n    P85549[\"Step 3<br/>---<br/>WR: Write keyword analysis results to G Sheet\"]\n    P87476 --> P85549\n\n    style P18730 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P87476 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P85549 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-digital-sales-machine|1226598114": {
    "project_alias": "ir-l1-data-processes-digital-sales-machine",
    "config_id": "1226598114",
    "name": "PAYG Analysis",
    "description": "Run the analytical queries and write result to Google Sheet",
    "is_disabled": false,
    "version": 6,
    "last_modified": "2025-10-02T10:32:22+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 1105,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "PAYG analysis",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1226565121",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 62064,
        "name": "Step 2",
        "depends_on": [
          1105
        ],
        "tasks": [
          {
            "name": "PAYG-onboarding-analysis",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k3k3jdbzg86vcnz0pqcnv2x4",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 58439,
        "name": "Step 3",
        "depends_on": [
          62064
        ],
        "tasks": [
          {
            "name": "PAYG Maturity for GoodData",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k6afwr00ztp9015jzjtyyn04",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 71197,
        "name": "Step 4",
        "depends_on": [
          58439
        ],
        "tasks": [
          {
            "name": "Write PAYG Analysis",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "1226592631",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 4,
    "total_phases": 4,
    "mermaid": "graph LR\n    P1105[\"Step 1<br/>---<br/>TR: PAYG analysis\"]\n    P62064[\"Step 2<br/>---<br/>TR: PAYG-onboarding-analysis\"]\n    P1105 --> P62064\n    P58439[\"Step 3<br/>---<br/>TR: PAYG Maturity for GoodData *\"]\n    P62064 --> P58439\n    P71197[\"Step 4<br/>---<br/>WR: Write PAYG Analysis\"]\n    P58439 --> P71197\n\n    style P1105 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P62064 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P58439 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P71197 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-digital-sales-machine|15478954": {
    "project_alias": "ir-l1-data-processes-digital-sales-machine",
    "config_id": "15478954",
    "name": "> GET SERP",
    "description": "Created from > GET SERP version #2\n\nExtracts search engine result pages for keywords specified in https://docs.google.com/spreadsheets/d/13LjVwNEFOSILTpTcL3tyjbj7E08NfSjomL-gc-8adqw/edit?gid=0#gid=0 and populates the spreadsheet with results.",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-26T10:34:08+0100",
    "last_modified_by": "ales.roubicek@hckr.studio",
    "phases": [
      {
        "id": 99170,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keywords to-do",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "1218770349",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 95748,
        "name": "Step 2",
        "depends_on": [
          99170
        ],
        "tasks": [
          {
            "name": "Google – Apify",
            "component_id": "apify.apify",
            "component_short": "apify.apify",
            "config_id": "1219630268",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "OT"
          },
          {
            "name": "Bing with Input Mapping – tri_angle/bing-search-scraper",
            "component_id": "apify.apify",
            "component_short": "apify.apify",
            "config_id": "1218773744",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "OT"
          }
        ]
      },
      {
        "id": 75094,
        "name": "Step 3",
        "depends_on": [
          95748
        ],
        "tasks": [
          {
            "name": "Merge SERP",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1219643978",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 32274,
        "name": "Step 4",
        "depends_on": [
          75094
        ],
        "tasks": [
          {
            "name": "Add domain info to SERP merged",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1219674424",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 26197,
        "name": "Step 5",
        "depends_on": [
          32274
        ],
        "tasks": [
          {
            "name": "Write keyword analysis results to G Sheet",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "1219659705",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 87052,
        "name": "Step 6",
        "depends_on": [
          26197
        ],
        "tasks": [
          {
            "name": "Clean up keywords",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1219655292",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 51218,
        "name": "Step 7",
        "depends_on": [
          87052
        ],
        "tasks": [
          {
            "name": "Move keywords",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "1219657962",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 8,
    "total_phases": 7,
    "mermaid": "graph LR\n    P99170[\"Step 1<br/>---<br/>EX: Keywords to-do\"]\n    P95748[\"Step 2<br/>---<br/>OT: Google – Apify<br/>OT: Bing with Input Mapping – tri_angle/bing-search...\"]\n    P99170 --> P95748\n    P75094[\"Step 3<br/>---<br/>TR: Merge SERP\"]\n    P95748 --> P75094\n    P32274[\"Step 4<br/>---<br/>TR: Add domain info to SERP merged\"]\n    P75094 --> P32274\n    P26197[\"Step 5<br/>---<br/>WR: Write keyword analysis results to G Sheet\"]\n    P32274 --> P26197\n    P87052[\"Step 6<br/>---<br/>TR: Clean up keywords\"]\n    P26197 --> P87052\n    P51218[\"Step 7<br/>---<br/>WR: Move keywords\"]\n    P87052 --> P51218\n\n    style P99170 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P95748 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P75094 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P32274 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P26197 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P87052 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P51218 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-engg|01k1awhsxccqspzgs8v6y4s78e": {
    "project_alias": "ir-l1-data-processes-engg",
    "config_id": "01k1awhsxccqspzgs8v6y4s78e",
    "name": "CF data load",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-07-29T12:45:22+0200",
    "last_modified_by": "daniel.borner@keboola.com",
    "phases": [
      {
        "id": 36305,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Processor using",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k19hknph6yea5z631hvwg0mx",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P36305[\"Step 1<br/>---<br/>TR: Processor using\"]\n\n    style P36305 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l1-data-processes-marketing-cdp|1163513859": {
    "project_alias": "ir-l1-data-processes-marketing-cdp",
    "config_id": "1163513859",
    "name": "CDP flow",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-09-30T16:05:26+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 12776,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "GA4_data",
            "component_id": "keboola.ex-google-bigquery-v2",
            "component_short": "ex-google-bigquery-v2",
            "config_id": "1163087818",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Segmentation app",
            "component_id": "keboola.ex-db-snowflake",
            "component_short": "ex-db-snowflake",
            "config_id": "1168619884",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 18687,
        "name": "Step 2",
        "depends_on": [
          12776
        ],
        "tasks": [
          {
            "name": "CDP - Transformation",
            "component_id": "keboola.dbt-transformation-remote-snowflake",
            "component_short": "dbt-transformation-remote-snowflake",
            "config_id": "1174274951",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 87936,
        "name": "Step 3",
        "depends_on": [
          18687
        ],
        "tasks": [
          {
            "name": "CDP - Postgre - stage",
            "component_id": "keboola.wr-db-pgsql",
            "component_short": "wr-db-pgsql",
            "config_id": "1170712584",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "CDP - Postgre - prod",
            "component_id": "keboola.wr-db-pgsql",
            "component_short": "wr-db-pgsql",
            "config_id": "1175435435",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 3,
    "mermaid": "graph LR\n    P12776[\"Step 1<br/>---<br/>EX: GA4_data *<br/>EX: Segmentation app\"]\n    P18687[\"Step 2<br/>[no enabled tasks]\"]\n    P12776 --> P18687\n    P87936[\"Step 3<br/>[no enabled tasks]\"]\n    P18687 --> P87936\n\n    style P12776 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P18687 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P87936 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l1-data-processes-marketing-cdp|1223403158": {
    "project_alias": "ir-l1-data-processes-marketing-cdp",
    "config_id": "1223403158",
    "name": "Stage",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-03-06T08:29:25+0100",
    "last_modified_by": "tomas.falt@marketing.bi",
    "phases": [
      {
        "id": 4315,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Consumption data",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "16013809",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 78653,
        "name": "Step 2",
        "depends_on": [
          4315
        ],
        "tasks": [
          {
            "name": "CDP - Transformation - stage",
            "component_id": "keboola.dbt-transformation-remote-snowflake",
            "component_short": "dbt-transformation-remote-snowflake",
            "config_id": "1223408824",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 82057,
        "name": "Step 3",
        "depends_on": [
          78653
        ],
        "tasks": [
          {
            "name": "CDP - Postgre - stage",
            "component_id": "keboola.wr-db-pgsql",
            "component_short": "wr-db-pgsql",
            "config_id": "1170712584",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P4315[\"Step 1<br/>---<br/>WR: Consumption data\"]\n    P78653[\"Step 2<br/>---<br/>TR: CDP - Transformation - stage\"]\n    P4315 --> P78653\n    P82057[\"Step 3<br/>---<br/>WR: CDP - Postgre - stage\"]\n    P78653 --> P82057\n\n    style P4315 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P78653 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P82057 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-marketing-cdp|88520693": {
    "project_alias": "ir-l1-data-processes-marketing-cdp",
    "config_id": "88520693",
    "name": "CDP segmentation",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-06-24T13:56:56+0200",
    "last_modified_by": "tomas.falt@marketing.bi",
    "phases": [
      {
        "id": 38132,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "CDP - Transformation - stage - segmentation",
            "component_id": "keboola.dbt-transformation-remote-snowflake",
            "component_short": "dbt-transformation-remote-snowflake",
            "config_id": "23623881",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 87395,
        "name": "Step 2",
        "depends_on": [
          38132
        ],
        "tasks": [
          {
            "name": "CDP - Postgre - stage - segmentation",
            "component_id": "keboola.wr-db-pgsql",
            "component_short": "wr-db-pgsql",
            "config_id": "01jygwrc8eg39d9et4a3nh5bna",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P38132[\"Step 1<br/>---<br/>TR: CDP - Transformation - stage - segmentation\"]\n    P87395[\"Step 2<br/>---<br/>WR: CDP - Postgre - stage - segmentation\"]\n    P38132 --> P87395\n\n    style P38132 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P87395 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-ml-automations|01k4vptscbsbh9vv9t10vm181g": {
    "project_alias": "ir-l1-data-processes-ml-automations",
    "config_id": "01k4vptscbsbh9vv9t10vm181g",
    "name": "Predict probability SFDC",
    "description": "",
    "is_disabled": false,
    "version": 5,
    "last_modified": "2025-09-22T12:09:17+0200",
    "last_modified_by": "adela.kostelecka@keboola.com",
    "phases": [
      {
        "id": 19167,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Combine SFDC data",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k4vnatpx13p4kr58yheregmg",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 38767,
        "name": "Step 2",
        "depends_on": [
          19167
        ],
        "tasks": [
          {
            "name": "Binary classification: predict probability",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "01k4w2wgb7fdn0ab33a915d6s2",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "One-class classification: predict SVM score",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "01k4w4khf7ewyszqpg67jmnq6x",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 14828,
        "name": "Step 3",
        "depends_on": [
          38767
        ],
        "tasks": [
          {
            "name": "Opportunity won probabilities",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "01k4vqx8d4atchfft2r9jx0jpr",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 4,
    "total_phases": 3,
    "mermaid": "graph LR\n    P19167[\"Step 1<br/>---<br/>TR: Combine SFDC data\"]\n    P38767[\"Step 2<br/>---<br/>TR: Binary classification: predict probability<br/>TR: One-class classification: predict SVM score\"]\n    P19167 --> P38767\n    P14828[\"Step 3<br/>---<br/>WR: Opportunity won probabilities\"]\n    P38767 --> P14828\n\n    style P19167 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P38767 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P14828 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-product|01k2zem54fvb732csr5ny7ws5d": {
    "project_alias": "ir-l1-data-processes-product",
    "config_id": "01k2zem54fvb732csr5ny7ws5d",
    "name": "Telemetry healtcheck",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-08-18T22:42:06+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 76645,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Unknown transformations",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k2yja097aqd973b3dq0e2ypd",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 97021,
        "name": "Step 2",
        "depends_on": [
          76645
        ],
        "tasks": [
          {
            "name": "Unknown transformations",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01k2ykn3xw8dwh9rmmb7g7n44j",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P76645[\"Step 1<br/>---<br/>TR: Unknown transformations\"]\n    P97021[\"Step 2<br/>---<br/>AP: Unknown transformations\"]\n    P76645 --> P97021\n\n    style P76645 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P97021 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l1-data-processes-product|1220956581": {
    "project_alias": "ir-l1-data-processes-product",
    "config_id": "1220956581",
    "name": "Product segrmentatio",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:13:19+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 9374,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "user_segmentation_users",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1220522492",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 65891,
        "name": "Step 2",
        "depends_on": [
          9374
        ],
        "tasks": [
          {
            "name": "user_segmentation_segments",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1221048707",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P9374[\"Step 1<br/>---<br/>TR: user_segmentation_users\"]\n    P65891[\"Step 2<br/>---<br/>TR: user_segmentation_segments\"]\n    P9374 --> P65891\n\n    style P9374 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P65891 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l1-data-processes-product|1226193709": {
    "project_alias": "ir-l1-data-processes-product",
    "config_id": "1226193709",
    "name": "Product ticket analyse",
    "description": "",
    "is_disabled": true,
    "version": 26,
    "last_modified": "2026-02-23T10:34:30+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 60335,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Jira Support Content Analyst",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1223320196",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Slack Content Analyst",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "14952978",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Confluence feedbacks",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "15642157",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Transcripts Git",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "17490890",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 13175,
        "name": "Step 2",
        "depends_on": [
          60335
        ],
        "tasks": [
          {
            "name": "Data Truncate",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "14978637",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 69675,
        "name": "Step 3",
        "depends_on": [
          13175
        ],
        "tasks": [
          {
            "name": "Product content AI - multiple feedback - SLACK",
            "component_id": "kds-team.app-generative-ai",
            "component_short": "app-generative-ai",
            "config_id": "15026023",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Product content AI",
            "component_id": "kds-team.app-generative-ai",
            "component_short": "app-generative-ai",
            "config_id": "1223319568",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Product content AI - multiple feedback - CONFLU",
            "component_id": "kds-team.app-generative-ai",
            "component_short": "app-generative-ai",
            "config_id": "15830440",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Product content AI - multiple feedback - Transcripts",
            "component_id": "kds-team.app-generative-ai",
            "component_short": "app-generative-ai",
            "config_id": "17492416",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 36350,
        "name": "Step 4",
        "depends_on": [
          69675
        ],
        "tasks": [
          {
            "name": "Content Analyst - combo",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1226193151",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 73980,
        "name": "Step 5",
        "depends_on": [
          36350
        ],
        "tasks": [
          {
            "name": "Product Content - embedding",
            "component_id": "kds-team.embed-lancedb",
            "component_short": "embed-lancedb",
            "config_id": "1226197468",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "OT"
          },
          {
            "name": "Product Content - embedding-v2",
            "component_id": "keboola.app-embeddings-v2",
            "component_short": "app-embeddings-v2",
            "config_id": "16430169",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 71678,
        "name": "Step 6",
        "depends_on": [
          73980
        ],
        "tasks": [
          {
            "name": "Content Analyst - time filtering",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "14843608",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 10439,
        "name": "Step 7",
        "depends_on": [
          71678
        ],
        "tasks": [
          {
            "name": "Clusters pre-calculation",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "15485547",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 13,
    "total_phases": 7,
    "mermaid": "graph LR\n    P60335[\"Step 1<br/>---<br/>TR: Jira Support Content Analyst<br/>TR: Slack Content Analyst<br/>AP: Confluence feedbacks<br/>AP: Transcripts Git\"]\n    P13175[\"Step 2<br/>---<br/>TR: Data Truncate *\"]\n    P60335 --> P13175\n    P69675[\"Step 3<br/>---<br/>AP: Product content AI - multiple feedback - SLACK<br/>AP: Product content AI<br/>AP: Product content AI - multiple feedback - CONFLU<br/>AP: Product content AI - multiple feedback - Transc...\"]\n    P13175 --> P69675\n    P36350[\"Step 4<br/>---<br/>TR: Content Analyst - combo\"]\n    P69675 --> P36350\n    P73980[\"Step 5<br/>---<br/>AP: Product Content - embedding-v2\"]\n    P36350 --> P73980\n    P71678[\"Step 6<br/>---<br/>TR: Content Analyst - time filtering\"]\n    P73980 --> P71678\n    P10439[\"Step 7<br/>---<br/>AP: Clusters pre-calculation\"]\n    P71678 --> P10439\n\n    style P60335 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P13175 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P69675 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P36350 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P73980 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P71678 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P10439 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l1-data-processes-product|15977286": {
    "project_alias": "ir-l1-data-processes-product",
    "config_id": "15977286",
    "name": "Scheduled AI flow builder data preparation",
    "description": "",
    "is_disabled": true,
    "version": 2,
    "last_modified": "2025-08-28T08:16:10+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [],
        "tasks": [
          {
            "name": "AI flow builder data preparation",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "15946929",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Scheduled configuration<br/>---<br/>TR: AI flow builder data preparation\"]\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l1-data-processes-product|17367986": {
    "project_alias": "ir-l1-data-processes-product",
    "config_id": "17367986",
    "name": "Salesforce Feedback",
    "description": "",
    "is_disabled": false,
    "version": 5,
    "last_modified": "2025-04-08T12:13:20+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 23990,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Smaller Chatter",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "17131172",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 31665,
        "name": "Step 2",
        "depends_on": [
          23990
        ],
        "tasks": [
          {
            "name": "Salesforce Feed AI",
            "component_id": "kds-team.app-generative-ai",
            "component_short": "app-generative-ai",
            "config_id": "17124901",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 77398,
        "name": "Step 3",
        "depends_on": [
          31665
        ],
        "tasks": [
          {
            "name": "Filter Chatter Transformation",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "17916461",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 30510,
        "name": "Step 4",
        "depends_on": [
          77398
        ],
        "tasks": [
          {
            "name": "Salesforce Feedback Summary AI",
            "component_id": "kds-team.app-generative-ai",
            "component_short": "app-generative-ai",
            "config_id": "18436472",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 53190,
        "name": "Step 5",
        "depends_on": [
          30510
        ],
        "tasks": [
          {
            "name": "Snapshot Summary",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "18436139",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 5,
    "mermaid": "graph LR\n    P23990[\"Step 1<br/>---<br/>TR: Smaller Chatter\"]\n    P31665[\"Step 2<br/>---<br/>AP: Salesforce Feed AI\"]\n    P23990 --> P31665\n    P77398[\"Step 3<br/>---<br/>TR: Filter Chatter Transformation\"]\n    P31665 --> P77398\n    P30510[\"Step 4<br/>---<br/>AP: Salesforce Feedback Summary AI\"]\n    P77398 --> P30510\n    P53190[\"Step 5<br/>---<br/>TR: Snapshot Summary\"]\n    P30510 --> P53190\n\n    style P23990 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P31665 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P77398 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P30510 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P53190 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l1-data-processes-product|17446232": {
    "project_alias": "ir-l1-data-processes-product",
    "config_id": "17446232",
    "name": "Scheduled Transcripts - GDrive2GitHub",
    "description": "",
    "is_disabled": true,
    "version": 5,
    "last_modified": "2026-02-23T10:33:48+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [],
        "tasks": [
          {
            "name": "Transcripts - GDrive2GitHub",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "17412995",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Transcripts - GDrive2GitHub-sales",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "17847579",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Transcripts - GDrive2GitHub-cloudtalk",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "18149182",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Scheduled configuration<br/>---<br/>AP: Transcripts - GDrive2GitHub<br/>AP: Transcripts - GDrive2GitHub-sales\"]\n\n    style P1 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l1-data-processes-sales|01jyp5zj7kn5mytgtae22edssh": {
    "project_alias": "ir-l1-data-processes-sales",
    "config_id": "01jyp5zj7kn5mytgtae22edssh",
    "name": "PAYG Sheet update",
    "description": "",
    "is_disabled": false,
    "version": 5,
    "last_modified": "2025-08-19T09:55:23+0200",
    "last_modified_by": "max.ottomansky@keboola.com",
    "phases": [
      {
        "id": 92065,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "[PAYG]00_GetSheet",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01jynx83v3nt1rkvj6hexvhc2p",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 36091,
        "name": "Step 2",
        "depends_on": [
          92065
        ],
        "tasks": [
          {
            "name": "PAYG Accounts Enhanced - Final Working Version",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01jyp5an84acv6jpwj94094dmw",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 96175,
        "name": "Step 3",
        "depends_on": [
          36091
        ],
        "tasks": [
          {
            "name": "[PAYG]UpdateSheet",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "01jyp5vtzm3m75d31e2sndwcnd",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P92065[\"Step 1<br/>---<br/>EX: [PAYG]00_GetSheet\"]\n    P36091[\"Step 2<br/>---<br/>TR: PAYG Accounts Enhanced - Final Working Version\"]\n    P92065 --> P36091\n    P96175[\"Step 3<br/>---<br/>WR: [PAYG]UpdateSheet\"]\n    P36091 --> P96175\n\n    style P92065 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P36091 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P96175 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-support|33892277": {
    "project_alias": "ir-l1-data-processes-support",
    "config_id": "33892277",
    "name": "Jira Context enrichment",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-06-13T21:28:09+0200",
    "last_modified_by": "jakub.sochan@keboola.com",
    "phases": [
      {
        "id": 5779,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Enriching Context field in support tickets",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01jx04qzdddeg5gey8ppe7sqqr",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 93522,
        "name": "Step 2",
        "depends_on": [
          5779
        ],
        "tasks": [
          {
            "name": "Jira Context enrichment",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "01jx05015xnyqqj8gxjwyvyek4",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P5779[\"Step 1<br/>---<br/>TR: Enriching Context field in support tickets\"]\n    P93522[\"Step 2<br/>---<br/>WR: Jira Context enrichment\"]\n    P5779 --> P93522\n\n    style P5779 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P93522 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-data-processes-support|74593800": {
    "project_alias": "ir-l1-data-processes-support",
    "config_id": "74593800",
    "name": "Jira organizations update",
    "description": "",
    "is_disabled": false,
    "version": 29,
    "last_modified": "2026-01-03T21:27:03+0100",
    "last_modified_by": "jakub.sochan@keboola.com",
    "phases": [
      {
        "id": 94698,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Create Jira organization",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "22809398",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Update Organization (SF) field values",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "01kdzcmeka8fzxb37y9jcqg5qw",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 83533,
        "name": "Step 2",
        "depends_on": [
          94698
        ],
        "tasks": [
          {
            "name": "Extract Jira organizations - refresh",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "22810317",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 69048,
        "name": "Step 3",
        "depends_on": [
          83533
        ],
        "tasks": [
          {
            "name": "Assign organizations to SUPPORT project",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "22810358",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Organization Details Enrichment",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "22981652",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 85814,
        "name": "Step 4",
        "depends_on": [
          69048
        ],
        "tasks": [
          {
            "name": "Set Jira organization details",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "22809456",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 6,
    "total_phases": 4,
    "mermaid": "graph LR\n    P94698[\"Step 1<br/>---<br/>WR: Create Jira organization<br/>WR: Update Organization [SF] field values\"]\n    P83533[\"Step 2<br/>---<br/>EX: Extract Jira organizations - refresh\"]\n    P94698 --> P83533\n    P69048[\"Step 3<br/>---<br/>WR: Assign organizations to SUPPORT project<br/>TR: Organization Details Enrichment\"]\n    P83533 --> P69048\n    P85814[\"Step 4<br/>---<br/>WR: Set Jira organization details\"]\n    P69048 --> P85814\n\n    style P94698 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P83533 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P69048 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P85814 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l1-gooddata-user-management|1179489847": {
    "project_alias": "ir-l1-gooddata-user-management",
    "config_id": "1179489847",
    "name": "GD User Management",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-02-13T00:11:54+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 5920,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "GD Cloud",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "1179336764",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 57911,
        "name": "Step 2",
        "depends_on": [
          5920
        ],
        "tasks": [
          {
            "name": "GD Users",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1179325643",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 33580,
        "name": "Step 3",
        "depends_on": [
          57911
        ],
        "tasks": [
          {
            "name": "Create GD Users",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1179366356",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Update GD Users",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1180449113",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Delete GD Users",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1180451068",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 3,
    "mermaid": "graph LR\n    P5920[\"Step 1<br/>---<br/>EX: GD Cloud\"]\n    P57911[\"Step 2<br/>---<br/>TR: GD Users\"]\n    P5920 --> P57911\n    P33580[\"Step 3<br/>---<br/>WR: Create GD Users<br/>WR: Update GD Users<br/>WR: Delete GD Users\"]\n    P57911 --> P33580\n\n    style P5920 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P57911 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P33580 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-3rd-party-data-share|842058249": {
    "project_alias": "ir-l2-3rd-party-data-share",
    "config_id": "842058249",
    "name": "Vendor stats",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:13:49+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Apps Statistics",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836689573",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 1,
        "name": "Load",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "App Stats - Revolt.bi",
            "component_id": "keboola.wr-storage",
            "component_short": "wr-storage",
            "config_id": "509007602",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "App Stats - Geneea",
            "component_id": "keboola.wr-storage",
            "component_short": "wr-storage",
            "config_id": "1190785775",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 2,
    "mermaid": "graph LR\n    P0[\"Transformation<br/>---<br/>TR: Apps Statistics\"]\n    P1[\"Load<br/>---<br/>WR: App Stats - Revolt.bi *<br/>WR: App Stats - Geneea *\"]\n    P0 --> P1\n\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-3rd-party-data-share|842594112": {
    "project_alias": "ir-l2-3rd-party-data-share",
    "config_id": "842594112",
    "name": "Crossbeam",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:13:49+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 85569,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Data for Partners (Crossbeam)",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "842557615",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 81590,
        "name": "Load",
        "depends_on": [
          85569
        ],
        "tasks": [
          {
            "name": "Crossbeam",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "842590745",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P85569[\"Transformation<br/>---<br/>TR: Data for Partners [Crossbeam]\"]\n    P81590[\"Load<br/>---<br/>WR: Crossbeam\"]\n    P85569 --> P81590\n\n    style P85569 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P81590 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-ai-exploration|01ka171t5b4y32zdhgkbbhct49": {
    "project_alias": "ir-l2-ai-exploration",
    "config_id": "01ka171t5b4y32zdhgkbbhct49",
    "name": "New Flow",
    "description": "",
    "is_disabled": false,
    "version": 1,
    "last_modified": "2025-11-14T13:57:09+0100",
    "last_modified_by": "vaclav.nosek@keboola.com",
    "phases": [],
    "total_tasks": 0,
    "total_phases": 0,
    "mermaid": null
  },
  "ir-l2-data-processes-ux-design|1136251524": {
    "project_alias": "ir-l2-data-processes-ux-design",
    "config_id": "1136251524",
    "name": "mm",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:14:01+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [],
    "total_tasks": 0,
    "total_phases": 0,
    "mermaid": null
  },
  "ir-l2-gd-telemetry-output|842058263": {
    "project_alias": "ir-l2-gd-telemetry-output",
    "config_id": "842058263",
    "name": "Telemetry Update",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:14:08+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Telemetry Data Preparation",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836690557",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Load",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Keboola Connection Telemetry v2",
            "component_id": "keboola.gooddata-writer",
            "component_short": "gooddata-writer",
            "config_id": "687128262",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P1[\"Transformation<br/>---<br/>TR: Telemetry Data Preparation\"]\n    P2[\"Load<br/>---<br/>WR: Keboola Connection Telemetry v2\"]\n    P1 --> P2\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-internal-bi-output|1207892633": {
    "project_alias": "ir-l2-internal-bi-output",
    "config_id": "1207892633",
    "name": "Scheduled GoodData - Marketing - Web Analytics",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-04-30T23:00:15+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [],
        "tasks": [
          {
            "name": "GoodData - Marketing - Web Analytics",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1190841499",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P1[\"Scheduled configuration<br/>---<br/>WR: GoodData - Marketing - Web Analytics\"]\n\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-internal-bi-output|16337725": {
    "project_alias": "ir-l2-internal-bi-output",
    "config_id": "16337725",
    "name": "GoodData Loads",
    "description": "This is uploading data to GoodData IR database",
    "is_disabled": false,
    "version": 7,
    "last_modified": "2025-10-16T09:47:20+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 90888,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "GoodData - Product [PROD]",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1185837144",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "GoodData - Finance Data [PROD]",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1184197088",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "GoodData - Support Data [PROD]",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1185769150",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "GoodData - Keboola Internal [PROD]",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1180816604",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "GoodData - Academy [PROD]",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1191608596",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 30126,
        "name": "Step 2",
        "depends_on": [
          90888
        ],
        "tasks": [
          {
            "name": "Cache Refresher",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836714590",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 23782,
        "name": "Step 3",
        "depends_on": [
          30126
        ],
        "tasks": [
          {
            "name": "Clear Cache GoodData",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1188517191",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 7,
    "total_phases": 3,
    "mermaid": "graph LR\n    P90888[\"Step 1<br/>---<br/>WR: GoodData - Product [PROD] *<br/>WR: GoodData - Finance Data [PROD] *<br/>WR: GoodData - Support Data [PROD] *<br/>WR: GoodData - Keboola Internal [PROD] *<br/>WR: GoodData - Academy [PROD] *\"]\n    P30126[\"Step 2<br/>---<br/>TR: Cache Refresher\"]\n    P90888 --> P30126\n    P23782[\"Step 3<br/>---<br/>WR: Clear Cache GoodData\"]\n    P30126 --> P23782\n\n    style P90888 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P30126 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P23782 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-internal-bi-output|842037499": {
    "project_alias": "ir-l2-internal-bi-output",
    "config_id": "842037499",
    "name": "Pull & Push",
    "description": "",
    "is_disabled": false,
    "version": 5,
    "last_modified": "2025-08-19T11:34:30+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Pull",
        "depends_on": [],
        "tasks": [
          {
            "name": "Sales & Marketing pull",
            "component_id": "kds-team.app-orchestration-trigger-queue-v2",
            "component_short": "app-orchestration-trigger-queue-v2",
            "config_id": "841595343",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "FL"
          },
          {
            "name": "Operations pull",
            "component_id": "kds-team.app-orchestration-trigger-queue-v2",
            "component_short": "app-orchestration-trigger-queue-v2",
            "config_id": "841592146",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "FL"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 1,
    "mermaid": "graph LR\n    P0[\"Pull<br/>---<br/>FL: Sales and Marketing pull *<br/>FL: Operations pull *\"]\n\n    style P0 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-internal-bi-output|842037511": {
    "project_alias": "ir-l2-internal-bi-output",
    "config_id": "842037511",
    "name": "Pull & Push - Finance",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-03-10T20:54:07+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction and Transformations",
        "depends_on": [],
        "tasks": [
          {
            "name": "Finance pull",
            "component_id": "kds-team.app-orchestration-trigger-queue-v2",
            "component_short": "app-orchestration-trigger-queue-v2",
            "config_id": "841593129",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "FL"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P0[\"Extraction and Transformations<br/>---<br/>FL: Finance pull *\"]\n\n    style P0 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-internal-bi-output|842037522": {
    "project_alias": "ir-l2-internal-bi-output",
    "config_id": "842037522",
    "name": "Pull & Push - Support",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-03-10T20:53:44+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "L0 Orchestration",
        "depends_on": [],
        "tasks": [
          {
            "name": "Support pull",
            "component_id": "kds-team.app-orchestration-trigger-queue-v2",
            "component_short": "app-orchestration-trigger-queue-v2",
            "config_id": "841595901",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "FL"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P0[\"L0 Orchestration<br/>---<br/>FL: Support pull\"]\n\n    style P0 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-internal-bi-output|842037534": {
    "project_alias": "ir-l2-internal-bi-output",
    "config_id": "842037534",
    "name": "Pull & Push - Academy",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-03-10T20:53:53+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "L0 Orchestration + Orchestrations detail",
        "depends_on": [],
        "tasks": [
          {
            "name": "Academy pull",
            "component_id": "kds-team.app-orchestration-trigger-queue-v2",
            "component_short": "app-orchestration-trigger-queue-v2",
            "config_id": "841591181",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "FL"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P0[\"L0 Orchestration + Orchestrations detail<br/>---<br/>FL: Academy pull\"]\n\n    style P0 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-internal-bi-output|842037551": {
    "project_alias": "ir-l2-internal-bi-output",
    "config_id": "842037551",
    "name": "Pull & Push - Product",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-03-10T20:54:01+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "L0 Orchestration",
        "depends_on": [],
        "tasks": [
          {
            "name": "Product pull",
            "component_id": "kds-team.app-orchestration-trigger-queue-v2",
            "component_short": "app-orchestration-trigger-queue-v2",
            "config_id": "841594709",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "FL"
          }
        ]
      }
    ],
    "total_tasks": 1,
    "total_phases": 1,
    "mermaid": "graph LR\n    P0[\"L0 Orchestration<br/>---<br/>FL: Product pull\"]\n\n    style P0 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|01jznkwz7mjf1wrx0bgn6d4ftg": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "01jznkwz7mjf1wrx0bgn6d4ftg",
    "name": "Account Scoring",
    "description": "",
    "is_disabled": true,
    "version": 4,
    "last_modified": "2025-07-24T14:45:58+0200",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 17169,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce - Account ICP Score",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1192785095",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 26833,
        "name": "Step 2",
        "depends_on": [
          17169
        ],
        "tasks": [
          {
            "name": "Accounts ICP scoring Update",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01jznkzy3p889k2r1a7f0c36mn",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P17169[\"Step 1<br/>---<br/>TR: Salesforce - Account ICP Score\"]\n    P26833[\"Step 2<br/>---<br/>WR: Accounts ICP scoring Update\"]\n    P17169 --> P26833\n\n    style P17169 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P26833 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|01k0xywn2vpcb6mvxy9byrfej1": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "01k0xywn2vpcb6mvxy9byrfej1",
    "name": "Account Intent & Scoring Pipeline",
    "description": "Consolidated flow combining account intent processing, new account creation, enrichment, and scoring. This flow processes intent data from external sources, creates new accounts when needed, enriches existing accounts, performs scoring, and updates Salesforce with the results.\n\n## Phase 1: Initial Intent Data Processing\nProcesses raw intent data and prepares it for account creation and matching.\n\n## Phase 2: New Account Creation & Google Sheets  \nCreates new accounts from unmatched intent data and writes results to Google Sheets for reporting.\n\n## Phase 3: Intent Data Gathering for Existing Accounts\nGathers and processes intent data for accounts that already exist in Salesforce.\n\n## Phase 4: Account Enrichment\nEnriches account records with additional data using Python transformations and data cleaning.\n\n## Phase 5: Account Scoring\nCalculates account scores based on intent signals and other attributes.\n\n## Phase 6: Final Salesforce Updates\nUpdates Salesforce with enriched account data, intent scores, and new account information.",
    "is_disabled": false,
    "version": 22,
    "last_modified": "2026-01-06T22:27:53+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": "phase1",
        "name": "Phase 1: Initial Intent Data Processing",
        "depends_on": [],
        "tasks": [
          {
            "name": "Account intent and engagement enrichment",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "21005191",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 28189,
        "name": "Phase 2: Data Division",
        "depends_on": [
          "phase1"
        ],
        "tasks": [
          {
            "name": "Divide Accounts with intent in SF vs not in SF",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "21429976",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": "phase2",
        "name": "Phase 3: New Account Creation & Google Sheets Reporting",
        "depends_on": [
          28189
        ],
        "tasks": [
          {
            "name": "Lead Intent",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "21507123",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "New Accounts with Intent",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01jz5hfpymt05k7c47bm418b2z",
            "enabled": false,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": "phase3",
        "name": "Phase 4: Intent Data Gathering for Existing Accounts",
        "depends_on": [
          "phase2"
        ],
        "tasks": [
          {
            "name": "Salesforce - Accounts Buyer Intent",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "21959068",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 65865,
        "name": "Phase 5: Intent Data Salesforce Update",
        "depends_on": [
          "phase3"
        ],
        "tasks": [
          {
            "name": "Accounts Buyer Intent",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "23046719",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": "phase4",
        "name": "Phase 6: Account Enrichment",
        "depends_on": [
          65865
        ],
        "tasks": [
          {
            "name": "Account Lusha Enrichment",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "01jynvta3vttffv6basbbqhzc5",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 77645,
        "name": "Step 7",
        "depends_on": [
          "phase4"
        ],
        "tasks": [
          {
            "name": "Industry standardization",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "01k33wptqqm4ytzem4d2ccyh6d",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": "phase5",
        "name": "Phase 7: Data Cleaning & Preparation - Account Scoring",
        "depends_on": [
          77645
        ],
        "tasks": [
          {
            "name": "Salesforce - Account ICP Score",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1192785095",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Lusha Enriched - Column names update",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01jz318vkzhhsds4m4h430gqrp",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": "phase6",
        "name": "Phase 8: Final Salesforce Updates",
        "depends_on": [
          "phase5"
        ],
        "tasks": [
          {
            "name": "Accounts ICP scoring Update",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01jznkzy3p889k2r1a7f0c36mn",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "Accounts Enrichment",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01jz30yar7t3btdtyzh16ntted",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 11,
    "total_phases": 9,
    "mermaid": "graph LR\n    Pphase1[\"Phase 1: Initial Intent Data Processing<br/>---<br/>TR: Account intent and engagement enrichment\"]\n    P28189[\"Phase 2: Data Division<br/>---<br/>TR: Divide Accounts with intent in SF vs not in SF\"]\n    Pphase1 --> P28189\n    Pphase2[\"Phase 3: New Account Creation and Google Sheets...<br/>---<br/>WR: Lead Intent\"]\n    P28189 --> Pphase2\n    Pphase3[\"Phase 4: Intent Data Gathering for Existing Acc...<br/>---<br/>TR: Salesforce - Accounts Buyer Intent\"]\n    Pphase2 --> Pphase3\n    P65865[\"Phase 5: Intent Data Salesforce Update<br/>---<br/>WR: Accounts Buyer Intent *\"]\n    Pphase3 --> P65865\n    Pphase4[\"Phase 6: Account Enrichment<br/>---<br/>TR: Account Lusha Enrichment\"]\n    P65865 --> Pphase4\n    P77645[\"Step 7<br/>---<br/>TR: Industry standardization\"]\n    Pphase4 --> P77645\n    Pphase5[\"Phase 7: Data Cleaning and Preparation - Accoun...<br/>---<br/>TR: Salesforce - Account ICP Score<br/>TR: Lusha Enriched - Column names update\"]\n    P77645 --> Pphase5\n    Pphase6[\"Phase 8: Final Salesforce Updates<br/>---<br/>WR: Accounts ICP scoring Update *<br/>WR: Accounts Enrichment *\"]\n    Pphase5 --> Pphase6\n\n    style Pphase1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P28189 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style Pphase2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style Pphase3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P65865 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style Pphase4 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P77645 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style Pphase5 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style Pphase6 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|01k0ya3gnmhy5tbwgmfdysh3rb": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "01k0ya3gnmhy5tbwgmfdysh3rb",
    "name": "Leads & Contacts Maintenance & Updates",
    "description": "Periodic maintenance pipeline for existing leads and contacts, focusing on data quality improvements, re-enrichment, campaign updates, and periodic re-scoring. This flow handles ongoing maintenance tasks that don't require immediate processing.\n\n## Phase 1: Data Refresh & Extraction\nRefreshes and extracts existing lead and contact data from multiple Salesforce endpoints.\n\n## Phase 2: Maintenance Processing\nPerforms Python-based maintenance processing and data quality checks.\n\n## Phase 3: Data Quality & Re-enrichment\nUpdates existing records with improved data quality and additional enrichment.\n\n## Phase 4: Campaign & Status Maintenance\nHandles campaign member updates and status maintenance tasks.\n\n## Phase 5: Periodic Re-scoring\nPerforms periodic re-scoring based on updated data and market changes.\n\n## Phase 6: Final Updates & Cleanup\nCompletes final updates and cleanup operations in Salesforce.",
    "is_disabled": true,
    "version": 25,
    "last_modified": "2025-12-19T02:16:48+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": "1",
        "name": "Phase 1: Data Refresh & Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          },
          {
            "name": "Lead Score Mapping & Common Email Domains",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "607205043",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": "2",
        "name": "Phase 2: Maintenance Processing",
        "depends_on": [
          "1"
        ],
        "tasks": [
          {
            "name": "Leads Validation",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "1218831026",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          },
          {
            "name": "Lusha Enrichment MQL Leads",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "01k0yepfzhsehzybsaa7dzrwb7",
            "enabled": false,
            "continue_on_failure": true,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 28898,
        "name": "Step 3",
        "depends_on": [
          "2"
        ],
        "tasks": [
          {
            "name": "Enriching MQL Leads with Lusha results",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k16qze1xtga4b0v3czmknrr2",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Lead Enrichment Updates with Account data",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k10eyfm9wp1geqrw1zqrhj8j",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": "4",
        "name": "Phase 4: Campaign & Status Maintenance",
        "depends_on": [
          28898
        ],
        "tasks": [
          {
            "name": "MQL Lead Enrichment",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "23642814",
            "enabled": false,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 29266,
        "name": "Step 5",
        "depends_on": [
          "4"
        ],
        "tasks": [
          {
            "name": "Lead Industry and Employees update - Account data",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01k10ph4b01mttsf8191aqb982",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 18871,
        "name": "Step 6",
        "depends_on": [
          29266
        ],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 55681,
        "name": "Step 7",
        "depends_on": [
          18871
        ],
        "tasks": [
          {
            "name": "Salesforce - Lead/Contact Score",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838067296",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 11479,
        "name": "Step 8",
        "depends_on": [
          55681
        ],
        "tasks": [
          {
            "name": "Lead/Contact Score UPDATE",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724025007",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 7,
    "total_phases": 8,
    "mermaid": "graph LR\n    P1[\"Phase 1: Data Refresh and Extraction<br/>---<br/>EX: Keboola - Specific Tables *<br/>EX: Lead Score Mapping and Common Email Domains\"]\n    P2[\"Phase 2: Maintenance Processing<br/>---<br/>TR: Leads Validation *\"]\n    P1 --> P2\n    P28898[\"Step 3<br/>---<br/>TR: Lead Enrichment Updates with Account data\"]\n    P2 --> P28898\n    P4[\"Phase 4: Campaign and Status Maintenance<br/>[no enabled tasks]\"]\n    P28898 --> P4\n    P29266[\"Step 5<br/>---<br/>WR: Lead Industry and Employees update - Account data\"]\n    P4 --> P29266\n    P18871[\"Step 6<br/>---<br/>EX: Keboola - Specific Tables\"]\n    P29266 --> P18871\n    P55681[\"Step 7<br/>---<br/>TR: Salesforce - Lead/Contact Score\"]\n    P18871 --> P55681\n    P11479[\"Step 8<br/>[no enabled tasks]\"]\n    P55681 --> P11479\n\n    style P1 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P2 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P28898 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P4 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P29266 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P18871 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P55681 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P11479 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|01k7h6xr98trwrz88dt035mn4v": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "01k7h6xr98trwrz88dt035mn4v",
    "name": "Ad Hoc Account/Opportunity Owner change",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-10-14T13:16:14+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 12333,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "SFDC Accounts changing Owner",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01k62b44432dx7j51mqzn1pq9w",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 70628,
        "name": "Step 2",
        "depends_on": [
          12333
        ],
        "tasks": [
          {
            "name": "Assign Account Owner",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01k62aypbs52579p5w7askcj9e",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P12333[\"Step 1<br/>---<br/>EX: SFDC Accounts changing Owner\"]\n    P70628[\"Step 2<br/>---<br/>WR: Assign Account Owner\"]\n    P12333 --> P70628\n\n    style P12333 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P70628 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|01k7s90mctx83j9qs9pkxtmd7p": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "01k7s90mctx83j9qs9pkxtmd7p",
    "name": "Linear Update",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-10-17T16:26:42+0200",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 26907,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Linear - Customers",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01k7rhdy7793pde88p0rn5t582",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2732,
        "name": "Step 2",
        "depends_on": [
          26907
        ],
        "tasks": [
          {
            "name": "Linear - Upsert Customers",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01k7pjbewfmf75zndryhb05v12",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P26907[\"Step 1<br/>---<br/>TR: Linear - Customers\"]\n    P2732[\"Step 2<br/>---<br/>AP: Linear - Upsert Customers\"]\n    P26907 --> P2732\n\n    style P26907 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2732 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|01kbfktg514abemggzzhv65q6e": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "01kbfktg514abemggzzhv65q6e",
    "name": "Salesforce enrichment & scoring",
    "description": "",
    "is_disabled": false,
    "version": 36,
    "last_modified": "2025-12-18T19:46:10+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 71091,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Data Preparation for Enrichment/Scoring",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbmcs9sx24vmcyz652sp1f30",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Lead Score Mapping / ICP Industry/Department",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01kbhk2yeqkpjw62mfvk3vnpqh",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 77480,
        "name": "Step 2",
        "depends_on": [
          71091
        ],
        "tasks": [
          {
            "name": "Lead/Contact - job_title Enrichment",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01kbmcx85a7kvt709gbrerm3dm",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Lead/Contact - company_size Enrichment",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01kbmmq704ayd5sar7jd5x84gk",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 66825,
        "name": "Step 3",
        "depends_on": [
          77480
        ],
        "tasks": [
          {
            "name": "Lead/Contact - enrichment Lusha",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbmf02jeab5gbwbtcc1ga4xd",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 33782,
        "name": "Step 4",
        "depends_on": [
          66825
        ],
        "tasks": [
          {
            "name": "Lusha Enrichment",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01kbmf3ast9snfjxbmkchkqbbt",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "Job Title Mapping - LLM",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01kbpnxahjccz0714bhtj0xk4t",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 70036,
        "name": "Step 5",
        "depends_on": [
          33782
        ],
        "tasks": [
          {
            "name": "AdHoc Cleanup of Standardized Job Titles + Adding Verticals",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbtzvf3yx3a2szt56bhkt4dt",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 60128,
        "name": "Step 6",
        "depends_on": [
          70036
        ],
        "tasks": [
          {
            "name": "Lusha Enrichment Seniority/Department/Job Title",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01kbhyghe4x9rpdcgh2ndck7hk",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 6705,
        "name": "Step 7",
        "depends_on": [
          60128
        ],
        "tasks": [
          {
            "name": "Lead/Contact Score Data Consolidation",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbq0as0yrqex3eegty0h9yfa",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 69776,
        "name": "Step 8",
        "depends_on": [
          6705
        ],
        "tasks": [
          {
            "name": "Lead/Contact Score & MQL",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbhqnggxxv3c9dqr6dhce4xe",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 19741,
        "name": "Step 9",
        "depends_on": [
          69776
        ],
        "tasks": [
          {
            "name": "Snapshot of contact table",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbt24eky8hemwnbfyt33zzxp",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 20305,
        "name": "Step 10",
        "depends_on": [
          19741
        ],
        "tasks": [
          {
            "name": "Lead/Contact Update Score",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01kc8xcf9ea6akntch7qe5tche",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 13,
    "total_phases": 10,
    "mermaid": "graph LR\n    P71091[\"Step 1<br/>---<br/>TR: Data Preparation for Enrichment/Scoring<br/>EX: Lead Score Mapping / ICP Industry/Department\"]\n    P77480[\"Step 2<br/>---<br/>AP: Lead/Contact - job_title Enrichment<br/>AP: Lead/Contact - company_size Enrichment\"]\n    P71091 --> P77480\n    P66825[\"Step 3<br/>---<br/>TR: Lead/Contact - enrichment Lusha\"]\n    P77480 --> P66825\n    P33782[\"Step 4<br/>---<br/>WR: Lusha Enrichment *<br/>AP: Job Title Mapping - LLM\"]\n    P66825 --> P33782\n    P70036[\"Step 5<br/>---<br/>TR: AdHoc Cleanup of Standardized Job Titles + Addi...\"]\n    P33782 --> P70036\n    P60128[\"Step 6<br/>---<br/>WR: Lusha Enrichment Seniority/Department/Job Title *\"]\n    P70036 --> P60128\n    P6705[\"Step 7<br/>---<br/>TR: Lead/Contact Score Data Consolidation\"]\n    P60128 --> P6705\n    P69776[\"Step 8<br/>---<br/>TR: Lead/Contact Score and MQL\"]\n    P6705 --> P69776\n    P19741[\"Step 9<br/>---<br/>TR: Snapshot of contact table\"]\n    P69776 --> P19741\n    P20305[\"Step 10<br/>---<br/>WR: Lead/Contact Update Score\"]\n    P19741 --> P20305\n\n    style P71091 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P77480 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P66825 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P33782 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P70036 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P60128 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P6705 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P69776 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P19741 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P20305 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|01kcr4febz3gdac1kb3ngs7w9m": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "01kcr4febz3gdac1kb3ngs7w9m",
    "name": "Salesforce Full Enrichment & Scoring",
    "description": "Created from Salesforce enrichment & scoring version #35",
    "is_disabled": false,
    "version": 18,
    "last_modified": "2026-01-07T11:14:02+0100",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 71091,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Data Preparation for Enrichment/Scoring",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbmcs9sx24vmcyz652sp1f30",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Lead Score Mapping / ICP Industry/Department",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01kbhk2yeqkpjw62mfvk3vnpqh",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 77480,
        "name": "Step 2",
        "depends_on": [
          71091
        ],
        "tasks": [
          {
            "name": "Account - Lusha enrichment",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01kck0q4n0nr6fp48c8jjgrj9e",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Lead/Contact - Lusha enrichment",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01kck0xba6twmt6bqb49af0v3q",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 66825,
        "name": "Step 3",
        "depends_on": [
          77480
        ],
        "tasks": [
          {
            "name": "Enrichment Lusha Data Processing",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kcma5hxcpxpjbvm3jjq3q3jt",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 33782,
        "name": "Step 4",
        "depends_on": [
          66825
        ],
        "tasks": [
          {
            "name": "Lusha Enrichment Primary",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01kcs08vwn7pkr2prtv0tfybg9",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "Job Title Mapping - Transformer Model",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01keahmekpn08rwh4kjqgggewz",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 70036,
        "name": "Step 5",
        "depends_on": [
          33782
        ],
        "tasks": [
          {
            "name": "AdHoc Cleanup of Standardized Job Titles + Adding Verticals",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbtzvf3yx3a2szt56bhkt4dt",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 60128,
        "name": "Step 6",
        "depends_on": [
          70036
        ],
        "tasks": [
          {
            "name": "Lusha Enrichment Seniority/Department/Job Title",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01kbhyghe4x9rpdcgh2ndck7hk",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "Industry Standardization",
            "component_id": "kds-team.app-custom-python",
            "component_short": "app-custom-python",
            "config_id": "01kcr5gcfvre0q47szq5k6rak4",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 6705,
        "name": "Step 7",
        "depends_on": [
          60128
        ],
        "tasks": [
          {
            "name": "Data Consolidation for Scoring",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kcrhh8sczp9wxyqt3t4dgv7r",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Lusha Enrichment Industries/Verticals",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01kcrtcx2qcs00ev08egapq38m",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 69776,
        "name": "Step 8",
        "depends_on": [
          6705
        ],
        "tasks": [
          {
            "name": "Lead/Contact Score & MQL",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbhqnggxxv3c9dqr6dhce4xe",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 5096,
        "name": "Step 9",
        "depends_on": [
          69776
        ],
        "tasks": [
          {
            "name": "Lead/Contact Update Score",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01kc8xcf9ea6akntch7qe5tche",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 19741,
        "name": "Step 10",
        "depends_on": [
          5096
        ],
        "tasks": [
          {
            "name": "Snapshot of contact table",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01kbt24eky8hemwnbfyt33zzxp",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 15,
    "total_phases": 10,
    "mermaid": "graph LR\n    P71091[\"Step 1<br/>---<br/>TR: Data Preparation for Enrichment/Scoring<br/>EX: Lead Score Mapping / ICP Industry/Department\"]\n    P77480[\"Step 2<br/>---<br/>AP: Account - Lusha enrichment<br/>AP: Lead/Contact - Lusha enrichment\"]\n    P71091 --> P77480\n    P66825[\"Step 3<br/>---<br/>TR: Enrichment Lusha Data Processing\"]\n    P77480 --> P66825\n    P33782[\"Step 4<br/>---<br/>WR: Lusha Enrichment Primary *<br/>AP: Job Title Mapping - Transformer Model\"]\n    P66825 --> P33782\n    P70036[\"Step 5<br/>---<br/>TR: AdHoc Cleanup of Standardized Job Titles + Addi...\"]\n    P33782 --> P70036\n    P60128[\"Step 6<br/>---<br/>WR: Lusha Enrichment Seniority/Department/Job Title *<br/>AP: Industry Standardization\"]\n    P70036 --> P60128\n    P6705[\"Step 7<br/>---<br/>TR: Data Consolidation for Scoring<br/>WR: Lusha Enrichment Industries/Verticals *\"]\n    P60128 --> P6705\n    P69776[\"Step 8<br/>---<br/>TR: Lead/Contact Score and MQL\"]\n    P6705 --> P69776\n    P5096[\"Step 9<br/>---<br/>WR: Lead/Contact Update Score\"]\n    P69776 --> P5096\n    P19741[\"Step 10<br/>---<br/>TR: Snapshot of contact table\"]\n    P5096 --> P19741\n\n    style P71091 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P77480 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P66825 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P33782 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P70036 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P60128 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P6705 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P69776 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P5096 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P19741 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|1072685841": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "1072685841",
    "name": "Marketplace",
    "description": "Flow creating CRM accounts and orders based on the original orders from Google Marketplace.",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-03-11T14:42:57+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 41320,
        "name": "Extraction 01 - Account",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 18741,
        "name": "Transfromation 01 - New Account",
        "depends_on": [
          41320
        ],
        "tasks": [
          {
            "name": "Marketplace - 01 Create Accounts",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1072663053",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 66930,
        "name": "Load 01 - New Account",
        "depends_on": [
          18741
        ],
        "tasks": [
          {
            "name": "Marketplace Accounts/Orders",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1077995194",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 65390,
        "name": "Extraction 02 - Account & Order",
        "depends_on": [
          66930
        ],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 98067,
        "name": "Transformation 02 - New Order",
        "depends_on": [
          65390
        ],
        "tasks": [
          {
            "name": "Marketplace - 02 Create Orders",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1072671561",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 12661,
        "name": "Load 02 - New Order",
        "depends_on": [
          98067
        ],
        "tasks": [
          {
            "name": "Marketplace Accounts/Orders",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1077995194",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 59038,
        "name": "Extraction 03 - Order & Item",
        "depends_on": [
          12661
        ],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 40800,
        "name": "Transformation 03 - New Item",
        "depends_on": [
          59038
        ],
        "tasks": [
          {
            "name": "Marketplace - 03 Create Order Items",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1072681226",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 64229,
        "name": "Load 03 - New Order Item",
        "depends_on": [
          40800
        ],
        "tasks": [
          {
            "name": "Marketplace Accounts/Orders",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1077995194",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 18037,
        "name": "Transformation 04 - Order Update",
        "depends_on": [
          64229
        ],
        "tasks": [
          {
            "name": "Marketplace - 04 Order Status Update",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1078945274",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 85780,
        "name": "Load 04 - Order Update",
        "depends_on": [
          18037
        ],
        "tasks": [
          {
            "name": "Marketplace Accounts/Orders",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1077995194",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 47444,
        "name": "Transformation 05 - KBC Organization CRM ID",
        "depends_on": [
          85780
        ],
        "tasks": [
          {
            "name": "Marketplace - 05 KBC Organization CRM ID Update",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1079014268",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 82415,
        "name": "Load 05 - Update CRM ID in KBC Organization",
        "depends_on": [
          47444
        ],
        "tasks": [
          {
            "name": "Marketplace CRM ID KBC Org",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "16424357",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 13,
    "total_phases": 13,
    "mermaid": "graph LR\n    P41320[\"Extraction 01 - Account<br/>---<br/>EX: Keboola - Specific Tables\"]\n    P18741[\"Transfromation 01 - New Account<br/>---<br/>TR: Marketplace - 01 Create Accounts\"]\n    P41320 --> P18741\n    P66930[\"Load 01 - New Account<br/>---<br/>WR: Marketplace Accounts/Orders\"]\n    P18741 --> P66930\n    P65390[\"Extraction 02 - Account and Order<br/>---<br/>EX: Keboola - Specific Tables\"]\n    P66930 --> P65390\n    P98067[\"Transformation 02 - New Order<br/>---<br/>TR: Marketplace - 02 Create Orders\"]\n    P65390 --> P98067\n    P12661[\"Load 02 - New Order<br/>---<br/>WR: Marketplace Accounts/Orders\"]\n    P98067 --> P12661\n    P59038[\"Extraction 03 - Order and Item<br/>---<br/>EX: Keboola - Specific Tables\"]\n    P12661 --> P59038\n    P40800[\"Transformation 03 - New Item<br/>---<br/>TR: Marketplace - 03 Create Order Items\"]\n    P59038 --> P40800\n    P64229[\"Load 03 - New Order Item<br/>---<br/>WR: Marketplace Accounts/Orders\"]\n    P40800 --> P64229\n    P18037[\"Transformation 04 - Order Update<br/>---<br/>TR: Marketplace - 04 Order Status Update\"]\n    P64229 --> P18037\n    P85780[\"Load 04 - Order Update<br/>---<br/>WR: Marketplace Accounts/Orders\"]\n    P18037 --> P85780\n    P47444[\"Transformation 05 - KBC Organization CRM ID<br/>---<br/>TR: Marketplace - 05 KBC Organization CRM ID Update\"]\n    P85780 --> P47444\n    P82415[\"Load 05 - Update CRM ID in KBC Organization<br/>---<br/>WR: Marketplace CRM ID KBC Org\"]\n    P47444 --> P82415\n\n    style P41320 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P18741 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P66930 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P65390 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P98067 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P12661 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P59038 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P40800 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P64229 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P18037 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P85780 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P47444 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P82415 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|1199988547": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "1199988547",
    "name": "Scheduled Update Organization ID ",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-02-13T00:18:54+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 73835,
        "name": "Upload into SFDC",
        "depends_on": [],
        "tasks": [
          {
            "name": "Telemetry IDs",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1199955426",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 16552,
        "name": "Download the new data from SFDC",
        "depends_on": [
          73835
        ],
        "tasks": [
          {
            "name": "Telemetry IDs Download",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "1200410499",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 65960,
        "name": "Step 3",
        "depends_on": [
          16552
        ],
        "tasks": [
          {
            "name": "Telemetry IDs Snapshot",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1200413801",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P73835[\"Upload into SFDC<br/>---<br/>WR: Telemetry IDs *\"]\n    P16552[\"Download the new data from SFDC<br/>---<br/>EX: Telemetry IDs Download\"]\n    P73835 --> P16552\n    P65960[\"Step 3<br/>---<br/>TR: Telemetry IDs Snapshot\"]\n    P16552 --> P65960\n\n    style P73835 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P16552 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P65960 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|1227046996": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "1227046996",
    "name": "Contact / Leads deletion",
    "description": "",
    "is_disabled": false,
    "version": 5,
    "last_modified": "2025-09-16T14:54:36+0200",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 9016,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Contacts / Leads to delete",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1227047474",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 62134,
        "name": "Step 2",
        "depends_on": [
          9016
        ],
        "tasks": [
          {
            "name": "Leads to delete",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "01k59927qsagns3d9kmgcyvnvj",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 98295,
        "name": "Step 3",
        "depends_on": [
          62134
        ],
        "tasks": [
          {
            "name": "Leads deletion",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "17433536",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P9016[\"Step 1<br/>---<br/>TR: Contacts / Leads to delete\"]\n    P62134[\"Step 2<br/>---<br/>EX: Leads to delete\"]\n    P9016 --> P62134\n    P98295[\"Step 3<br/>---<br/>WR: Leads deletion\"]\n    P62134 --> P98295\n\n    style P9016 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P62134 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P98295 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|16138657": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "16138657",
    "name": "UK list validation",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-03-07T15:05:39+0100",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 76080,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "UK list data source",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "16138673",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 22251,
        "name": "Step 2",
        "depends_on": [
          76080
        ],
        "tasks": [
          {
            "name": "UK validation",
            "component_id": "kds-team.app-generative-ai",
            "component_short": "app-generative-ai",
            "config_id": "16139592",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P76080[\"Step 1<br/>---<br/>EX: UK list data source\"]\n    P22251[\"Step 2<br/>---<br/>AP: UK validation\"]\n    P76080 --> P22251\n\n    style P76080 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P22251 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|19958975": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "19958975",
    "name": "Scheduled ga4-bigquery Date Formating",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-05-05T20:31:20+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [],
        "tasks": [
          {
            "name": "ga4-bigquery Date Formatting",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "19958366",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 75302,
        "name": "Step 2",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "project attributes - GA user",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "20301038",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P1[\"Scheduled configuration<br/>---<br/>TR: ga4-bigquery Date Formatting\"]\n    P75302[\"Step 2<br/>---<br/>TR: project attributes - GA user\"]\n    P1 --> P75302\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P75302 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|21429633": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "21429633",
    "name": "Account Intent - New Accounts Creation + Google Sheets",
    "description": "The `Lead Intent - New Accounts Creation + Google Sheets` flow is designed to transform data and write it to Google Sheets and Salesforce. It consists of four sequential phases, each building on the previous one to ensure data is processed and stored efficiently.\n\n## Scheduled configuration\nThis initial phase focuses on transforming data using Snowflake. The task in this phase executes a Snowflake transformation to prepare the data for subsequent processing.\n\n## Step 2\nIn this phase, another Snowflake transformation is executed. This task further processes the data, refining it for the next steps in the flow.\n\n## Step 3\nThis phase involves writing the transformed data to Google Sheets. The task here uses the Google Sheets writer to store the processed data, making it accessible for analysis or reporting.\n\n## Step 4\nThe final phase writes the data to Salesforce. The task utilizes the Salesforce writer to ensure the data is integrated into Salesforce, completing the flow's objective of data transformation and storage.",
    "is_disabled": true,
    "version": 16,
    "last_modified": "2025-07-24T14:45:43+0200",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [],
        "tasks": [
          {
            "name": "Account intent and engagement enrichment",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "21005191",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 3070,
        "name": "Step 2",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Divide Accounts with intent in SF vs not in SF",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "21429976",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 52089,
        "name": "Step 3",
        "depends_on": [
          3070
        ],
        "tasks": [
          {
            "name": "Lead Intent",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "21507123",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 35941,
        "name": "Step 4",
        "depends_on": [
          52089
        ],
        "tasks": [
          {
            "name": "New Accounts with Intent",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01jz5hfpymt05k7c47bm418b2z",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 96105,
        "name": "Step 5",
        "depends_on": [
          35941
        ],
        "tasks": [
          {
            "name": "Salesforce - Account ICP Score",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1192785095",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 70355,
        "name": "Step 6",
        "depends_on": [
          96105
        ],
        "tasks": [
          {
            "name": "Accounts ICP scoring Update",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01jznkzy3p889k2r1a7f0c36mn",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 6,
    "total_phases": 6,
    "mermaid": "graph LR\n    P1[\"Scheduled configuration<br/>---<br/>TR: Account intent and engagement enrichment\"]\n    P3070[\"Step 2<br/>---<br/>TR: Divide Accounts with intent in SF vs not in SF\"]\n    P1 --> P3070\n    P52089[\"Step 3<br/>---<br/>WR: Lead Intent\"]\n    P3070 --> P52089\n    P35941[\"Step 4<br/>---<br/>WR: New Accounts with Intent *\"]\n    P52089 --> P35941\n    P96105[\"Step 5<br/>---<br/>TR: Salesforce - Account ICP Score\"]\n    P35941 --> P96105\n    P70355[\"Step 6<br/>---<br/>WR: Accounts ICP scoring Update\"]\n    P96105 --> P70355\n\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P3070 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P52089 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P35941 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P96105 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P70355 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|48907227": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "48907227",
    "name": "Enrow Email and Phone Validation",
    "description": "",
    "is_disabled": false,
    "version": 5,
    "last_modified": "2025-06-12T11:12:49+0200",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 25046,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "SF Lead and Contact Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "1219868234",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 7878,
        "name": "Step 2",
        "depends_on": [
          25046
        ],
        "tasks": [
          {
            "name": "Leads to Verify",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "22977275",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2520,
        "name": "Step 3",
        "depends_on": [
          7878
        ],
        "tasks": [
          {
            "name": "python-transformation-v2 (22907930)",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "22907930",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 3,
    "mermaid": "graph LR\n    P25046[\"Step 1<br/>---<br/>EX: SF Lead and Contact Tables\"]\n    P7878[\"Step 2<br/>---<br/>TR: Leads to Verify\"]\n    P25046 --> P7878\n    P2520[\"Step 3<br/>[no enabled tasks]\"]\n    P7878 --> P2520\n\n    style P25046 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P7878 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2520 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|50213487": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "50213487",
    "name": "Account Buyers Intent + Enrichment",
    "description": "Updating `Intent Data` Account field with buyers intent obtained from external srouces.",
    "is_disabled": true,
    "version": 29,
    "last_modified": "2025-07-24T14:45:51+0200",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 31809,
        "name": "Gathers Intent Data for Accounts existing in SF",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce - Accounts Buyer Intent",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "21959068",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 65638,
        "name": "Writes Intent data",
        "depends_on": [
          31809
        ],
        "tasks": [
          {
            "name": "Accounts Buyer Intent",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "23046719",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 51880,
        "name": "Enriches Accounts that have Intent Data",
        "depends_on": [
          65638
        ],
        "tasks": [
          {
            "name": "Account Lusha Enrichment",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "01jynvta3vttffv6basbbqhzc5",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 26091,
        "name": "Cleans the output to update the data in SF",
        "depends_on": [
          51880
        ],
        "tasks": [
          {
            "name": "Lusha Enriched - Column names update",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "01jz318vkzhhsds4m4h430gqrp",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 92037,
        "name": "Updates enriched data in SF",
        "depends_on": [
          26091
        ],
        "tasks": [
          {
            "name": "Accounts Enrichment",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "01jz30yar7t3btdtyzh16ntted",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 5,
    "mermaid": "graph LR\n    P31809[\"Gathers Intent Data for Accounts existing in SF<br/>---<br/>TR: Salesforce - Accounts Buyer Intent\"]\n    P65638[\"Writes Intent data<br/>---<br/>WR: Accounts Buyer Intent\"]\n    P31809 --> P65638\n    P51880[\"Enriches Accounts that have Intent Data<br/>---<br/>TR: Account Lusha Enrichment\"]\n    P65638 --> P51880\n    P26091[\"Cleans the output to update the data in SF<br/>---<br/>TR: Lusha Enriched - Column names update\"]\n    P51880 --> P26091\n    P92037[\"Updates enriched data in SF<br/>---<br/>WR: Accounts Enrichment\"]\n    P26091 --> P92037\n\n    style P31809 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P65638 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P51880 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P26091 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P92037 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038316": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038316",
    "name": "Update Campaign Member Status",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-02-13T00:18:55+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Get only Campaign Members",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Create table with updated member statuses",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Salesforce - Campaign Member Status Update",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838087829",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Update Member Statuses in Salesforce",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Campaign Member UPDATE",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724028215",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P0[\"Get only Campaign Members<br/>---<br/>EX: Keboola - Specific Tables\"]\n    P1[\"Create table with updated member statuses<br/>---<br/>TR: Salesforce - Campaign Member Status Update\"]\n    P0 --> P1\n    P2[\"Update Member Statuses in Salesforce<br/>---<br/>WR: Campaign Member UPDATE\"]\n    P1 --> P2\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038323": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038323",
    "name": "Customer.io Update",
    "description": "",
    "is_disabled": false,
    "version": 8,
    "last_modified": "2026-01-08T16:03:30+0100",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 19202,
        "name": "Transformation 01 - Create customers",
        "depends_on": [],
        "tasks": [
          {
            "name": "Customer.io - Customers",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "848316567",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 0,
        "name": "Transformation 02 - Create events and webinar collection",
        "depends_on": [
          19202
        ],
        "tasks": [
          {
            "name": "Customer.io - Main Events - 01 Create Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838001616",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Load - Customers",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Customer.io - Customers (API V2)",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1157363742",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 3,
        "name": "Load - Events",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "Customer.io - Events",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "608948633",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 4,
        "name": "Transformation 03 - Store sent events",
        "depends_on": [
          3
        ],
        "tasks": [
          {
            "name": "Customer.io - Main Events - 02 Store Sent Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838001618",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 5,
        "name": "Delete Unwanted (Junk Lead) Customer",
        "depends_on": [
          4
        ],
        "tasks": [
          {
            "name": "Customer.io - Delete Unwanted Customers (API V2)",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1157386381",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 6,
    "total_phases": 6,
    "mermaid": "graph LR\n    P19202[\"Transformation 01 - Create customers<br/>---<br/>TR: Customer.io - Customers\"]\n    P0[\"Transformation 02 - Create events and webinar c...<br/>---<br/>TR: Customer.io - Main Events - 01 Create Events\"]\n    P19202 --> P0\n    P2[\"Load - Customers<br/>---<br/>WR: Customer.io - Customers [API V2]\"]\n    P0 --> P2\n    P3[\"Load - Events<br/>---<br/>WR: Customer.io - Events\"]\n    P2 --> P3\n    P4[\"Transformation 03 - Store sent events<br/>---<br/>TR: Customer.io - Main Events - 02 Store Sent Events\"]\n    P3 --> P4\n    P5[\"Delete Unwanted [Junk Lead] Customer<br/>---<br/>WR: Customer.io - Delete Unwanted Customers [API V2]\"]\n    P4 --> P5\n\n    style P19202 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P3 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P4 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P5 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038329": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038329",
    "name": "Lead/Contact Unsubscribe Update",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-04-16T16:53:44+0200",
    "last_modified_by": "camila.monsalve@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce - Unsubscribed Flag",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838066543",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 1,
        "name": "Load",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Lead/Contact Unsubscribe UPDATE",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724023711",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P0[\"Transformation<br/>---<br/>TR: Salesforce - Unsubscribed Flag\"]\n    P1[\"Load<br/>---<br/>WR: Lead/Contact Unsubscribe UPDATE\"]\n    P0 --> P1\n\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038332": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038332",
    "name": "PAYG Credits Top-up",
    "description": "",
    "is_disabled": false,
    "version": 5,
    "last_modified": "2025-04-24T13:15:56+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "PAYG Credits",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "2802162274",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          },
          {
            "name": "Contember - Startup PAYG Projects",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "818187503",
            "enabled": false,
            "continue_on_failure": true,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation 01",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "PAYG Credits - Top-up - 01 Main",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838106463",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Load 01",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "PAYG Credits Top-up - com-keboola-azure-north-europe",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "2802195027",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "PAYG Credits Top-up - com-keboola-gcp-europe-west3",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1168180961",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "PAYG Credits Top-up - com-keboola-gcp-us-east4",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1168180806",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 3,
        "name": "Transformation 02",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "PAYG Credits - Top-up - 02 Store Events + Customer.io",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838106468",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 4,
        "name": "Load 02",
        "depends_on": [
          3
        ],
        "tasks": [
          {
            "name": "Customer.io - Credit Grant Events",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "2802411379",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 7,
    "total_phases": 5,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: PAYG Credits\"]\n    P1[\"Transformation 01<br/>---<br/>TR: PAYG Credits - Top-up - 01 Main\"]\n    P0 --> P1\n    P2[\"Load 01<br/>---<br/>WR: PAYG Credits Top-up - com-keboola-azure-north-e...<br/>WR: PAYG Credits Top-up - com-keboola-gcp-europe-west3<br/>WR: PAYG Credits Top-up - com-keboola-gcp-us-east4\"]\n    P1 --> P2\n    P3[\"Transformation 02<br/>---<br/>TR: PAYG Credits - Top-up - 02 Store Events + Custo...\"]\n    P2 --> P3\n    P4[\"Load 02<br/>---<br/>WR: Customer.io - Credit Grant Events\"]\n    P3 --> P4\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P4 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038335": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038335",
    "name": "PAYG Inactive Project",
    "description": "",
    "is_disabled": false,
    "version": 12,
    "last_modified": "2025-09-03T14:22:00+0200",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "PAYG Do Not Delete List",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "723940929",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation 01",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Customer.io - PAYG Inactivity - 01 Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838104578",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Load 01",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Customer.io - Inactive Project Events",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "2886214149",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 3,
        "name": "Transformation 02",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "Customer.io - PAYG Inactivity - 02 Store Sent Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838104584",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 81236,
        "name": "Transformation 03",
        "depends_on": [
          3
        ],
        "tasks": [
          {
            "name": "PAYG Delete Projects & Empty Orgs - 01 Projects and Orgs to Delete",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838098782",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 81810,
        "name": "Load 02",
        "depends_on": [
          81236
        ],
        "tasks": [
          {
            "name": "PAYG Delete Inactive Projects - com-keboola-azure-north-europe",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "951170659",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "PAYG Delete Inactive Projects - com-keboola-gcp-europe-west3",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1168198344",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "PAYG Delete Inactive Projects - com-keboola-gcp-us-east4",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1168199232",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 59541,
        "name": "Load 3",
        "depends_on": [
          81810
        ],
        "tasks": [
          {
            "name": "PAYG Delete Empty Organizations - com-keboola-azure-north-europe",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "714198874",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "PAYG Delete Empty Organizations - com-keboola-gcp-europe-west3",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1168198925",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "PAYG Delete Empty Organizations - com-keboola-gcp-us-east4",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1168199536",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 7455,
        "name": "Transformation 04",
        "depends_on": [
          59541
        ],
        "tasks": [
          {
            "name": "PAYG Delete Projects & Empty Orgs - 02 Store Sent Data",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838098783",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 12,
    "total_phases": 8,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: PAYG Do Not Delete List\"]\n    P1[\"Transformation 01<br/>---<br/>TR: Customer.io - PAYG Inactivity - 01 Events\"]\n    P0 --> P1\n    P2[\"Load 01<br/>---<br/>WR: Customer.io - Inactive Project Events\"]\n    P1 --> P2\n    P3[\"Transformation 02<br/>---<br/>TR: Customer.io - PAYG Inactivity - 02 Store Sent E...\"]\n    P2 --> P3\n    P81236[\"Transformation 03<br/>---<br/>TR: PAYG Delete Projects and Empty Orgs - 01 Projec...\"]\n    P3 --> P81236\n    P81810[\"Load 02<br/>---<br/>WR: PAYG Delete Inactive Projects - com-keboola-azu...<br/>WR: PAYG Delete Inactive Projects - com-keboola-gcp...<br/>WR: PAYG Delete Inactive Projects - com-keboola-gcp...\"]\n    P81236 --> P81810\n    P59541[\"Load 3<br/>---<br/>WR: PAYG Delete Empty Organizations - com-keboola-a... *<br/>WR: PAYG Delete Empty Organizations - com-keboola-g... *<br/>WR: PAYG Delete Empty Organizations - com-keboola-g... *\"]\n    P81810 --> P59541\n    P7455[\"Transformation 04<br/>---<br/>TR: PAYG Delete Projects and Empty Orgs - 02 Store ...\"]\n    P59541 --> P7455\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P81236 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P81810 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P59541 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P7455 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038337": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038337",
    "name": "PAYG Low Credits",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-02-13T00:18:56+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Transformation 01",
        "depends_on": [],
        "tasks": [
          {
            "name": "Customer.io - PAYG Low Credits - 01 Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838101578",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 1,
        "name": "Load",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Customer.io - Low Credits Events",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "656029912",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Transformation 02",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Customer.io - PAYG Low Credits - 02 Store Sent Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838101583",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 3,
    "total_phases": 3,
    "mermaid": "graph LR\n    P0[\"Transformation 01<br/>---<br/>TR: Customer.io - PAYG Low Credits - 01 Events\"]\n    P1[\"Load<br/>---<br/>WR: Customer.io - Low Credits Events\"]\n    P0 --> P1\n    P2[\"Transformation 02<br/>---<br/>TR: Customer.io - PAYG Low Credits - 02 Store Sent ...\"]\n    P1 --> P2\n\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P2 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038346": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038346",
    "name": "Webflow Components",
    "description": "",
    "is_disabled": false,
    "version": 4,
    "last_modified": "2025-07-30T09:19:40+0200",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Webflow Components",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "698624944",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Webflow - Components Collection",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838065230",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Load 1 (separate Webflow writers to not overload the API)",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Webflow - CREATE Components Collection Item",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "697637719",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 3,
        "name": "Load 2",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "Webflow - UPDATE Components Collection Item",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "698662860",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 4,
        "name": "Load 3",
        "depends_on": [
          3
        ],
        "tasks": [
          {
            "name": "Webflow - DELETE Components Collection Item",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "698677046",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 4,
    "total_phases": 5,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: Webflow Components\"]\n    P1[\"Transformation<br/>---<br/>TR: Webflow - Components Collection\"]\n    P0 --> P1\n    P2[\"Load 1 [separate Webflow writers to not overloa...<br/>---<br/>WR: Webflow - CREATE Components Collection Item\"]\n    P1 --> P2\n    P3[\"Load 2<br/>---<br/>WR: Webflow - UPDATE Components Collection Item\"]\n    P2 --> P3\n    P4[\"Load 3<br/>[no enabled tasks]\"]\n    P3 --> P4\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P3 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P4 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038348": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038348",
    "name": "Public Components List",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-02-13T00:18:57+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Public Components List",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838021868",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 1,
        "name": "Load",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Components",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "698939997",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P0[\"Transformation<br/>---<br/>TR: Public Components List\"]\n    P1[\"Load<br/>---<br/>WR: Components\"]\n    P0 --> P1\n\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038350": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038350",
    "name": "Salesforce NEW Leads/Events Preprocessing & Load",
    "description": "The `Salesforce Leads/Events Preprocessing & Load` flow is designed to efficiently manage and process Salesforce data, specifically focusing on leads and events. The flow is structured into multiple phases, each building upon the previous ones to ensure a seamless data transformation and loading process.\n\n## Extraction 1\nThis phase initiates the flow by extracting data from Salesforce using the `kds-team.ex-salesforce-v2` component. The tasks in this phase are set to continue on failure, ensuring that the extraction process is resilient and can handle errors without halting the entire flow.\n\n## Transformation - Leads\nFollowing the extraction, this phase focuses on transforming the leads data using `keboola.snowflake-transformation`. The tasks are critical for preparing the data for subsequent processing, and they do not continue on failure, emphasizing the importance of successful transformations.\n\n## Enriched Leads & Contacts\nThis phase enriches the leads and contacts data, leveraging `keboola.snowflake-transformation` to enhance the dataset with additional information. The tasks here are enabled to continue on failure, allowing for flexibility in handling potential issues during enrichment.\n\n## Load - Leads & Contacts\nIn this phase, the transformed and enriched leads and contacts data is loaded back into Salesforce using `kds-team.wr-salesforce`. The tasks are configured to continue on failure, ensuring that the loading process is robust and can proceed despite minor errors.\n\n## Transformation - Events\nAfter processing leads, the flow shifts focus to events data transformation using `keboola.snowflake-transformation`. This phase is crucial for preparing events data for loading, with tasks set to halt on failure to maintain data integrity.\n\n## Load - Events\nFinally, the transformed events data is loaded into Salesforce using `kds-team.wr-salesforce`. Similar to the leads loading phase, tasks are configured to continue on failure, providing resilience and continuity in the data loading process.",
    "is_disabled": false,
    "version": 65,
    "last_modified": "2026-01-12T09:31:29+0100",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 16530,
        "name": "Step 2",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "New Leads Validation - Created Leads Only",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "01k16svt9hxpr01f73m3dz601c",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation - Leads",
        "depends_on": [
          16530
        ],
        "tasks": [
          {
            "name": "Salesforce - PAYG/SPC/Academy Leads & Events - 01 Leads",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838080149",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Salesforce - Webforms - 01 Leads & Missing Campaign Members Statuses",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838087826",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "UTMs Campaign update",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1192786079",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 23474,
        "name": "Step 4",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Lusha Enrichment New Leads/Contacts",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "1225217919",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 42664,
        "name": "Enriched Leads & Contacts",
        "depends_on": [
          23474
        ],
        "tasks": [
          {
            "name": "Enriching New Leads with Lusha results",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1224963670",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Load - Leads & Contacts",
        "depends_on": [
          42664
        ],
        "tasks": [
          {
            "name": "Webform/Webinar Lead/Contact",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724025277",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "PAYG/SPC Lead/Contact",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724024544",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "Campaign Member Status",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724019562",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "Campaign",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1192798482",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 6796,
        "name": "Step 7",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 4,
        "name": "Transformation - Events",
        "depends_on": [
          6796
        ],
        "tasks": [
          {
            "name": "Salesforce - PAYG/SPC/Academy Leads & Events - 02 Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838080150",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Salesforce - Webforms - 02 Events & Campaign Members",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838087828",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          },
          {
            "name": "Assign Lead/Contact to Campaign",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1192847074",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 25516,
        "name": "Step 9",
        "depends_on": [
          4
        ],
        "tasks": [
          {
            "name": "Salesforce - PAYG/SPC/Academy Leads & Events - 03 Update Events of Converted Leads",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "883956414",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 88417,
        "name": "Step 10",
        "depends_on": [
          25516
        ],
        "tasks": [
          {
            "name": "CampaignMember Campaign Update",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1193103620",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 5,
        "name": "Load - Events",
        "depends_on": [
          88417
        ],
        "tasks": [
          {
            "name": "Webform/Webinar Event",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724025134",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          },
          {
            "name": "Campaign Member",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724028102",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 43936,
        "name": "Step 12",
        "depends_on": [
          5
        ],
        "tasks": [
          {
            "name": "PAYG/SPC Event",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724023947",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 53544,
        "name": "Step 13",
        "depends_on": [
          43936
        ],
        "tasks": [
          {
            "name": "Campaign_c field",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1193146341",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 86526,
        "name": "Step 14",
        "depends_on": [
          53544
        ],
        "tasks": [
          {
            "name": "Lead Campaign Update",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "1193145225",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 22,
    "total_phases": 14,
    "mermaid": "graph LR\n    P0[\"Extraction 1<br/>---<br/>EX: Keboola - Specific Tables *\"]\n    P16530[\"Step 2<br/>---<br/>TR: New Leads Validation - Created Leads Only\"]\n    P0 --> P16530\n    P1[\"Transformation - Leads<br/>---<br/>TR: Salesforce - PAYG/SPC/Academy Leads and Events ...<br/>TR: Salesforce - Webforms - 01 Leads and Missing Ca...<br/>TR: UTMs Campaign update\"]\n    P16530 --> P1\n    P23474[\"Step 4<br/>---<br/>TR: Lusha Enrichment New Leads/Contacts *\"]\n    P1 --> P23474\n    P42664[\"Enriched Leads and Contacts<br/>---<br/>TR: Enriching New Leads with Lusha results *\"]\n    P23474 --> P42664\n    P2[\"Load - Leads and Contacts<br/>---<br/>WR: Webform/Webinar Lead/Contact *<br/>WR: PAYG/SPC Lead/Contact *<br/>WR: Campaign Member Status *<br/>WR: Campaign\"]\n    P42664 --> P2\n    P6796[\"Step 7<br/>---<br/>EX: Keboola - Specific Tables *\"]\n    P2 --> P6796\n    P4[\"Transformation - Events<br/>---<br/>TR: Salesforce - PAYG/SPC/Academy Leads and Events ...<br/>TR: Salesforce - Webforms - 02 Events and Campaign ...<br/>TR: Assign Lead/Contact to Campaign\"]\n    P6796 --> P4\n    P25516[\"Step 9<br/>---<br/>TR: Salesforce - PAYG/SPC/Academy Leads and Events ...\"]\n    P4 --> P25516\n    P88417[\"Step 10<br/>---<br/>WR: CampaignMember Campaign Update *\"]\n    P25516 --> P88417\n    P5[\"Load - Events<br/>---<br/>WR: Webform/Webinar Event *<br/>WR: Campaign Member *\"]\n    P88417 --> P5\n    P43936[\"Step 12<br/>---<br/>WR: PAYG/SPC Event *\"]\n    P5 --> P43936\n    P53544[\"Step 13<br/>---<br/>TR: Campaign_c field\"]\n    P43936 --> P53544\n    P86526[\"Step 14<br/>---<br/>WR: Lead Campaign Update\"]\n    P53544 --> P86526\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P16530 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P23474 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P42664 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P6796 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P4 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P25516 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P88417 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P5 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P43936 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P53544 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P86526 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038354": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038354",
    "name": "PAYG New Project",
    "description": "",
    "is_disabled": false,
    "version": 6,
    "last_modified": "2025-05-21T18:51:56+0200",
    "last_modified_by": "dasa.damaskova@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "PAYG New Project",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "712433150",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation - App Data Prep",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "PAYG New Project - 01 App Data Prep",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838068556",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "App - Create New PAYG Project",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Create New PAYG Project [com-keboola-azure-north-europe]",
            "component_id": "kds-team.app-payg-project-generator",
            "component_short": "app-payg-project-generator",
            "config_id": "712152701",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Create New PAYG Project [com-keboola-gcp-us-east4]",
            "component_id": "kds-team.app-payg-project-generator",
            "component_short": "app-payg-project-generator",
            "config_id": "1181963301",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          },
          {
            "name": "Create New PAYG Project [com-keboola-gcp-europe-west3]",
            "component_id": "kds-team.app-payg-project-generator",
            "component_short": "app-payg-project-generator",
            "config_id": "21440742",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 3,
        "name": "Transformation - Add Attributes to Project",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "PAYG New Project - 02 Add Attributes",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838068557",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 6,
    "total_phases": 4,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: PAYG New Project\"]\n    P1[\"Transformation - App Data Prep<br/>---<br/>TR: PAYG New Project - 01 App Data Prep\"]\n    P0 --> P1\n    P2[\"App - Create New PAYG Project<br/>---<br/>AP: Create New PAYG Project [com-keboola-azure-nort...<br/>AP: Create New PAYG Project [com-keboola-gcp-us-east4]<br/>AP: Create New PAYG Project [com-keboola-gcp-europe...\"]\n    P1 --> P2\n    P3[\"Transformation - Add Attributes to Project<br/>---<br/>TR: PAYG New Project - 02 Add Attributes\"]\n    P2 --> P3\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038355": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038355",
    "name": "Expert Project Request",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-02-13T00:18:58+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Expert Project Request",
            "component_id": "fisa.ex-typeform",
            "component_short": "fisa.ex-typeform",
            "config_id": "719208418",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation 01",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Expert Project Request - 01 Main",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838018992",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "App",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Create Requested Expert Project",
            "component_id": "kds-team.app-payg-project-generator",
            "component_short": "app-payg-project-generator",
            "config_id": "719292805",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "AP"
          }
        ]
      },
      {
        "id": 3,
        "name": "Transformation 02 - Add Project Attributes",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "Expert Project Request - 02 Add Attributes",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838019052",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      }
    ],
    "total_tasks": 4,
    "total_phases": 4,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: Expert Project Request\"]\n    P1[\"Transformation 01<br/>---<br/>TR: Expert Project Request - 01 Main\"]\n    P0 --> P1\n    P2[\"App<br/>---<br/>AP: Create Requested Expert Project\"]\n    P1 --> P2\n    P3[\"Transformation 02 - Add Project Attributes<br/>---<br/>TR: Expert Project Request - 02 Add Attributes\"]\n    P2 --> P3\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038359": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038359",
    "name": "Feature Requests",
    "description": "",
    "is_disabled": true,
    "version": 4,
    "last_modified": "2025-03-28T13:41:57+0100",
    "last_modified_by": "martin.matejka@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Transformation 01",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce - Feature Request - 01 Leads",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "784507669",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 1,
        "name": "Load 01",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Feature Request Lead/Contact",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "784603270",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Extraction 01",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "Keboola - Specific Tables",
            "component_id": "kds-team.ex-salesforce-v2",
            "component_short": "ex-salesforce-v2",
            "config_id": "723991028",
            "enabled": true,
            "continue_on_failure": true,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 3,
        "name": "Transformation 02",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "Salesforce - Feature Request - 02 Events",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "784515197",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 4,
        "name": "Load 02",
        "depends_on": [
          3
        ],
        "tasks": [
          {
            "name": "Feature Request Event",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "784606152",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 5,
    "mermaid": "graph LR\n    P0[\"Transformation 01<br/>---<br/>TR: Salesforce - Feature Request - 01 Leads\"]\n    P1[\"Load 01<br/>---<br/>WR: Feature Request Lead/Contact *\"]\n    P0 --> P1\n    P2[\"Extraction 01<br/>---<br/>EX: Keboola - Specific Tables *\"]\n    P1 --> P2\n    P3[\"Transformation 02<br/>---<br/>TR: Salesforce - Feature Request - 02 Events\"]\n    P2 --> P3\n    P4[\"Load 02<br/>---<br/>WR: Feature Request Event\"]\n    P3 --> P4\n\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P2 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P4 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|842038376": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "842038376",
    "name": "Exchange Rates",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-02-13T00:18:58+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Transformation",
        "depends_on": [],
        "tasks": [
          {
            "name": "Salesforce - Exchange Rates",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "838070112",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 1,
        "name": "Load",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "Exchange Rates",
            "component_id": "kds-team.wr-salesforce",
            "component_short": "wr-salesforce",
            "config_id": "724018847",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P0[\"Transformation<br/>---<br/>TR: Salesforce - Exchange Rates\"]\n    P1[\"Load<br/>---<br/>WR: Exchange Rates\"]\n    P0 --> P1\n\n    style P0 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|867064704": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "867064704",
    "name": "PAYG Credits Recharge",
    "description": "",
    "is_disabled": false,
    "version": 9,
    "last_modified": "2025-05-08T09:38:45+0200",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Contember - Startup PAYG Projects",
            "component_id": "ex-generic-v2",
            "component_short": "generic-ext",
            "config_id": "818187503",
            "enabled": false,
            "continue_on_failure": true,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation 01",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "PAYG Credits - Recharge - 01 Main",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "867061897",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Load 01",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "PAYG Credits Recharge - com-keboola-azure-north-europe",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "2820865093",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "PAYG Credits Recharge - com-keboola-gcp-europe-west3",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1168162635",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "PAYG Credits Recharge - com-keboola-gcp-us-east4",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "1168163050",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      },
      {
        "id": 3,
        "name": "Transformation 02",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "PAYG Credits - Recharge - 02 Store Events + Customer.io",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "867063694",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 4,
        "name": "Load 02",
        "depends_on": [
          3
        ],
        "tasks": [
          {
            "name": "Customer.io - Credit Grant Events",
            "component_id": "kds-team.wr-generic",
            "component_short": "wr-generic",
            "config_id": "2802411379",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 6,
    "total_phases": 5,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>[no enabled tasks]\"]\n    P1[\"Transformation 01<br/>---<br/>TR: PAYG Credits - Recharge - 01 Main\"]\n    P0 --> P1\n    P2[\"Load 01<br/>---<br/>WR: PAYG Credits Recharge - com-keboola-azure-north...<br/>WR: PAYG Credits Recharge - com-keboola-gcp-europe-...<br/>WR: PAYG Credits Recharge - com-keboola-gcp-us-east4\"]\n    P1 --> P2\n    P3[\"Transformation 02<br/>---<br/>TR: PAYG Credits - Recharge - 02 Store Events + Cus...\"]\n    P2 --> P3\n    P4[\"Load 02<br/>---<br/>WR: Customer.io - Credit Grant Events\"]\n    P3 --> P4\n\n    style P0 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb\n    style P3 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P4 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-sales-marketing-output|973967523": {
    "project_alias": "ir-l2-sales-marketing-output",
    "config_id": "973967523",
    "name": "Scheduled PAYG Friction full wizard report",
    "description": "",
    "is_disabled": false,
    "version": 3,
    "last_modified": "2025-02-13T00:18:59+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 69026,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "Friction_full_wizard_sort_by_date",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1001226931",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 1,
        "name": "Scheduled configuration",
        "depends_on": [
          69026
        ],
        "tasks": [
          {
            "name": "PAYG Friction full wizard report",
            "component_id": "keboola.wr-google-drive",
            "component_short": "wr-google-drive",
            "config_id": "1093239692",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 2,
    "mermaid": "graph LR\n    P69026[\"Step 1<br/>---<br/>TR: Friction_full_wizard_sort_by_date\"]\n    P1[\"Scheduled configuration<br/>---<br/>WR: PAYG Friction full wizard report\"]\n    P69026 --> P1\n\n    style P69026 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P1 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  },
  "ir-l2-telemety-extractor|1223254408": {
    "project_alias": "ir-l2-telemety-extractor",
    "config_id": "1223254408",
    "name": "Public Telemetry Update",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T15:16:14+0100",
    "last_modified_by": "lucie.krasna@keboola.com",
    "phases": [
      {
        "id": 30584,
        "name": "Step 1",
        "depends_on": [],
        "tasks": [
          {
            "name": "01. Telemetry Pre-process",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1223170120",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 48219,
        "name": "Step 2",
        "depends_on": [
          30584
        ],
        "tasks": [
          {
            "name": "02. Tables for Public Telemetry",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "1223175052",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 17208,
        "name": "Step 3",
        "depends_on": [
          48219
        ],
        "tasks": [
          {
            "name": "Telemetry Data Extractor (PROD)",
            "component_id": "keboola.wr-db-snowflake-gcs-s3",
            "component_short": "wr-db-snowflake-gcs-s3",
            "config_id": "1223181559",
            "enabled": false,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 2,
    "total_phases": 3,
    "mermaid": "graph LR\n    P30584[\"Step 1<br/>---<br/>TR: 01. Telemetry Pre-process\"]\n    P48219[\"Step 2<br/>---<br/>TR: 02. Tables for Public Telemetry\"]\n    P30584 --> P48219\n    P17208[\"Step 3<br/>[no enabled tasks]\"]\n    P48219 --> P17208\n\n    style P30584 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P48219 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P17208 fill:#2a2e3d,stroke:#8b90a0,color:#e1e4eb"
  },
  "ir-l2-vc-output|842026063": {
    "project_alias": "ir-l2-vc-output",
    "config_id": "842026063",
    "name": "VC Sheet Daily",
    "description": "",
    "is_disabled": false,
    "version": 2,
    "last_modified": "2025-02-13T00:39:20+0100",
    "last_modified_by": "project-migration-gcp@keboola.com",
    "phases": [
      {
        "id": 0,
        "name": "Extraction",
        "depends_on": [],
        "tasks": [
          {
            "name": "Account Transfers",
            "component_id": "keboola.ex-google-drive",
            "component_short": "ex-google-drive",
            "config_id": "518169665",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "EX"
          }
        ]
      },
      {
        "id": 1,
        "name": "Transformation 01",
        "depends_on": [
          0
        ],
        "tasks": [
          {
            "name": "01 MRR",
            "component_id": "keboola.snowflake-transformation",
            "component_short": "snowflake-transformation",
            "config_id": "836709423",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 2,
        "name": "Transformation 02",
        "depends_on": [
          1
        ],
        "tasks": [
          {
            "name": "02 MRR - Pivot",
            "component_id": "keboola.python-transformation-v2",
            "component_short": "python-transformation-v2",
            "config_id": "836709424",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "TR"
          }
        ]
      },
      {
        "id": 3,
        "name": "Load",
        "depends_on": [
          2
        ],
        "tasks": [
          {
            "name": "VC",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "508945407",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          },
          {
            "name": "Contract Product MRR",
            "component_id": "keboola.wr-google-sheets",
            "component_short": "wr-google-sheets",
            "config_id": "885045600",
            "enabled": true,
            "continue_on_failure": false,
            "type_icon": "WR"
          }
        ]
      }
    ],
    "total_tasks": 5,
    "total_phases": 4,
    "mermaid": "graph LR\n    P0[\"Extraction<br/>---<br/>EX: Account Transfers\"]\n    P1[\"Transformation 01<br/>---<br/>TR: 01 MRR\"]\n    P0 --> P1\n    P2[\"Transformation 02<br/>---<br/>TR: 02 MRR - Pivot\"]\n    P1 --> P2\n    P3[\"Load<br/>---<br/>WR: VC<br/>WR: Contract Product MRR\"]\n    P2 --> P3\n\n    style P0 fill:#1a3a5c,stroke:#4f8ff7,color:#e1e4eb\n    style P1 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P2 fill:#2d1f4e,stroke:#b388ff,color:#e1e4eb\n    style P3 fill:#1a3c2c,stroke:#3ecf8e,color:#e1e4eb"
  }
};
