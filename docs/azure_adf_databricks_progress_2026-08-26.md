# Azure Retail Lakehouse — Progress Note

**Date:** 26 August 2026  
**Project stage:** Actual Week 3 — ahead of schedule  
**Focus:** ADF → Key Vault → Databricks orchestration and end-to-end pipeline validation

---

## What I completed today

Today I extended the Azure Data Factory pipeline so that, after ingesting the five Olist CSV files into ADLS Gen2, ADF can securely trigger the existing Databricks Job.

The working end-to-end flow is now:

```text
ADLS landing
    ↓
ADF Lookup
    ↓
ADF ForEach
    ↓
ADF Copy Activity
    ↓
ADLS raw
    ↓
ADF Web Activity
    ↓
Azure Key Vault
    ↓
Databricks Jobs REST API
    ↓
silver_pipeline
    ↓
gold_dimensions
    ↓
gold_validation
```

The complete ADF Debug run and the downstream Databricks Job both completed successfully.

---

## 1. Databricks Job integration

The existing Databricks Job already contains:

```text
silver_pipeline
      ↓
gold_dimensions
      ↓
gold_validation
```

The job is triggered from ADF rather than rebuilt inside ADF.

### Why this design?

ADF is responsible for orchestration and ingestion, while Databricks remains responsible for Spark transformations and validation.

This avoids duplicating transformation logic.

The responsibilities are separated clearly:

```text
ADF
→ ingestion and orchestration

Databricks
→ transformation and validation

ADLS
→ file storage
```

---

## 2. Databricks Jobs API

ADF triggers the Databricks Job using the Jobs REST API.

The endpoint used is conceptually:

```text
POST /api/2.2/jobs/run-now
```

with the existing Databricks Job ID.

### Why use the REST API?

The current Databricks Free Edition workspace is hosted on:

```text
cloud.databricks.com
```

rather than:

```text
azuredatabricks.net
```

Because it is not a native Azure Databricks workspace, the normal Azure Databricks linked-service activity is not the right integration path.

Using a Web activity and the Databricks Jobs REST API lets ADF orchestrate the existing Databricks Free Edition job without creating another paid Databricks environment.

---

## 3. Databricks Personal Access Token

A short-lived Databricks Personal Access Token was created specifically for ADF.

The token was scoped to the Jobs API rather than broad access.

### Why use a limited scope?

This follows the principle of least privilege.

ADF only needs enough permission to trigger the existing Databricks Job.

It does not need unrestricted access to the Databricks workspace.

The token itself is never stored directly in the pipeline definition.

---

## 4. Azure Key Vault

Created an Azure Key Vault and stored the Databricks token as:

```text
databricks-token
```

The vault uses:

```text
Azure role-based access control
```

### Why use Key Vault?

Secrets should not be hard-coded in:

- pipeline JSON;
- notebook code;
- source control;
- activity parameters;
- monitoring output.

Key Vault provides a central secure location for credentials and tokens.

---

## 5. Key Vault RBAC

Two different role assignments were used.

### My user account

Assigned:

```text
Key Vault Secrets Officer
```

This allowed me to create and manage the secret.

### Azure Data Factory managed identity

Assigned:

```text
Key Vault Secrets User
```

This allows ADF to read the secret value but not manage all secrets.

### Why separate these roles?

Different identities should receive only the permissions they need.

My user needs to administer the secret.

ADF only needs to read it at runtime.

---

## 6. Retrieving the token from ADF

Added a Web activity:

```text
get_databricks_token
```

It uses:

```text
Method: GET
Authentication: System Assigned Managed Identity
Resource: https://vault.azure.net
```

The activity retrieves the Databricks token from Azure Key Vault.

### Why Managed Identity here?

ADF authenticates to Azure Key Vault using its own Azure identity.

This means no Azure credential needs to be stored inside ADF.

The only external secret involved is the Databricks PAT, and that secret is stored in Key Vault.

---

## 7. Secure activity output

The Key Vault Web activity uses secure output.

### Why?

The response from Key Vault contains the actual Databricks token.

Without secure output, sensitive values could appear in ADF monitoring logs.

The Databricks trigger activity also uses secure input so the Authorization header is not exposed unnecessarily.

---

## 8. Triggering the Databricks Job

Added another Web activity:

```text
trigger_databricks_job
```

It performs a POST request to the Databricks Jobs API.

The Authorization header is created dynamically from the Key Vault activity output:

```text
Bearer <token>
```

The request body contains the existing Databricks Job ID.

### Why use two Web activities?

The flow intentionally separates:

```text
retrieve secret
```

from:

```text
call external API
```

This makes the pipeline easier to understand, troubleshoot, and secure.

---

## 9. ADF dependency debugging

At first, the Web activities were not executed.

The dependency from:

```text
foreach_entity
```

to:

```text
get_databricks_token
```

was not configured as an **Upon Success** dependency.

After correcting the dependency, the pipeline correctly continued to the Web activities.

### What I learned

A pipeline activity can exist on the canvas but still never run if its dependency condition is wrong.

ADF dependencies are part of the execution logic, not only visual connectors.

---

## 10. Web activity URL debugging

The first Databricks API call failed because the workspace URL was malformed.

The URL effectively contained:

```text
https://https://...
```

After correcting the URL, the API call succeeded.

### What I learned

REST API integrations often fail because of simple request construction issues:

- wrong URL;
- wrong HTTP method;
- incorrect header;
- malformed body;
- authentication failure.

Before assuming the remote service is broken, inspect the actual request configuration.

---

## 11. ADF successfully triggered Databricks

After fixing the Web activity configuration:

```text
get_databricks_token
```

succeeded, and:

```text
trigger_databricks_job
```

also succeeded.

A new run appeared in Databricks Jobs & Pipelines.

This confirmed that:

```text
ADF → Key Vault → Databricks REST API
```

was working correctly.

---

## 12. Databricks Job failure after successful ADF trigger

The Databricks Job initially failed in:

```text
silver_pipeline
```

with:

```text
DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION
```

The Silver notebook contained a learning/demo query using:

```sql
VERSION AS OF 0
```

### Why did this fail now?

Delta Lake history can still list old table versions even after the physical files required to reconstruct those versions are no longer retained.

The table had:

```text
delta.deletedFileRetentionDuration = 168 HOURS
```

which is seven days.

Version 0 was older than the available deleted-file retention period.

---

## 13. Making the time-travel demo workflow-safe

The hard-coded historical query was changed.

Instead of:

```text
VERSION AS OF 0
```

the notebook now reads the Delta table history and finds the latest available version.

Example logic:

```python
history = spark.sql(f"""
    DESCRIBE HISTORY {catalog}.silver.orders
""")

latest_version = history.agg(F.max("version")).first()[0]
```

Then:

```sql
VERSION AS OF <latest_version>
```

is used.

### Why is this better?

The original code was suitable for a one-time learning exercise.

It was not safe for a notebook that is now executed repeatedly as part of an automated workflow.

A production-style pipeline should not depend on an old historical version remaining physically readable forever.

---

## 14. Fixing the comparison cell

A second cell also still referenced:

```text
orders_v0
```

and `VERSION AS OF 0`.

It was updated to compare:

```text
orders_latest
```

with:

```text
orders_current
```

using:

```python
exceptAll()
```

### Why?

This keeps the Delta time-travel demonstration without introducing a fragile dependency on an old version.

The latest historical version should logically match the current table.

---

## 15. Important production lesson

The failure demonstrated a useful distinction:

```text
learning/demo code
≠
production-safe pipeline code
```

A notebook may begin as an exploration notebook, but once it becomes part of an automated workflow, demo sections must be reviewed for assumptions such as:

- fixed historical versions;
- temporary test data;
- hard-coded paths;
- manually created state;
- one-time setup operations.

---

## 16. Final Databricks validation

After correcting the time-travel code, the Databricks Job ran successfully:

```text
silver_pipeline   → Succeeded
gold_dimensions   → Succeeded
gold_validation   → Succeeded
```

The Gold validation task also completed successfully.

---

## 17. Final ADF end-to-end validation

The complete ADF pipeline was then run again.

The full process succeeded:

```text
lookup_entities
      ↓
foreach_entity
      ↓
5 × copy_entity_to_raw
      ↓
get_databricks_token
      ↓
trigger_databricks_job
```

ADF successfully copied all source files and triggered Databricks.

The Databricks workflow then successfully completed all three tasks.

This proves the current orchestration path works end to end.

---

## 18. Local and Databricks notebook synchronization

After confirming the Databricks Job worked, the same Silver notebook corrections were applied to the local project copy.

### Why keep both copies synchronized?

The project currently uses both:

```text
local PyCharm development
```

and:

```text
Databricks Workspace execution
```

If only one copy is updated, later changes can accidentally reintroduce old bugs.

Keeping them aligned makes the Git repository a reliable representation of the working project.

---

## Current architecture checkpoint

```text
Local source files
      ↓
ADLS landing
      ↓
ADF Lookup
      ↓
ADF ForEach
      ↓
ADF Copy
      ↓
ADLS raw
      ↓
ADF retrieves Databricks token from Key Vault
      ↓
ADF calls Databricks Jobs API
      ↓
silver_pipeline
      ↓
gold_dimensions
      ↓
gold_validation
```

---

## Security choices made

The project now uses:

- Azure Data Factory System Assigned Managed Identity;
- Azure RBAC for ADLS access;
- Azure Key Vault for the Databricks PAT;
- Key Vault Secrets User for ADF;
- short-lived, limited-scope Databricks token;
- secure input/output on sensitive ADF Web activities.

These choices reduce the need for hard-coded credentials.

---

## Key engineering lessons from today

1. **Orchestration should reuse existing transformation pipelines rather than duplicate them.**
2. **REST APIs are useful when native service integrations do not fit the environment.**
3. **Secrets belong in a secret manager, not in pipeline definitions.**
4. **Managed Identity is the preferred way for Azure services to authenticate to other Azure services.**
5. **Least-privilege RBAC reduces unnecessary access.**
6. **ADF dependency conditions directly affect whether downstream activities execute.**
7. **A successful API trigger does not guarantee the downstream job itself will succeed.**
8. **Automated pipelines expose fragile assumptions that may not appear during manual notebook development.**
9. **Delta table history and Delta time-travel readability are not the same thing.**
10. **Local source code and deployed notebook code should stay synchronized.**

---

## Current project status

The project now has:

- working Bronze, Silver, and Gold Delta layers;
- rejected-record handling;
- incremental Delta `MERGE`;
- SCD Type 1 and Type 2 demonstrations;
- Gold validation quality gate;
- a three-task Databricks workflow;
- ADLS Gen2 landing and raw storage;
- metadata-driven ADF ingestion;
- secure ADF → Key Vault integration;
- ADF → Databricks Jobs API integration;
- successful end-to-end ADF + Databricks execution.

---

## What comes next

The next ADF improvements can focus on production-minded orchestration, such as:

```text
pipeline parameters
run identifiers
retry/timeout configuration
failure handling
monitoring
safe reruns
```

After the Azure orchestration phase is complete, the remaining major project stage is the Power BI reporting layer.

---

## Interview explanation

> I extended the project so Azure Data Factory now orchestrates both ingestion and downstream Databricks processing. ADF uses a metadata-driven Lookup and ForEach pattern to copy five Olist source files from an ADLS landing area into raw storage. After ingestion, ADF retrieves a short-lived Databricks token from Azure Key Vault using its system-assigned managed identity and calls the Databricks Jobs REST API to trigger my existing Silver → Gold → Validation workflow. I used Azure RBAC and secure activity input/output rather than embedding credentials. During testing, I also fixed a Delta time-travel demo that had become unsafe for automated execution because version 0 had passed the seven-day deleted-file retention window. The final ADF run successfully triggered the full Databricks workflow and all validation checks passed.
