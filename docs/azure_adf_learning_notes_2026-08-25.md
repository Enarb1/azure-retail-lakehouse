# Azure Data Factory + ADLS Gen2 — Learning Notes

**Date:** 25 August 2026  
**Project:** Azure Retail Lakehouse  
**Focus:** Azure Data Factory ingestion, ADLS Gen2, metadata-driven orchestration, managed identity, and troubleshooting

---

## 1. What I built today

Today I extended the retail lakehouse project into Azure by creating the first real Azure Data Factory ingestion flow.

The working flow is:

```text
Local raw Olist CSV files
        ↓
Manual upload to ADLS landing container
        ↓
ADF Lookup activity
        ↓
ADF ForEach activity
        ↓
ADF Copy activity
        ↓
ADLS raw container
```

The pipeline successfully processes these five source files:

- `olist_orders_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_products_dataset.csv`
- `olist_order_payments_dataset.csv`

---

## 2. Azure resources created

### Resource group

Created a dedicated resource group:

```text
rg-azure-retail-lakehouse
```

### Why use a dedicated resource group?

A resource group keeps all resources for one project together.

This makes it easier to manage the project, monitor cost, apply permissions, find related Azure resources, and delete the whole learning environment later.

For a portfolio project, this is much cleaner than mixing resources from unrelated experiments.

---

## 3. Region selection and Azure Policy

The first attempt used **West Europe**, but the Azure for Students subscription returned:

```text
RequestDisallowedByPolicy
```

The subscription policy only allows:

```text
Norway East
Germany West Central
Italy North
UK South
Poland Central
```

I therefore used:

```text
Germany West Central
```

### Why this matters

Azure subscriptions can have policies that limit where resources may be created.

This is common in enterprise environments, student subscriptions, and governed cloud environments.

A deployment error is not always caused by bad configuration. It can come from an organizational or subscription-level policy.

> Always read the complete Azure error message before changing the resource configuration.

---

## 4. Azure Storage / ADLS Gen2

Created a Storage Account with:

```text
Performance: Standard
Redundancy: LRS
Hierarchical namespace: Enabled
Primary service: Blob Storage / ADLS Gen2
```

### Why Standard storage?

The project only contains a small educational dataset. Premium storage would be unnecessary and more expensive.

### Why LRS?

LRS means **Locally Redundant Storage**. Azure keeps multiple copies of the data inside one region.

For this learning project, high geographic resilience is unnecessary, LRS is cheaper, the original source files are available elsewhere, and the goal is to learn the architecture rather than build a disaster-recovery solution.

### Why enable hierarchical namespace?

This is the important setting that gives the Storage Account **ADLS Gen2 capabilities**.

It allows storage to behave more like a filesystem:

```text
container/
    directory/
        file
```

This is useful for data engineering because lake data is normally organized into directories or zones.

---

## 5. Storage containers

Created:

```text
landing
raw
```

The `raw` container also contains:

```text
metadata/
    entities.json
```

### Why separate `landing` and `raw`?

`landing` represents the place where source files first arrive. The data is still exactly as received from the source.

`raw` represents data that has been ingested by the pipeline.

This gives us a clear boundary:

```text
source / external delivery
        ↓
landing
        ↓
ADF ingestion
        ↓
raw
```

Even though both currently contain the same CSV content, the separation is important because it shows **pipeline ownership**.

---

## 6. Azure Data Factory

Created an Azure Data Factory:

```text
branimir01
```

ADF is being used as the **orchestration and ingestion layer**.

### Why use ADF when Databricks already has Workflows?

Databricks Workflows already orchestrate:

```text
silver_pipeline
      ↓
gold_dimensions
      ↓
gold_validation
```

ADF adds a different responsibility:

- external ingestion;
- integration with Azure storage;
- metadata-driven pipelines;
- scheduling and orchestration across Azure services.

A useful architectural separation is:

```text
ADF
→ moves and orchestrates data/services

Databricks
→ transforms and validates data
```

---

## 7. Cost protection

Created a budget of:

```text
$10
```

with alerts.

### Why create a budget?

The Azure for Students subscription has limited credit.

A budget helps detect unexpected usage before too much credit is consumed.

Important lesson:

> A budget is an alerting mechanism, not necessarily a hard shutdown mechanism.

---

## 8. Managed Identity

The ADF linked service uses:

```text
Authentication type:
System Assigned Managed Identity
```

### Why use Managed Identity?

Without Managed Identity, ADF might need storage account keys, passwords, secrets, or manually managed credentials.

Managed Identity lets Azure give the Data Factory its own identity:

```text
Data Factory
     ↓
Azure identity
     ↓
RBAC permission
     ↓
Storage Account
```

This is safer than embedding credentials in pipelines.

---

## 9. Storage Blob Data Contributor role

The first ADF connection attempt failed with:

```text
ADLSGen2ForbiddenError
AuthorizationPermissionMismatch
```

The fix was assigning:

```text
Storage Blob Data Contributor
```

to the Data Factory managed identity.

### Why this role?

ADF needs to read files from `landing` and write files into `raw`.

A read-only role would not be enough because the Copy activity must write data.

This role gives ADF data access without giving it unnecessary ownership of the Azure subscription.

### Important distinction

Azure resource permissions and storage data permissions are different.

Being able to see a Storage Account does not automatically mean an identity can read or write the files inside it.

---

## 10. Soft delete / endpoint compatibility issue

The first ADLS linked-service test failed with:

```text
EndpointUnsupportedAccountFeatures
```

After adjusting the Storage Account data-protection settings, the error changed to the RBAC permission error.

### What I learned

Troubleshooting often happens in layers:

```text
1. Endpoint/storage feature issue
        ↓ fixed
2. Authorization issue
        ↓ fixed
3. Successful connection
```

The first error may hide the next problem.

---

## 11. ADF Linked Service

Created:

```text
ls_adls_retail
```

This linked service represents the connection between ADF and ADLS Gen2.

### Why use a linked service?

A linked service is a reusable connection definition.

Instead of configuring the Storage Account separately inside every dataset or activity, the connection is defined once and reused.

A useful comparison is:

```text
Airflow Connection  ≈  ADF Linked Service
```

---

## 12. Parameterized datasets

Created:

```text
ds_adls_landing_csv
ds_adls_raw_csv
ds_adls_entities_json
```

Both CSV datasets contain a parameter:

```text
file_name
```

Their filename is configured dynamically using:

```text
@dataset().file_name
```

### Why parameterize the datasets?

Without parameters, I could create five separate datasets for orders, customers, products, and so on.

Instead, one dataset can represent any CSV file:

```text
ds_adls_landing_csv
        +
file_name parameter
```

This is more scalable and easier to maintain.

> Prefer reusable metadata-driven components over duplicated pipelines and datasets.

---

## 13. Metadata file

Created:

```text
raw/metadata/entities.json
```

with five records such as:

```json
{
  "entity_name": "orders",
  "source_file": "olist_orders_dataset.csv"
}
```

### Why use metadata?

The metadata tells the pipeline **what to process**.

Instead of hard-coding five activities, ADF reads a configuration file.

This separates pipeline logic from entity configuration.

If another source entity is added later, the design can potentially be extended by changing metadata instead of duplicating the whole pipeline.

---

## 14. Lookup activity

Created:

```text
lookup_entities
```

It reads:

```text
ds_adls_entities_json
```

with:

```text
First row only = False
```

The output returned:

```text
count = 5
```

### Why use Lookup?

Lookup is being used to load configuration data into the pipeline.

Its output becomes an array that can drive a `ForEach`.

### Why disable "First row only"?

If enabled, Lookup would return only one metadata record.

We need all five because the pipeline must process every entity.

---

## 15. ForEach activity

Created:

```text
foreach_entity
```

with:

```text
@activity('lookup_entities').output.value
```

### What this expression means

`activity('lookup_entities')` references the Lookup activity.

`.output.value` references the array returned by the Lookup.

Therefore the ForEach loops over all five metadata objects.

Conceptually:

```text
for entity in entities:
    process(entity)
```

### Why use ForEach?

Without `ForEach`, the pipeline would require separate activities for every file.

With ForEach:

```text
one pipeline
+
one Copy activity
+
five metadata records
=
five file copies
```

This is a classic metadata-driven ingestion pattern.

---

## 16. `@item()` inside ForEach

Inside the ForEach, the current metadata record is referenced using:

```text
@item()
```

For example:

```text
@item().source_file
```

During one iteration this might resolve to:

```text
olist_orders_dataset.csv
```

and during another:

```text
olist_products_dataset.csv
```

### Why this matters

This makes the Copy activity dynamic.

The activity does not need to know which file it is processing in advance. The current metadata record provides that information.

---

## 17. Copy Activity

Created:

```text
copy_entity_to_raw
```

Source:

```text
ds_adls_landing_csv
file_name = @item().source_file
```

Sink:

```text
ds_adls_raw_csv
file_name = @item().source_file
```

The result is:

```text
landing/olist_orders_dataset.csv
            ↓
raw/olist_orders_dataset.csv
```

and the same pattern is repeated for all five entities.

### Why Copy Activity?

ADF Copy Activity is designed for data movement.

It is more appropriate than writing transformation code when the requirement is simply to read a file, move/copy it, and preserve the data.

Transformation belongs later in Databricks.

---

## 18. Debugging the wrong sink dataset

The first pipeline run reported success:

```text
filesRead: 1
filesWritten: 1
errors: []
```

but no new files appeared in `raw`.

The Copy activity Sink was accidentally configured as:

```text
ds_adls_landing_csv
```

instead of:

```text
ds_adls_raw_csv
```

So ADF was effectively doing:

```text
landing
   ↓
landing
```

instead of:

```text
landing
   ↓
raw
```

### What I learned

A successful activity does not always mean the business result is correct.

ADF correctly performed the operation it was configured to perform. The configuration itself was wrong.

> Technical success and logical correctness are different things.

Always validate the final data location and content, not only the green success status.

---

## 19. Reading ADF Debug output

The successful Copy activity showed metrics such as:

```text
dataRead
dataWritten
filesRead
filesWritten
copyDuration
throughput
usedDataIntegrationUnits
billingReference
```

### Why these metrics matter

They help answer:

- Did ADF really read a file?
- Did it actually write a file?
- How much data moved?
- How long did it take?
- How much Integration Runtime was used?
- Were there errors?

These metrics are useful when debugging and monitoring production pipelines.

---

## 20. Azure Integration Runtime

The pipeline output showed:

```text
AutoResolveIntegrationRuntime
Germany West Central
```

### What is an Integration Runtime?

The Integration Runtime is the compute/infrastructure ADF uses to perform activities such as Copy.

A useful mental model is:

```text
ADF pipeline
= orchestration definition

Integration Runtime
= execution/data-movement engine
```

For this cloud-to-cloud copy, the Azure Integration Runtime is sufficient.

---

## 21. Why the files were manually uploaded to `landing`

The original Olist CSV files are stored locally.

ADF's cloud Integration Runtime cannot directly read arbitrary files from my laptop.

One option would be to install a Self-hosted Integration Runtime, but that would add unnecessary complexity for this project.

Instead, the files were manually uploaded to `landing`, and ADF performs the real ingestion step:

```text
landing → raw
```

### Why this was a reasonable choice

The learning objective is ADF orchestration, metadata-driven ingestion, ADLS, parameterization, and monitoring.

Installing and maintaining a self-hosted runtime would distract from those goals without adding much value for this small portfolio project.

---

## 22. Publishing the pipeline

After the successful Debug run, I used:

```text
Publish all
```

### Debug vs Publish

Debug runs the current development version.

Publish deploys the current ADF objects as the saved factory version.

```text
Authoring changes
        ↓
Debug/test
        ↓
Publish
```

The pipeline should be tested before publishing.

---

## 23. Final working ADF ingestion design

```text
ADLS landing
    |
    | five raw Olist CSV files
    ↓

lookup_entities
    |
    | reads entities.json
    | returns 5 metadata records
    ↓

foreach_entity
    |
    | iterates over Lookup output
    ↓

copy_entity_to_raw
    |
    | source filename:
    | @item().source_file
    ↓

ADLS raw
```

This is a metadata-driven ingestion pipeline.

---

## 24. Relationship to the existing Databricks project

Before today, the Databricks project already contained:

```text
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Validation
```

and the Databricks workflow:

```text
silver_pipeline
      ↓
gold_dimensions
      ↓
gold_validation
```

ADF now introduces the Azure ingestion/orchestration layer before those transformations.

The target architecture is moving toward:

```text
Source files
     ↓
ADF
     ↓
ADLS
     ↓
Databricks
     ↓
Bronze
     ↓
Silver
     ↓
Gold
     ↓
Validation
     ↓
Power BI
```

---

## 25. Airflow concepts mapped to ADF

| Airflow | Azure Data Factory |
|---|---|
| DAG | Pipeline |
| Task | Activity |
| Connection | Linked Service |
| Dynamic task pattern | Lookup + ForEach |
| DAG parameters | Pipeline parameters |
| XCom-like activity output | Activity output / variables |
| Scheduler | Trigger |
| Task logs | Monitor / activity output |

The concepts are similar even though the implementation and UI are different.

---

## 26. Key engineering lessons from today

1. **Read cloud error messages carefully.** The Azure policy error listed the allowed regions; the permission error pointed to authorization; the endpoint error pointed to unsupported account features.
2. **Use identities instead of secrets.** Managed Identity plus RBAC is safer than embedding Storage Account keys in ADF.
3. **Parameterize repeated patterns.** One reusable dataset and one Copy activity are better than five almost identical configurations.
4. **Separate configuration from processing logic.** `entities.json` tells ADF what to process; the pipeline defines how processing happens.
5. **A successful run can still produce the wrong result.** The first Copy run was green but pointed back to `landing`.
6. **Keep architecture responsibilities clear.** ADF handles ingestion/orchestration; Databricks handles Spark transformations and validation; ADLS stores files.
7. **Design according to project needs.** For a small learning project, Standard storage, LRS, and Azure Integration Runtime are sufficient.

---

## 27. Current checkpoint

At the end of today's work:

- Azure for Students subscription is active.
- Azure credit is available.
- A $10 Azure budget with alerts is configured.
- Project resource group exists.
- ADLS Gen2 Storage Account exists.
- `landing` and `raw` containers exist.
- ADF is deployed.
- ADF authenticates to ADLS using Managed Identity.
- ADF has `Storage Blob Data Contributor`.
- `ls_adls_retail` linked service works.
- Parameterized landing and raw CSV datasets exist.
- Metadata JSON dataset exists.
- `entities.json` contains five entity definitions.
- `lookup_entities` returns all five entities.
- `foreach_entity` loops over all five records.
- `copy_entity_to_raw` dynamically copies each source file.
- All five files successfully arrive in `raw`.
- The pipeline has been published.

---

## 28. What comes next

The next logical stage is to extend orchestration beyond raw ingestion.

The future flow will be approximately:

```text
ADF ingestion
     ↓
Databricks processing
     ↓
Silver
     ↓
Gold
     ↓
Validation
```

After the Azure orchestration is complete, the remaining major project stage is the Power BI report.

---

## Interview explanation

> I built a metadata-driven Azure Data Factory ingestion pipeline for five Olist source entities. The source files land in ADLS Gen2, ADF reads an entity configuration file with a Lookup activity, iterates through the entities using ForEach, and uses one parameterized Copy activity to move each file from a landing container into the raw data-lake area. I used a system-assigned managed identity with Storage Blob Data Contributor instead of storage keys. I also troubleshot Azure Policy region restrictions, ADLS endpoint settings, RBAC permissions, and a logical sink-dataset configuration error. The design avoids duplicated pipelines and prepares the Azure ingestion layer to orchestrate the existing Databricks Silver, Gold, and validation workflow.
