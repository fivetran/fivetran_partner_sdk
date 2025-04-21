{This is a template for standard destination connector docs. Items between curly brackets need to be replaced with specific names or directions. For links to Fivetran documentation pages/sections, do _not_ use the absolute path format like [page name](https://fivetran.com/docs/...), use [page/section name](/docs/...) instead. Also, do _not_ include `/index.md` in the path, trim the path at the page directory. For example,  [connector page name](/docs/connectors/applications/some-connector) is correct while [connector page name](/docs/connectors/applications/some-connector/index.md) is _incorrect_.}

---
name: {Item name in the left nav menu}
title: {SEO title}
description: {Meta Description}
hidden: {true or false. Set to true if the article should only be accessible via a direct link and not indexed by crawlers.}
---

# {Destination Name} {% badge text="Partner-Built" /%} {% availabilityBadge connector="destination_name_destination" /%}

{Description - Required - What is the service? Overview of what the service is (for example, Snowflake/Redshift). You can refer to publicly available documentation, however, remember to tailor it to Fivetran's context. Also, add a link to the related public website for the user’s reference.}

{Example: [Azure Data Lake Storage](https://azure.microsoft.com/en-gb/products/storage/data-lake-storage/) is a cloud-based, scalable data storage solution for big data analytics. ADLS allows you to store and manage massive amounts of data in any format. Fivetran supports data lakes built on ADLS as a destination.}

{Add this text to partner-built destination files: This destination is [partner-built](/docs/partner-built-program). For any questions related to {destination name} destination and its documentation, contact [{destination name} Support](_add support link_).}

------------------

## Setup guide

Follow our [step-by-step {Destination} setup guide](/docs/{path}/setup-guide) to connect your {Destination} destination with Fivetran.

------------------

## Type transformation and mapping

The data types in your {New Destination} follow Fivetran's [standard data type storage](/docs/destinations#datatypes).

We use the following data type conversions: <The following is an example>

| Fivetran Data Type | Destination Data Type | Notes |
| - | - | - |
| BOOLEAN | BOOLEAN | |
| SHORT | SMALLINT | |
| INT | INTEGER | |
| LONG | BIGINT | |
| FLOAT | REAL | |
| DOUBLE | DOUBLEPRECISION | |
| BIGDECIMAL | DECIMAL | |
| LOCALDATE | DATE | |
| INSTANT | TIMESTAMP_TZ | |
| LOCALDATETIME | TIMESTAMP_NTZ | |
| STRING | VARCHAR or TEXT | VARCHAR if `bytelength`is present, else TEXT |
| JSON | VARIANT | |
| BINARY | BINARY | |

<Example content>

> NOTE: SMALLINT, INTEGER, and BIGINT are synonymous with [NUMBER](https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint).

------------------

## Hybrid Deployment support

We {support/do not support} for the [Hybrid Deployment model](/docs/core-concepts/architecture/hybrid-deployment) for {destination name} destinations.

------------------

## Limitations (if applicable)

{Document the destination's limitations - for example, this destination does not support history mode.}

------------------

## Optimize {Destination} (if applicable)
{List steps to optimize destination performance.}

------------------

## Data load costs

List any additional cost info our customers need to know.
------------------

## Migrate destinations {This is an example section}

1. Enumerate the steps.
2. Use screenshots if necessary.