{This is a template for the source connector overview page. Items between curly brackets need to be replaced with specific names or directions. For links to Fivetran documentation pages/sections, do _not_ use the absolute path format like [page name](https://fivetran.com/docs/...), use [page/section name](/docs/...) instead. Also, do _not_ include `/index.md` in the path, trim the path at the page directory. For example, [connector page name](/docs/connectors/applications/some-connector) is correct while [connector page name](/docs/connectors/applications/some-connector/index.md) is _incorrect_.}

---
name: {Item name in the left nav menu}
title: {Source system} connector by Fivetran | Fivetran documentation
Description: Connect your {source system} data to your destination using Fivetran.
hidden: {true or false. Set to true if the article should only be accessible via a direct link and not indexed by crawlers.}
---

# {Source Connector Name} {% badge text="Partner-Built" /%} {% availabilityBadge connector="connector_name_source" /%}

{Description - Required - What is the service? Overview of what the service is (Marketo = Email marketing tool). You can refer to publicly available documentation, however, remember to tailor it to Fivetran's context.
Also, add a link to a related public website for users’ reference.}

{Example: [15Five](https://www.15five.com/) is an employee performance management platform.}

------------------

## Features

{List the supported features and their notes separated by a colon to populate the Supported and Notes columns of the Feature table. Unsupported features are added automatically. Use the colon after the feature name. Each feature should be on a new line. The feature list should be wrapped by `{% featureTable connector="connector_id" %} ... {% /featureTable %}`.  It helps to autopopulate some data in the Note column. If you want to add a new feature to the Feature table or change a link of an existing feature, make the relevant change to the template file: `/docs/_template_articles/feature-table.yaml`. Add notes to clarify the conditions under which the particular feature is supported for the connector. For example, if a connector only supports custom data for certain tables, specify these tables. Or, if a connector supports data blocking at every level, list all the levels: "Column level, table level, and schema level". For definitions of features, see our [Features documentation](https://fivetran.com/docs/using-fivetran/features). }

{% featureTable connector="connector_id" %}
Capture Deletes:
Custom Data:
Data Blocking:
Column Hashing:
Re-sync: Connector level
History Mode:
API Configurable:
Priority-first sync:
Fivetran data models:
Private Networking:
{% /featureTable %}

{ If the connector supports API configuration, add it to the list of [Connectors supported by API and their authorization methods]
(/docs/rest-api/getting-started#connectorssupportedbyapiandtheirauthorizationmethods) and specify whether it supports Connect Card and API authorization.}

------------------

## Setup guide

Follow our [step-by-step {Connector name} setup guide](/docs/{path}/setup-guide) to connect {Connector} with your destination using Fivetran connectors.

------------------

## Sync overview

{Optional}
{Brief overview of how the connector works.
What processes do we use to sync the data? Query the initial sync, then read off the logs?
If the connector uses webhooks as part of its sync strategy, add it to the list in the [Exceptions section](/docs/security#exceptions) - under the **Event data from the Webhooks connector and other connectors using webhooks** item. Also, specify its data retention period.}

{Use subheadings (### and ####) to cover items such as rollback syncs and conversion windows}

------------------

## Schema information

{Embed a link to the connector’s ERD.}

{In subheadings (### and ####) cover items such as initial sync, updating data, deleted rows, deleted columns, type transformations and mapping, and Fivetran-generated columns.}
