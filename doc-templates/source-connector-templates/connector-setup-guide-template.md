{This is a connector setup guide template. Items between curly brackets need to be replaced with specific names or directions. For links to Fivetran documentation pages/sections, do _not_ use the absolute path format like [page name](https://fivetran.com/docs/...), use [page/section name](/docs/...) instead. Also, do _not_ include `/index.md` in the path, trim the path at the page directory. For example,  [connector page name](/docs/connectors/applications/some-connector) is correct while [connector page name](/docs/connectors/applications/some-connector/index.md) is _incorrect_.}

---
name: {Item name in the left nav menu}
title: {SEO title}
description: {Meta Description - Example: Read step-by-step instructions on how to connect {Source system} with your destination using Fivetran connectors.}
hidden: {true or false. Set to true if the article should only be accessible via a direct link and not indexed by crawlers.}
---

# {Connector} Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="connector_name_source" /%}

Follow our setup guide to connect {Connector} to Fivetran.

-----

## Prerequisites

{Required}

To connect your {Source Connector} account to Fivetran, you need:
{List what the user needs to know or do before they get started}

- Administrator account {this is an example}
- Version 17 of Acme installed {this is an example}

---

## Setup instructions

To authorize Fivetran to connect to your {Connector} app, follow these instructions:

### <span class="step-item">{Descriptive name of step, for example, Get your API key}</span>

1. Enumerate the steps.
2. List one action per step.
   {use screenshots when needed}

### <span class="step-item">Second step</span>

1. Enumerate the steps.
2. List one action per step.
   {use screenshots when needed}

### <span class="step-item">Finish Fivetran configuration </span>

{Required}
1. In the [connector setup form](/docs/using-fivetran/fivetran-dashboard/connectors#addanewconnector), enter {field specific to this connector}.
1. {Additional instructions for this specific connector}
1. Click **Save & Test**. Fivetran will take it from here and sync your data from your {Connector Name} account.

### Setup tests (applies to all connectors but Applications connectors)

{List the setup tests that we run in the Fivetran dashboard for this connector.}

Fivetran performs the following {Connector} connection tests:
- The Validate Secrets test checks if you have entered the secrets in a valid JSON format. {this is an example}
- The Connecting to AWS test checks the connection and the cluster accessibility. {this is an example}


---

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Connector Overview](/docs/connectors/{connector category}/newconnector)
{Replace "connector category" with the actual category the connector falls into. The options are applications, databases, events, files, or functions.}

<b> </b>

[<i aria-hidden="true" class="material-icons">account_tree</i> Schema Information](/docs/connectors/{connector category}/newconnector#schemainformation)
{Replace "connector category" with the actual category the connector falls into. The options are applications, databases, events, files, or functions.}

<b> </b>

[<i aria-hidden="true" class="material-icons">assignment</i> Release Notes](/docs/connectors/{connector category}/newconnector/changelog)
{Only add the Release Notes related article if the connector is in Beta or GA. Private Preview connectors don't have release notes.}

<b> </b>

[<i aria-hidden="true" class="material-icons">settings</i> API Connector Configuration](/docs/rest-api/connectors/config#newconnector)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
