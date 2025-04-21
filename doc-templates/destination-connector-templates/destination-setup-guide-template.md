{This is a destination connector setup guide template. Items between curly brackets need to be replaced with specific names or directions. For links to Fivetran documentation pages/sections, do _not_ use the absolute path format like [page name](https://fivetran.com/docs/...), use [page/section name](/docs/...) instead. Also, do _not_ include `/index.md` in the path, trim the path at the page directory. For example,  [connector page name](/docs/connectors/applications/some-connector) is correct, while [connector page name](/docs/connectors/applications/some-connector/index.md) is _incorrect_.}

---
name: {Item name in the left nav menu}
title: {SEO title}
description: {Meta Description}
hidden: {true or false. Set to true if the article should only be accessible via a direct link and not indexed by crawlers.}
---

# {Destination Name} Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="destination_name_destination" /%}

Follow our setup guide to connect {Destination} to Fivetran.

-----

## Prerequisites

To connect a {Destination} to Fivetran, you need the following:

{List what the user needs to know or do before they get started}

- A Fivetran role with the [Create Destinations or Manage Destinations](/docs/using-fivetran/fivetran-dashboard/account-settings/role-based-access-control#rbacpermissions) permissions
- Administrator account {this is an example}*
- Version 17 of Acme installed {this is an example}
    
    > \*NOTE: If you have an account tier system, this section should specify the minimum account tier required for your customers to set up a destination.

---

## Setup instructions

### <span class="step-item">{Descriptive name of the step; for example, Get your API key}</span>

1. Enumerate the steps.
2. List one action per step.
   {use screenshots when needed}

### <span class="step-item">Second step</span>

1. Enumerate the steps.
2. List one action per step.
   {use screenshots when needed}

### <span class="step-item"> Complete Fivetran configuration </span>

{Required}
1. Log in to your [Fivetran account](https://fivetran.com/login).
2. Go to the **Destinations** page and click **Add destination**.
3. Enter a **Destination name** of your choice and then click **Add**.
4. Select **<Destination>** as the destination type.
5. In the destination setup form, enter the **Host** name you found in [Step 1](/docs/destinations/).
6. Enumerate the steps in from the destination setup form.
7. List one action per step.
8. Click **Save & Test**.

   Fivetran [tests and validates](/docs/destinations/newdestination/setup-guide#setuptests) the <Destination> connection. Upon successfully completing the setup tests, you can sync your data using Fivetran connectors to the <Destination> destination.


### Setup tests

Fivetran performs the following {Destination} connection tests: <The following are examples>:

The Host Connection test checks the host's accessibility and validates the database credentials you provided in the setup form.
- The Validate Passphrase test validates your private key against the passphrase if you use key-pair authentication.
- The Default Warehouse test checks if the Snowflake warehouse exists and if you have set it as the default warehouse.
- The Database Connection test checks if we can connect to your Snowflake database.
  The Permission test checks whether we have the CREATE SCHEMA and CREATE TEMPORARY TABLES permissions on your Snowflake database.

  > NOTE: The tests may take a couple of minutes to complete.

---

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Destination Overview](/docs/destinations/newdestination)

<b> </b>

[<i aria-hidden="true" class="material-icons">assignment</i> Release Notes](/docs/destinations/newdestination/changelog)

<b> </b>

[<i aria-hidden="true" class="material-icons">settings</i> API Destination Configuration](/docs/rest-api/destinations/config#newdestination)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)

