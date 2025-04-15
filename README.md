# Fivetran Partner SDK
A way for partners to create source and destination connectors that run on [Fivetran’s platform](https://www.fivetran.com/). Read more in our [product docs](https://fivetran.com/docs/partner-built-program). 

## Repo Structure
This repo consists of example source and destination connectors along with a local testing environment. Both connectors are written in a [gRPC supported language](https://grpc.io/docs/languages/) that can generate a statically linked binary. We recommend Java, Golang, or Rust. 
* [Examples](examples)
* [Local Testing Tools](tools/)

## Development
Follow the [SDK Development Guide](development-guide.md) for guidance on how to develop your code. 

Once you have your code ready to run:
1. Start up your connector running on port 50051 (for destination code, use port 50052).
2. Run the local test environment.

Our READMEs for [source connectors](tools/source-connector-tester/) and [destination connectors](tools/destination-connector-tester/) will take you through setting up the local test environment. 

> NOTE: To run an example connector from the `examples` directory, follow the README specific to the example. 

### Other Examples
You can look at the following public repos to see how other partners have built their connectors:
#### Connectors
* [PlanetScale](https://github.com/planetscale/fivetran-source)
* [Convex](https://github.com/get-convex/convex-backend/tree/main/crates/fivetran_source)
#### Destinations
* [Materialize](https://github.com/MaterializeInc/materialize/tree/main/src/fivetran-destination)
* [Motherduck](https://github.com/MotherDuck-Open-Source/motherduck-fivetran-connector)
* [Clickhouse](https://github.com/ClickHouse/clickhouse-fivetran-destination)

## Partner Process

### How does it work?
You build a source or destination connector using our SDK in this repo. We’ll deploy it to our platform, and customers can discover it via the Fivetran website, dashboard, and product documentation. We’ll tag it with a `partner-built` designation and explain the support process to customers. 

### How do we deploy?
Right now, we are employing manual processes before getting into automation:
1. You put your code in a public GitHub repo or provide a couple of our team members access to your private repo. 
2. We pull the code, inspect it, and build the executable binary to deploy to our platform. Allow up to a week for us to complete our review and deploy process.

### How do we test?
We expect testing to ensure your source or destination connector is working as expected, either with test data or with a customer who is eager to adopt. We provide [testers](tools/) in this repo you can use locally without connecting to Fivetran platform. We also recommend you think through typical scenarios for testing:

* Handle all data type mappings (between Fivetran data types and source/destination data types)
* Big data loads
* Big incremental updates
* Narrow event tables
* Wide fact tables

### How do we collaborate on support?
Once we get customers using your source or destination connector, they can reach out to either Fivetran or you via support portals. Whoever receives a support ticket triages it and either fixes the issue and emails the customer back or passes it to the other side for handling.

Our response times are articulated in our [Fivetran Support Policy](https://support.fivetran.com/hc/en-us/articles/5893119459223-Fivetran-Support-Policy) documentation. Bear in mind customers may expect your support response times to be similar. We recommend linking to your own SLAs/Support policies from the documentation you create for Fivetran. 

### How do we create product documentation?
All Fivetran connectors have public documentation. For partner-built connectors, we will need you to produce the following:
* Setup Guide [example](https://fivetran.com/docs/databases/cosmos/setup-guide)
* Overview Page [example](https://fivetran.com/docs/databases/cosmos)

To help you produce the documentation, we provide helpful resources:
* [Source connector templates](doc-templates/source-connector-templates/)
* [Destination connector templates](doc-templates/destination-connector-templates/)
* [Fivetran's documentation style guide](https://github.com/fivetran/fivetran_sdk/tree/main/doc-templates/style-guide/style-guide.md)
* [Fivetran-approved glossary of common terms](https://github.com/fivetran/fivetran_sdk/tree/main/doc-templates/style-guide/common-terms-glossary.md)

We’ll review these docs and our Tech Writing team will edit for grammar and style, sending back to your team (TBD on automation) to keep in sync. The documentation is to be hosted in [Fivetran's documentation site](https://fivetran.com/docs/getting-started).

### How do we go-to-market?
Fivetran will market through multiple methods:
* Connector release blogs
* Monthly email updates

Your connector will appear in the following places:
* On our website in our connector directory
* In our dashboard connector list/search
* In our public documentation

### I am sold! How do I get started?
Reach out to us via this [intake form](https://docs.google.com/forms/d/e/1FAIpQLScyxlu4Lhm_P4WfTit-WM_PazbFmZ1YBHDCeFXvu0O_5sA45w/viewform) and we'll go from there.
The process typically looks like this:
1. Explore this repo.
2. Meet with us to go over the Partnership Agreement.
3. Build the connector (source, destination, or both).
4. Test it locally.
5. Create product documentation (Overview and Setup Guide pages as Markdown files)
6. Deploy to production on our platform.
7. Test in production.
8. Move to [Private Preview](https://fivetran.com/docs/core-concepts#releasephases).
9. Handle support:
    * Share an email address we can send customer related issues to
    * Share a link to your SLAs
10. Go-to-market with us.

### How do I get help?
After you meet with us, we will create a shared Slack channel where we can assist you.
