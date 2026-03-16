# Lead Engine OS

Internal lead ingestion, normalization, scoring, and dialer-export system.

## What it does

Lead Engine OS turns messy raw lead files into structured, campaign-ready call lists.

### Core pipeline

Import  
→ Schema detection  
→ Strict normalization  
→ Reject / trash handling  
→ Clean pool  
→ Completion scoring  
→ Vertical-specific filtering  
→ Dialer-ready export

## Current product vision

This system is designed as an internal operating tool for lead generation and lead sales.

It supports:

- ingestion of raw lead files from multiple sources
- strict normalization into one uniform structure
- reject handling via trash pile
- a clean central pool of usable contacts
- campaign / vertical-specific list building
- CloudTalk-ready exports
- future affiliator attribution

## Canonical contact model

Every usable contact should normalize into the same structure:

- first_name
- last_name
- phone
- email
- address
- city
- state
- zip
- dob
- age
- source
- campaign
- completeness_score
- vertical_score
- status
- affiliator_id

## System layers

### 1. Ingestion
Uploads raw CSV / spreadsheet lead files into the system.

### 2. Normalization
Maps inconsistent fields into one canonical schema.

### 3. Trash pile
Invalid or unusable rows are separated from the ecosystem and can be reviewed, restored, or purged.

### 4. Pool
Clean records move into the pool, where every contact follows the same rules and structure.

### 5. Scoring
Current scoring is based primarily on data completeness.
Later scoring can include vertical fit and intent signals.

### 6. Vertical builder
Users choose a vertical first, then build custom call lists from the pool.

Initial verticals:
- Debt Settlement
- Tax Relief
- Final Expense
- Mortgage Protection
- Medicare
- Home Improvement

### 7. Export
Creates dialer-ready files, including CloudTalk-compatible lists.

## Affiliate / contributor model

The system is architected to support outside contributors later through an `affiliator_id` field.

For now:
- affiliate mode is conceptually OFF
- all leads are treated as internal unless otherwise assigned
- future contributor data can be ingested, traced, and attributed when sold

## Positioning

This is not a generic CSV cleaner.

This is a lead operating system:
- ingesting raw records
- enforcing structure
- separating trash from usable contacts
- building vertical-specific campaign lists
- exporting dialer-ready lead batches

## Notes

This repository intentionally excludes live lead data, exports, and databases.
Only application code and system structure belong in version control.
