# Chrome Bookmarks Extractor v1

A SAFE community scroll that extracts bookmarks from Google Chrome.

## What it does
- Reads Chrome's `Bookmarks` JSON file from the default macOS location
- Extracts title, URL, domain, folder hierarchy, and timestamps
- Writes structured JSONL entries to the vault's `Bookmarks/` directory

## Tier: SAFE
- No network access
- No dependencies
- Read-only filesystem access (Chrome's bookmarks file)
- Zero manual steps required

## Quality Metrics
- **Speed**: Processes thousands of bookmarks in <1 second
- **Human Annoyance**: 0 manual steps
- **Data Richness**: 9 fields per record including folder hierarchy
- **Storage**: ~200 bytes per bookmark
