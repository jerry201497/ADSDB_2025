# Trusted Zone – Data Quality Report (Template)

## Scope
- Image modality: chest X-ray PNGs
- Text modality: short clinical notes (anonymized)

## Checks Performed
- Resolution threshold: 256x256 +
- Duplicates (perceptual hash): removed
- Format homogenization: PNG (images), TXT (texts)
- Text cleaning: whitespace normalization
- Anonymization: heuristic removal of names and long numeric IDs

## Findings
- Total images scanned: <N>
- Low-res dropped: <N>
- Duplicates dropped: <N>
- Text files scanned: <N>
- Anonymized replacements: <N>

## Known Limitations
- Heuristic anonymization is not HIPAA-compliant
- Perceptual hashing can miss near-duplicates under heavy transforms
- No clinical labels; embeddings use self-supervised similarity only
