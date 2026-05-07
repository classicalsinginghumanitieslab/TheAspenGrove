// Canonical lists of field name variants for relationship source data.
//
// The underlying data was imported from multiple sources over time, so the same
// concept (e.g. "where did this relationship come from?") appears under many
// different property names. These arrays establish the lookup priority order
// used throughout the app when extracting source text/URLs from raw API data.

// Ordered list of property names to probe when looking for a relationship's
// source text on a link or node object.
export const RELATIONSHIP_SOURCE_FIELDS = [
  'relationshipSourceDisplay',
  'relationship_source_display',
  'teacher_rel_source',
  'relationship_rel_source',
  'relationship_source',
  'relationshipSource',
  'sourceInfo',
  'source',
  'relSource',
  'reference_source',
  'referenceSource',
  'citation',
  'notes',
  'text',
  'label'
];

// Per-category field name variants for node-level source attribution.
// Used by the CSV export to resolve provenance fields from person nodes.
export const SOURCE_FIELD_KEYS = {
  spelling: [
    'spelling_source_url',
    'spellingSourceUrl',
    'spelling_source_text',
    'spellingSourceText',
    'spelling_source',
    'spellingSource'
  ],
  voiceType: [
    'voice_type_source_url',
    'voiceType_source_url',
    'voiceTypeSourceUrl',
    'voice_type_source_text',
    'voiceType_source_text',
    'voiceTypeSourceText',
    'voice_type_source',
    'voiceType_source',
    'voiceTypeSource'
  ],
  dates: [
    'dates_source_url',
    'datesSourceUrl',
    'dates_source_text',
    'datesSourceText',
    'dates_source',
    'datesSource'
  ],
  birthplace: [
    'birthplace_source_url',
    'birthplaceSourceUrl',
    'birthplace_source_text',
    'birthplaceSourceText',
    'birthplace_source',
    'birthplaceSource'
  ]
};
