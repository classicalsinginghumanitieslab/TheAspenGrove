// Voice type definitions with their display colors.
// Used by: the network visualization (node coloring), FilterPanel, and HelpCenter color key.

export const VOICE_TYPES = [
  // Traditional Female Voices
  { name: 'Soprano', color: '#ae996b' }, // tree trunk
  { name: 'Mezzo-soprano', color: '#695531' }, // brown trunk
  { name: 'Contralto', color: '#443f39' }, // knot

  // Traditional Male Voices
  { name: 'Countertenor', color: '#4e2d06' }, // Brown
  { name: 'Tenor', color: '#e4a201' }, // yellow leaf
  { name: 'Baritone', color: '#6a7304' }, // darker green leaf
  { name: 'Bass-baritone', color: '#a09602' }, // lighter green leaf
  { name: 'Bass', color: '#a09602' }, // dark green-grey

  // Historical/Specialized Voices
  { name: 'Castrato', color: '#99c0e3' }, // Pale blue
  { name: 'Soprano castrato', color: '#99c0e3' }, // Pale blue
  { name: 'Alto castrato', color: '#99c0e3' }, // Pale blue
  { name: 'Haute-contre', color: '#99c0e3' }, // Pale blue
  { name: 'Treble, unchanged voice', color: '#99c0e3' }, // Pale blue

  // Professional Roles - Music
  { name: 'Composer', color: '#7c8b23' },
  { name: 'Conductor', color: '#7c8b23' },
  { name: 'Instrumentalist', color: '#7c8b23' },
  { name: 'Opera director', color: '#7c8b23' },

  // Professional Roles - Education
  { name: 'Teacher, other', color: '#7c8b23' },
  { name: 'Vocal coach', color: '#7c8b23' },
  { name: 'Speech Language Pathologist', color: '#7c8b23' },

  // Professional Roles - Literary/Creative
  { name: 'Librettist', color: '#7c8b23' },
  { name: 'Critic', color: '#7c8b23' },
  { name: 'Actor', color: '#7c8b23' },
  { name: 'Inventor', color: '#7c8b23' },

  // Other/Special Categories
  { name: 'Non-singing', color: '#7c8b23' },
  { name: 'Unknown', color: '#7c8b23' }
];

// Colors for non-person node types in the graph.
export const TYPE_FILTER_COLORS = {
  Opera: '#8b5cf6',
  Book: '#14b8a6'
};
