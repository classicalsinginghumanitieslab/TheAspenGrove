// Node identity, alias resolution, and graph node manipulation utilities.
// Pure functions — no React, no d3 dependencies.

export const toTitleCase = (str = '') =>
  str
    .split(' ')
    .filter(Boolean)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');

export const formatRelationshipTypeLabel = (value) => {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return '';
  const normalized = raw.toLowerCase().replace(/[\s_-]+/g, '');
  switch (normalized) {
    case 'parentof':
      return 'Child';
    case 'grandparentof':
      return 'Grandchild';
    case 'parent':
      return 'Parent';
    case 'grandparent':
      return 'Grandparent';
    case 'spouse':
    case 'spouseof':
      return 'Spouse';
    case 'sibling':
      return 'Sibling';
    default: {
      const withSpaces = raw
        .replace(/[_-]+/g, ' ')
        .replace(/([a-z])([A-Z])/g, '$1 $2')
        .replace(/\s+/g, ' ')
        .trim()
        .toLowerCase();
      return toTitleCase(withSpaces);
    }
  }
};

export const isPersonOperaPair = (typeA, typeB) => {
  const normalizedA = typeof typeA === 'string' ? typeA.trim().toLowerCase() : '';
  const normalizedB = typeof typeB === 'string' ? typeB.trim().toLowerCase() : '';
  if (!normalizedA || !normalizedB) return false;
  return (
    (normalizedA === 'person' && normalizedB === 'opera') ||
    (normalizedA === 'opera' && normalizedB === 'person')
  );
};

export const normalizeNodeId = (value) => {
  if (value === null || value === undefined) return '';
  return String(value)
    .normalize('NFKC')
    .replace(/[\u2010-\u2015\u2212\u2017\u00AD\u2011\uFE63\uFF0D]/g, '-') // unifies dash variants
    .replace(/[\u0000-\u001F\u007F]+/g, '')
    .replace(/\s+/g, ' ')
    .trim();
};

export const buildNodeAliasKey = (value, type = '') => {
  const normalized = normalizeNodeId(value);
  if (!normalized) return '';
  const prefix = type ? String(type).toLowerCase() : '';
  return `${prefix}::${normalized.toLowerCase()}`;
};

export const collectNodeAliasValues = (candidate) => {
  const values = new Set();
  if (!candidate || typeof candidate !== 'object') return values;
  const push = (val) => {
    const normalized = normalizeNodeId(val);
    if (normalized) values.add(normalized);
  };
  const possibleFields = [
    candidate.id,
    candidate.name,
    candidate.full_name,
    candidate.fullName,
    candidate.label,
    candidate.display_name,
    candidate.displayName,
    candidate.title
  ];
  possibleFields.forEach(push);
  if (candidate.properties && typeof candidate.properties === 'object') {
    const props = candidate.properties;
    [
      props.id,
      props.name,
      props.full_name,
      props.fullName,
      props.label,
      props.display_name,
      props.displayName,
      props.title
    ].forEach(push);
  }
  if (Array.isArray(candidate.aliases)) {
    candidate.aliases.forEach(push);
  }
  return values;
};

export const registerNodeAliases = (aliasMap, node) => {
  if (!aliasMap || !node) return;
  const canonicalId = normalizeNodeId(node.id ?? node.name);
  if (!canonicalId) return;
  const typeKey = (node.type || '').toLowerCase();
  const values = collectNodeAliasValues(node);
  values.add(canonicalId);
  values.forEach(value => {
    const keyWithType = buildNodeAliasKey(value, typeKey);
    if (keyWithType) aliasMap.set(keyWithType, canonicalId);
    const keyWithoutType = buildNodeAliasKey(value, '');
    if (keyWithoutType) aliasMap.set(keyWithoutType, canonicalId);
  });
};

export const resolveAliasIdFromMap = (aliasMap, candidate) => {
  if (!aliasMap || !candidate) return '';
  const typeKey = (candidate.type || '').toLowerCase();
  const values = Array.from(collectNodeAliasValues(candidate));
  if (candidate.id !== undefined) {
    const normalizedId = normalizeNodeId(candidate.id);
    if (normalizedId) values.unshift(normalizedId);
  }
  for (const value of values) {
    const keyWithType = buildNodeAliasKey(value, typeKey);
    if (keyWithType && aliasMap.has(keyWithType)) {
      return aliasMap.get(keyWithType);
    }
  }
  for (const value of values) {
    const keyWithoutType = buildNodeAliasKey(value, '');
    if (keyWithoutType && aliasMap.has(keyWithoutType)) {
      return aliasMap.get(keyWithoutType);
    }
  }
  return '';
};

export const OPERA_FORBIDDEN_FIELDS = [
  'voiceType',
  'birthYear',
  'deathYear',
  'birthplace',
  'spelling_source',
  'voice_type_source',
  'dates_source',
  'birthplace_source',
  'composer',
  'author',
  'role',
  'teacher_rel_source',
  'teacher_rel_source_text',
  'teacher_rel_source_url',
  'book_id',
  'title',
  'link',
  'source',
  'sourceInfo',
  'source_url',
  'opera_source_text',
  'opera_source_url'
];

export const BOOK_FORBIDDEN_FIELDS = [
  'voiceType',
  'birthYear',
  'deathYear',
  'birthplace',
  'spelling_source',
  'voice_type_source',
  'dates_source',
  'birthplace_source',
  'composer',
  'teacher_rel_source',
  'teacher_rel_source_text',
  'teacher_rel_source_url',
  'opera_id',
  'opera_name',
  'version',
  'book_type',
  'source',
  'sourceInfo',
  'source_url',
  'opera_source_text',
  'opera_source_url'
];

export const GRAPH_BASE_KEYS = new Set([
  'id',
  'name',
  'type',
  'x',
  'y',
  'vx',
  'vy',
  'fx',
  'fy',
  'index',
  'isCenter',
  'homeX',
  'homeY',
  'radius',
  'color',
  'opacity',
  'stroke',
  'strokeWidth',
  'selected',
  'highlighted',
  'hovered',
  'pinned',
  'locked',
  'dragging',
  'dragged',
  'layoutGroup',
  'clusterId',
  'pathIndex',
  'pathOrder',
  'pathCategory',
  'pathType',
  'pathGroup',
  'pathGroupKey',
  'pathGroupId',
  'pathSegment',
  'pathSource',
  'pathTarget',
  'pathSteps',
  'pathSequence',
  'pathLength',
  'pathWeight',
  'distance',
  'degree',
  'incomingDegree',
  'outgoingDegree',
  'counts',
  'meta',
  'searchMeta',
  'appliedFilters',
  'previewClass',
  'historyKey',
  'historyLabel',
  'historySnapshot',
  'historyTimestamp',
  'legendKey',
  'legendColor',
  'labelX',
  'labelY',
  'labelAngle',
  'labelOffset',
  'labelLines',
  'renderHint',
  'z',
  'layer',
  'scale',
  'size',
  'icon',
  'image',
  'avatar',
  'badge',
  'category',
  'group',
  'subgroup',
  'timeline',
  'timelineOrder',
  'timelineGroup',
  'timelineLabel',
  'timelineTimestamp',
  'frozen',
  'frozenDuringDrag',
  '_frozenDuringDrag',
  'homeTheta',
  'homeRadius',
  'homeZ',
  'renderCache'
]);

export const stripOperaBookFields = (node) => {
  if (!node || typeof node !== 'object') return node;
  if (node.type === 'opera') {
    OPERA_FORBIDDEN_FIELDS.forEach(field => {
      if (field in node) delete node[field];
    });
    if (node.version !== undefined && node.version !== null) {
      const versionStr = String(node.version).trim();
      if (versionStr) {
        node.version = versionStr;
      } else {
        delete node.version;
      }
    }
    if (!node.opera_name && node.name) {
      node.opera_name = node.name;
    }
    if (node.bookId !== undefined) delete node.bookId;
  } else if (node.type === 'book') {
    BOOK_FORBIDDEN_FIELDS.forEach(field => {
      if (field in node) delete node[field];
    });
    if (!node.title && node.name) {
      node.title = node.name;
    }
    if (node.bookId !== undefined) {
      node.book_id = normalizeNodeId(node.bookId);
      delete node.bookId;
    }
    if (node.book_id) {
      node.book_id = normalizeNodeId(node.book_id);
    }
    if (node.link && typeof node.link === 'string') {
      node.link = node.link.trim();
    }
    if (node.author !== undefined) delete node.author;
  }
  return node;
};

export const copyGraphBaseProps = (source, target) => {
  if (!source || !target) return;
  GRAPH_BASE_KEYS.forEach((key) => {
    if (key === 'id' || key === 'name' || key === 'type') return;
    if (source[key] !== undefined) {
      target[key] = source[key];
    }
  });
  Object.keys(source || {}).forEach((key) => {
    if (GRAPH_BASE_KEYS.has(key)) return;
    if (key.startsWith('_')) {
      target[key] = source[key];
    }
  });
};
