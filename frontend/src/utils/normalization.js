// Data normalization utilities shared across the app.
// Pure functions — no React, no d3 dependencies.

import { RELATIONSHIP_SOURCE_FIELDS } from '../constants/fieldNames';

export const parseYearValue = (value) => {
  if (value === null || value === undefined) return NaN;
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : NaN;
  }
  if (typeof value === 'string') {
    const match = value.match(/-?\d{3,4}/);
    return match ? parseInt(match[0], 10) : NaN;
  }
  if (typeof value === 'object') {
    if (Number.isFinite(value.year)) return value.year;
    if (typeof value.year === 'string') {
      const match = value.year.match(/-?\d{3,4}/);
      if (match) return parseInt(match[0], 10);
    }
    if (Number.isFinite(value.low)) return value.low;
    if (Number.isFinite(value.high)) return value.high;
  }
  return NaN;
};

export const normalizePersonNode = (node) => {
  if (!node || node.type !== 'person') return node;
  let normalized = node;
  const ensureClone = () => {
    if (normalized === node) normalized = { ...node };
  };
  const props = (normalized.properties && typeof normalized.properties === 'object')
    ? normalized.properties : null;
  const firstDefined = (...values) => {
    for (const value of values) {
      if (value !== null && value !== undefined) return value;
    }
    return null;
  };

  const currentVoice = extractTextValue(normalized.voiceType);
  if (currentVoice && currentVoice !== normalized.voiceType) {
    ensureClone();
    normalized.voiceType = currentVoice;
  } else if (!currentVoice) {
    const voiceCandidate = firstDefined(
      normalized.voice_type,
      props && props.voiceType,
      props && props.voice_type,
      props && props.voice_type_label,
      props && props.voice
    );
    const voiceText = extractTextValue(voiceCandidate);
    if (voiceText) { ensureClone(); normalized.voiceType = voiceText; }
  }

  const existingBirthplace = canonicalizePlaceText(normalized.birthplace);
  const birthplaceCandidate = firstDefined(
    normalized.birth_place, normalized.citizen,
    props && props.birthplace, props && props.birth_place,
    props && props.birthplace_display, props && props.birthplace_label,
    props && props.place_of_birth, props && props.birthplace_text,
    props && props.birth_location, props && props.citizen
  );
  const birthplaceText = existingBirthplace || canonicalizePlaceText(birthplaceCandidate);
  if (birthplaceText && birthplaceText !== normalized.birthplace) {
    ensureClone(); normalized.birthplace = birthplaceText;
  }
  const citizenCandidate = firstDefined(normalized.citizen, props && props.citizen);
  const citizenText = canonicalizePlaceText(citizenCandidate);
  if (citizenText && citizenText !== normalized.citizen) {
    ensureClone(); normalized.citizen = citizenText;
  }

  const birthCandidate = firstDefined(
    normalized.birthYear, normalized.birth_year, normalized.birth,
    props && props.birthYear, props && props.birth_year, props && props.birth,
    props && props.birthDate, props && props.birth_date, props && props.date_of_birth,
    props && props.birthyear, props && props.birth_low, props && props.birth_high
  );
  const deathCandidate = firstDefined(
    normalized.deathYear, normalized.death_year, normalized.death,
    props && props.deathYear, props && props.death_year, props && props.death,
    props && props.deathDate, props && props.death_date, props && props.date_of_death,
    props && props.deathyear, props && props.death_low, props && props.death_high
  );
  const birthYear = parseYearValue(birthCandidate);
  const deathYear = parseYearValue(deathCandidate);
  if (Number.isFinite(birthYear) && normalized.birthYear !== birthYear) {
    ensureClone(); normalized.birthYear = birthYear;
  }
  if (Number.isFinite(deathYear) && normalized.deathYear !== deathYear) {
    ensureClone(); normalized.deathYear = deathYear;
  }
  return normalized;
};

export const normalizeSourceValue = (value) => {
  if (value == null) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    const str = String(value).trim();
    return str;
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      const candidate = normalizeSourceValue(entry);
      if (candidate) return candidate;
    }
    return '';
  }
  if (typeof value === 'object') {
    for (const key of RELATIONSHIP_SOURCE_FIELDS) {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        const candidate = normalizeSourceValue(value[key]);
        if (candidate) return candidate;
      }
    }
    for (const candidate of Object.values(value)) {
      const text = normalizeSourceValue(candidate);
      if (text) return text;
    }
    return '';
  }
  return '';
};

export const extractTextValue = (value) => {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    const text = String(value).trim();
    return text;
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      const candidate = extractTextValue(entry);
      if (candidate) return candidate;
    }
    return '';
  }
  if (typeof value === 'object') {
    const priorityKeys = [
      'text',
      'label',
      'name',
      'display_name',
      'displayName',
      'title',
      'value',
      'place',
      'location',
      'city',
      'country'
    ];
    for (const key of priorityKeys) {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        const candidate = extractTextValue(value[key]);
        if (candidate) return candidate;
      }
    }
    const normalized = normalizeSourceValue(value);
    return typeof normalized === 'string' ? normalized.trim() : '';
  }
  return '';
};

export const collapsePlaceWhitespace = (value) =>
  value.replace(/[\u00a0\u1680\u2000-\u200A\u202F\u205F\u3000]+/g, ' ').replace(/\s+/g, ' ');

export const canonicalizePlaceText = (value) => {
  const text = extractTextValue(value);
  if (!text) return '';
  const normalized = text
    .normalize('NFKC')
    .replace(/[\u200b-\u200d\u2060]/g, '') // zero-width characters
    .replace(/[\u202a-\u202e\u2066-\u2069]/g, '') // directional controls
    .replace(/['\u2019]/g, '\'')
    .replace(/[\u201c\u201d]/g, '"');
  const collapsed = collapsePlaceWhitespace(normalized).trim();
  return collapsed;
};

export const deriveRelationshipSourceText = (...values) => {
  for (const value of values) {
    const candidate = normalizeSourceValue(value);
    if (candidate) return candidate;
  }
  return '';
};

export const deriveOperaName = (opera = {}, fallback = 'Unknown Opera') => {
  if (opera === null || opera === undefined) return fallback;

  const collectCandidates = (entry, seen = new Set()) => {
    if (!entry || typeof entry !== 'object' || seen.has(entry)) return [];
    seen.add(entry);
    const directValues = [
      entry.opera_name,
      entry.operaName,
      entry.operaTitle,
      entry.opera_title,
      entry.title,
      entry.name,
      entry.label,
      entry.display_name,
      entry.displayName
    ];
    const nestedValues = [];
    if (entry.properties && typeof entry.properties === 'object') {
      nestedValues.push(...collectCandidates(entry.properties, seen));
    }
    if (entry.opera && typeof entry.opera === 'object') {
      nestedValues.push(...collectCandidates(entry.opera, seen));
    }
    if (entry.allOperaProps && typeof entry.allOperaProps === 'object') {
      nestedValues.push(...collectCandidates(entry.allOperaProps, seen));
    }
    return [...directValues, ...nestedValues];
  };

  const candidateFields = collectCandidates(opera);
  for (const field of candidateFields) {
    if (field === null || field === undefined) continue;
    const trimmed = String(field).trim();
    if (trimmed && !/^unknown\b/i.test(trimmed)) return trimmed;
  }

  const version = typeof opera.version === 'string' ? opera.version.trim() : '';
  const operaId = opera.opera_id ?? opera.id;
  if (version) {
    return version;
  }
  if (operaId !== null && operaId !== undefined) {
    const idString = String(operaId).trim();
    if (idString) {
      return `Opera ${idString}`;
    }
  }
  return fallback;
};

export const normalizeLinks = (links = []) =>
  (Array.isArray(links) ? links : []).map((link) => {
    const sourceText = deriveRelationshipSourceText(
      link.relationshipSourceDisplay,
      link.relationship_source_display,
      link.teacher_rel_source,
      link.teacher_rel_source_text,
      link.relationship_source,
      link.relationshipSource,
      link.opera_source_text,
      link.sourceInfo,
      link.source
    );
    return {
      ...link,
      relationshipSourceDisplay: sourceText,
      sourceInfo: sourceText
    };
  });

export const normalizeDetailsRelationshipSources = (details = {}) => {
  const clone = { ...details };
  const toTrimmedString = (value) => {
    if (value == null) return '';
    if (typeof value === 'string') return value.trim();
    if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
    return '';
  };
  const derivePrimaryName = (entry) => {
    const candidateKeys = [
      'full_name',
      'name',
      'title',
      'opera_name',
      'author',
      'editor',
      'singer',
      'role',
      'book_title'
    ];
    for (const key of candidateKeys) {
      const value = toTrimmedString(entry?.[key]);
      if (value) return value;
    }
    return '';
  };
  const deriveLastNameKey = (entry) => {
    const explicit = toTrimmedString(
      entry?.last_name ||
      entry?.surname ||
      entry?.family_name ||
      entry?.author_last_name ||
      entry?.editor_last_name
    );
    if (explicit) return explicit.toLowerCase();
    const primary = derivePrimaryName(entry);
    if (!primary) return '';
    const parts = primary.split(/\s+/).filter(Boolean);
    if (parts.length === 0) return '';
    return parts[parts.length - 1].toLowerCase();
  };
  const deriveFullNameKey = (entry) => {
    const primary = derivePrimaryName(entry);
    return primary ? primary.toLowerCase() : '';
  };
  const extractSortKey = (entry) => {
    if (!entry || typeof entry !== 'object') return '';
    const lastKey = deriveLastNameKey(entry);
    const fullKey = deriveFullNameKey(entry);
    return `${lastKey}|${fullKey}`;
  };
  const sortList = (list) => {
    if (!Array.isArray(list)) return [];
    return [...list].sort((a, b) => {
      const aKey = extractSortKey(a);
      const bKey = extractSortKey(b);
      return aKey.localeCompare(bKey);
    });
  };
  const normalizeList = (list, priorityFields = [], options = {}) => {
    const { allowFallback = true } = options;
    if (!Array.isArray(list)) return [];
    return list.map((entry) => {
      const teacherRaw = entry?.teacher_rel_source;
      const normalizedTeacherValue = (() => {
        if (teacherRaw == null) return '';
        if (typeof teacherRaw === 'string') return teacherRaw.trim();
        return deriveRelationshipSourceText(teacherRaw);
      })();
      const prioritizedValues = priorityFields
        .map((key) => entry?.[key])
        .filter((val) => val != null && val !== '');
      const fallbackValues = allowFallback
        ? [
            entry.relationshipSourceDisplay,
            entry.relationship_source_display,
            entry.teacher_rel_source,
            entry.relationship_source,
            entry.relationshipSource,
            entry.source
          ]
        : [];
      const sourceText = deriveRelationshipSourceText(
        ...prioritizedValues,
        ...fallbackValues
      );
      const normalizedEntry = {
        ...entry,
        relationshipSourceDisplay: sourceText,
      };
      if (priorityFields.includes('teacher_rel_source_text')) {
        const normalizedText = deriveRelationshipSourceText(entry?.teacher_rel_source_text);
        normalizedEntry.teacher_rel_source_text = normalizedText || sourceText || null;
      }
      if (priorityFields.includes('opera_source_text')) {
        const normalizedOperaText = deriveRelationshipSourceText(entry?.opera_source_text);
        normalizedEntry.opera_source_text = normalizedOperaText || sourceText || null;
      }
      if (priorityFields.includes('teacher_rel_source')) {
        const normalizedTeacher = normalizedTeacherValue || deriveRelationshipSourceText(...prioritizedValues);
        normalizedEntry.teacher_rel_source = normalizedTeacher || null;
        normalizedEntry.teacher_rel_source_display = normalizedTeacher || null;
        if (!normalizedEntry.teacher_rel_source_text && normalizedTeacher) {
          normalizedEntry.teacher_rel_source_text = normalizedTeacher;
        }
      }
      if (priorityFields.includes('source')) {
        const normalizedSource = deriveRelationshipSourceText(...prioritizedValues);
        normalizedEntry.source = normalizedSource || null;
      }
      return normalizedEntry;
    });
  };

  clone.teachers = sortList(normalizeList(clone.teachers, ['teacher_rel_source_text', 'teacher_rel_source'], { allowFallback: false }));
  clone.students = sortList(normalizeList(clone.students, ['teacher_rel_source_text', 'teacher_rel_source'], { allowFallback: false }));
  clone.family = sortList(normalizeList(clone.family, ['teacher_rel_source_text', 'teacher_rel_source'], { allowFallback: false }));
  clone.premieredRoles = sortList(normalizeList(clone.premieredRoles, ['opera_source_text', 'source']));

  if (clone.works && typeof clone.works === 'object') {
    const worksClone = { ...clone.works };
    worksClone.operas = sortList(normalizeList(worksClone.operas, ['opera_source_text', 'source']));
    worksClone.books = sortList(normalizeList(worksClone.books, []));
    worksClone.composedOperas = sortList(normalizeList(worksClone.composedOperas, ['opera_source_text', 'source']));
    clone.works = worksClone;
  }

  clone.authors = sortList(normalizeList(clone.authors, ['source']));
  clone.editors = sortList(normalizeList(clone.editors, ['source']));

  return clone;
};
