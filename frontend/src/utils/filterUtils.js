import { canonicalizePlaceText, normalizePersonNode } from './normalization';
import { DEFAULT_BIRTH_RANGE, DEFAULT_DEATH_RANGE } from '../constants/defaults';
import { VOICE_TYPES, TYPE_FILTER_COLORS } from '../constants/voiceTypes';

// ---------------------------------------------------------------------------
// Pure helpers — no React state dependencies
// ---------------------------------------------------------------------------

export const normalizePlaceName = (value) => {
  const canonical = canonicalizePlaceText(value);
  return canonical ? canonical.toLowerCase() : '';
};

export const computeRangesFromNodes = (nodesList = []) => {
  const defaults = {
    birthRange: [...DEFAULT_BIRTH_RANGE],
    deathRange: [...DEFAULT_DEATH_RANGE],
  };
  const personNodes = Array.isArray(nodesList)
    ? nodesList
        .filter(node => node && node.type === 'person')
        .map(node => normalizePersonNode(node) ?? node)
    : [];

  if (personNodes.length === 0) return defaults;

  const birthYears = personNodes
    .map(node => node.birthYear)
    .filter(year => year && !isNaN(year))
    .map(year => parseInt(year, 10));

  const deathYears = personNodes
    .map(node => node.deathYear)
    .filter(year => year && !isNaN(year))
    .map(year => parseInt(year, 10));

  return {
    birthRange: birthYears.length > 0
      ? [Math.min(...birthYears), Math.max(...birthYears)]
      : defaults.birthRange,
    deathRange: deathYears.length > 0
      ? [Math.min(...deathYears), Math.max(...deathYears)]
      : defaults.deathRange,
  };
};

// ---------------------------------------------------------------------------
// Pure filter logic — each takes (node, filterState)
//
// filterState shape:
//   { selectedVoiceTypes, selectedBirthplaces,
//     birthYearRange, deathYearRange,
//     birthRangeIsUserSet, deathRangeIsUserSet }
// ---------------------------------------------------------------------------

export const isNodeVisibleWithoutVoiceFilter = (node, filterState) => {
  const {
    selectedBirthplaces, birthYearRange, deathYearRange,
    birthRangeIsUserSet, deathRangeIsUserSet,
  } = filterState;

  if (node.type === 'person') {
    const n = normalizePersonNode(node) ?? node;

    if (selectedBirthplaces.size > 0) {
      const props = (n && typeof n.properties === 'object') ? n.properties : null;
      const placeRaw =
        n.birthplace ??
        n.citizen ??
        (props && (
          props.birthplace ??
          props.citizen ??
          props.birth_place ??
          props.birthplace_display ??
          props.place_of_birth ??
          props.birthplace_text ??
          props.birth_location
        ));
      const place = canonicalizePlaceText(placeRaw);
      if (!place || !selectedBirthplaces.has(place.toLowerCase())) return false;
    }

    if (birthRangeIsUserSet && n.birthYear) {
      const birthYear = parseInt(n.birthYear);
      if (!isNaN(birthYear) && (birthYear < birthYearRange[0] || birthYear > birthYearRange[1])) return false;
    }

    if (deathRangeIsUserSet && n.deathYear) {
      const deathYear = parseInt(n.deathYear);
      if (!isNaN(deathYear) && (deathYear < deathYearRange[0] || deathYear > deathYearRange[1])) return false;
    }
  }
  return true;
};

export const isNodeVisibleWithoutBirthplaceFilter = (node, filterState) => {
  const {
    selectedVoiceTypes, birthYearRange, deathYearRange,
    birthRangeIsUserSet, deathRangeIsUserSet,
  } = filterState;

  if (node.type === 'person') {
    const n = normalizePersonNode(node) ?? node;

    if (selectedVoiceTypes.size > 0) {
      const voiceTypeMatch = !n.voiceType
        ? selectedVoiceTypes.has('Unknown')
        : selectedVoiceTypes.has(n.voiceType);
      if (!voiceTypeMatch) return false;
    }

    if (birthRangeIsUserSet && n.birthYear) {
      const birthYear = parseInt(n.birthYear);
      if (!isNaN(birthYear) && (birthYear < birthYearRange[0] || birthYear > birthYearRange[1])) return false;
    }

    if (deathRangeIsUserSet && n.deathYear) {
      const deathYear = parseInt(n.deathYear);
      if (!isNaN(deathYear) && (deathYear < deathYearRange[0] || deathYear > deathYearRange[1])) return false;
    }
  }
  return true;
};

export const isNodeVisible = (node, filterState) => {
  const {
    selectedVoiceTypes, selectedBirthplaces,
    birthYearRange, deathYearRange,
    birthRangeIsUserSet, deathRangeIsUserSet,
  } = filterState;

  if (node.type === 'person') {
    const n = normalizePersonNode(node) ?? node;

    if (selectedVoiceTypes.size > 0) {
      const rawVoice = n.voiceType && String(n.voiceType).trim();
      const voiceName = rawVoice && rawVoice.length > 0 ? rawVoice : 'Unknown';
      if (!selectedVoiceTypes.has(voiceName)) return false;
    }

    if (selectedBirthplaces.size > 0) {
      const place = n.birthplace || n.citizen || null;
      if (!place || !selectedBirthplaces.has(normalizePlaceName(place))) return false;
    }

    if (birthRangeIsUserSet && n.birthYear) {
      const birthYear = parseInt(n.birthYear);
      if (!isNaN(birthYear) && (birthYear < birthYearRange[0] || birthYear > birthYearRange[1])) return false;
    }

    if (deathRangeIsUserSet && n.deathYear) {
      const deathYear = parseInt(n.deathYear);
      if (!isNaN(deathYear) && (deathYear < deathYearRange[0] || deathYear > deathYearRange[1])) return false;
    }
  }

  if (selectedVoiceTypes.size > 0) {
    if (node.type === 'opera' && !selectedVoiceTypes.has('Opera')) return false;
    if (node.type === 'book' && !selectedVoiceTypes.has('Book')) return false;
  }

  if ((birthRangeIsUserSet || deathRangeIsUserSet || selectedBirthplaces.size > 0) && node.type !== 'person') {
    return false;
  }

  return true;
};

export const getNodeOpacity = (node, filterState) => {
  const {
    selectedVoiceTypes, selectedBirthplaces,
    birthYearRange, deathYearRange,
    birthRangeIsUserSet, deathRangeIsUserSet,
  } = filterState;

  if (selectedVoiceTypes.size > 0) {
    if (node.type === 'opera' && !selectedVoiceTypes.has('Opera')) return 0.2;
    if (node.type === 'book' && !selectedVoiceTypes.has('Book')) return 0.2;
    if (node.type === 'person') {
      const rawVoice = node.voiceType && String(node.voiceType).trim();
      const voiceName = rawVoice && rawVoice.length > 0 ? rawVoice : 'Unknown';
      if (!selectedVoiceTypes.has(voiceName)) return 0.2;
    }
  }

  if (selectedBirthplaces.size > 0 && node.type === 'person') {
    const place = node.birthplace || node.citizen || null;
    if (!place || !selectedBirthplaces.has(normalizePlaceName(place))) return 0.2;
  }

  if ((birthRangeIsUserSet || deathRangeIsUserSet || selectedBirthplaces.size > 0) && node.type !== 'person') {
    return 0.2;
  }

  if (node.type !== 'person') return 1;

  const n = normalizePersonNode(node) ?? node;
  let uncertain = false;

  if (birthRangeIsUserSet) {
    const birthYear = parseInt(n.birthYear);
    if (!Number.isNaN(birthYear)) {
      if (birthYear < birthYearRange[0] || birthYear > birthYearRange[1]) return 0.2;
    } else {
      uncertain = true;
    }
  }

  if (deathRangeIsUserSet) {
    const deathYear = parseInt(n.deathYear);
    if (!Number.isNaN(deathYear)) {
      if (deathYear < deathYearRange[0] || deathYear > deathYearRange[1]) return 0.2;
    } else {
      uncertain = true;
    }
  }

  return uncertain ? 0.5 : 1;
};

export const getVisibleVoiceTypes = (nodes, filterState) => {
  const counts = new Map();
  const colors = new Map();
  const voiceColorMap = {};
  VOICE_TYPES.forEach(v => { voiceColorMap[v.name] = v.color; });

  const register = (name, color) => {
    counts.set(name, (counts.get(name) || 0) + 1);
    if (color && !colors.has(name)) colors.set(name, color);
  };

  nodes
    .filter(node => node.type === 'person')
    .forEach(node => {
      if (!isNodeVisibleWithoutVoiceFilter(node, filterState)) return;
      const n = normalizePersonNode(node) ?? node;
      const vtRaw = n.voiceType && String(n.voiceType).trim();
      const voiceName = vtRaw && vtRaw.length > 0 ? vtRaw : 'Unknown';
      register(voiceName, voiceColorMap[voiceName] || '#6B7280');
    });

  nodes.forEach(node => {
    if (node.type === 'opera') register('Opera', TYPE_FILTER_COLORS.Opera);
    else if (node.type === 'book') register('Book', TYPE_FILTER_COLORS.Book);
  });

  return Array.from(counts.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, count]) => ({
      name,
      color: colors.get(name) || voiceColorMap[name] || '#6B7280',
      count,
    }));
};

export const getVisibleBirthplaces = (nodes, filterState) => {
  const counts = new Map();

  nodes
    .filter(node => node.type === 'person')
    .forEach(node => {
      if (!isNodeVisibleWithoutBirthplaceFilter(node, filterState)) return;
      const n = normalizePersonNode(node) ?? node;
      const props = (n && typeof n.properties === 'object') ? n.properties : null;
      const placeValue =
        n.birthplace ??
        n.citizen ??
        (props && (
          props.birthplace ??
          props.citizen ??
          props.birth_place ??
          props.birthplace_display ??
          props.place_of_birth ??
          props.birthplace_text ??
          props.birth_location
        ));
      const canonical = canonicalizePlaceText(placeValue);
      if (!canonical) return;
      const key = canonical.toLowerCase();
      if (!counts.has(key)) counts.set(key, { name: canonical, count: 0 });
      counts.get(key).count += 1;
    });

  return Array.from(counts.values()).sort((a, b) => a.name.localeCompare(b.name));
};
