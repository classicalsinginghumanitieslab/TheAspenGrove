// Pure functions for constructing node payloads and resolving node identity.
// No React, no D3 dependencies.

import { normalizeNodeId, stripOperaBookFields, copyGraphBaseProps } from './nodeUtils';

export const parseTypedId = (value) => {
  const normalized = normalizeNodeId(value);
  if (!normalized) return { type: '', value: '' };
  const colonIndex = normalized.indexOf(':');
  if (colonIndex === -1) {
    return { type: '', value: normalized };
  }
  const type = normalized.slice(0, colonIndex).toLowerCase();
  const rawValue = normalized.slice(colonIndex + 1).trim();
  return { type, value: rawValue };
};

export const createOperaNodePayload = (input = {}) => {
  const node = {
    type: 'opera'
  };
  copyGraphBaseProps(input, node);
  if (input.id !== undefined) node.id = input.id;
  if (input.name !== undefined) node.name = input.name;
  if (input.isCenter !== undefined) node.isCenter = input.isCenter;
  const typed = parseTypedId(input.id ?? input.opera_id ?? input.operaId ?? '');
  const explicitOperaId = input.opera_id ?? input.operaId ?? (typed.type === 'opera' ? typed.value : '');
  if (explicitOperaId) node.opera_id = normalizeNodeId(explicitOperaId);
  const candidateName = input.opera_name ?? input.operaName ?? input.title ?? node.name;
  if (candidateName) node.opera_name = String(candidateName).trim() || node.name;
  if (!node.name && node.opera_name) {
    node.name = node.opera_name;
  }
  const versionCandidate = input.version ?? input.opera_version ?? (input.opera && input.opera.version);
  if (versionCandidate !== undefined && versionCandidate !== null) {
    const versionStr = String(versionCandidate).trim();
    if (versionStr) node.version = versionStr;
  }
  stripOperaBookFields(node);
  return node;
};

export const createBookNodePayload = (input = {}) => {
  const node = {
    type: 'book'
  };
  copyGraphBaseProps(input, node);
  if (input.id !== undefined) node.id = input.id;
  if (input.name !== undefined) node.name = input.name;
  if (input.isCenter !== undefined) node.isCenter = input.isCenter;
  const typed = parseTypedId(input.id ?? input.book_id ?? input.bookId ?? '');
  const explicitBookId = input.book_id ?? input.bookId ?? (typed.type === 'book' ? typed.value : '');
  if (explicitBookId) node.book_id = normalizeNodeId(explicitBookId);
  const titleCandidate = input.title ?? input.book_title ?? input.name;
  if (titleCandidate) node.title = String(titleCandidate).trim() || input.name;
  if (!node.name && node.title) {
    node.name = node.title;
  }
  if (input.link) {
    node.link = String(input.link).trim();
  } else if (input.url) {
    node.link = String(input.url).trim();
  } else if (input.href) {
    node.link = String(input.href).trim();
  }
  stripOperaBookFields(node);
  return node;
};

export const resolveLinkEndpointId = (endpoint) => {
  if (endpoint === null || endpoint === undefined) return '';
  if (typeof endpoint === 'string') return normalizeNodeId(endpoint);
  if (typeof endpoint === 'object') {
    if (endpoint.id !== undefined && endpoint.id !== null) return normalizeNodeId(endpoint.id);
    if (endpoint.name !== undefined && endpoint.name !== null) return normalizeNodeId(endpoint.name);
  }
  return normalizeNodeId(endpoint);
};

export const isPlaceholderName = (value) => {
  const normalized = normalizeNodeId(value);
  if (!normalized) return true;
  const simplified = normalized
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!simplified) return true;
  if (simplified === 'unknown') return true;
  if (simplified.includes('unknown opera')) return true;
  if (simplified.includes('unknown book')) return true;
  if (simplified.startsWith('unknown ')) return true;
  if (
    simplified.includes('teacher provided') ||
    simplified.includes('teacherprovided')
  ) return true;
  if (
    simplified.includes('student provided') ||
    simplified.includes('studentprovided')
  ) return true;
  return false;
};
