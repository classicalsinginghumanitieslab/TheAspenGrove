import React, { useState, useEffect, useRef, useLayoutEffect, Suspense, useCallback } from 'react';
import * as d3 from 'd3';
import useViewport from './useViewport';
import useDebounce from './useDebounce';
import initTouchInteractions from './touchInteractions';
const HelpCenter = React.lazy(() => import('./HelpCenter'));

// Global console helpers (defined at module load) to avoid undefined in console
if (typeof window !== 'undefined') {
  try {
    window.__CMG_DEBUG_EVENTS = window.__CMG_DEBUG_EVENTS || [];
    if (typeof window.__CMG_dumpLongEdges !== 'function') {
      window.__CMG_dumpLongEdges = (threshold = (window.__CMG_LONG_EDGE_THRESHOLD || 260)) => {
        const snap = window.__CMG_lastNetwork || window.__cmg_lastNetwork || {};
        const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
        const links = Array.isArray(snap.links) ? snap.links : [];
        const idTo = new Map(nodes.map(n => [n.id, n]));
        const rows = [];
        links.forEach(l => {
          const s = (typeof l.source === 'string' ? idTo.get(l.source) : idTo.get(l?.source?.id) || l.source);
          const t = (typeof l.target === 'string' ? idTo.get(l.target) : idTo.get(l?.target?.id) || l.target);
          if (!s || !t) return;
          const len = Math.hypot((Number(t.x)||0)-(Number(s.x)||0), (Number(t.y)||0)-(Number(s.y)||0));
          if (Number.isFinite(len) && len >= threshold) rows.push({ s: s.id, t: t.id, type: l.type, len: Math.round(len) });
        });
        rows.sort((a,b) => b.len - a.len);
        try { console.table(rows.slice(0, 50)); } catch (_) { console.log(rows.slice(0, 50)); }
        return rows;
      };
    }
    if (typeof window.__CMG_findNodeIdByName !== 'function') {
      window.__CMG_findNodeIdByName = (name) => {
        const snap = window.__CMG_lastNetwork || window.__cmg_lastNetwork || {};
        const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
        const nm = String(name || '').trim().toLowerCase();
        const n = nodes.find(x => String(x?.name||'').toLowerCase() === nm);
        return n ? n.id : null;
      };
    }
  } catch (_) {}
}

const SESSION_SNAPSHOT_KEY = 'cmgActiveSession_v1';
const SESSION_SNAPSHOT_FILTERLESS_KEY = 'cmgActiveSession_filtersReset';
const TOKEN_LOGIN_TS_KEY = 'cmgTokenLoginTs';
const LOGIN_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;

const RELATIONSHIP_SOURCE_FIELDS = [
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

const normalizeSourceValue = (value) => {
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

const extractTextValue = (value) => {
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

const collapsePlaceWhitespace = (value) =>
  value.replace(/[\u00a0\u1680\u2000-\u200A\u202F\u205F\u3000]+/g, ' ').replace(/\s+/g, ' ');

const canonicalizePlaceText = (value) => {
  const text = extractTextValue(value);
  if (!text) return '';
  const normalized = text
    .normalize('NFKC')
    .replace(/[\u200b-\u200d\u2060]/g, '') // zero-width characters
    .replace(/[\u202a-\u202e\u2066-\u2069]/g, '') // directional controls
    .replace(/[’]/g, '\'')
    .replace(/[“”]/g, '"');
  const collapsed = collapsePlaceWhitespace(normalized).trim();
  return collapsed;
};

const deriveRelationshipSourceText = (...values) => {
  for (const value of values) {
    const candidate = normalizeSourceValue(value);
    if (candidate) return candidate;
  }
  return '';
};

const URL_DETECT_REGEX = /https?:\/\/[^\s)]+/i;
const TRAILING_PUNCTUATION_REGEX = /[),.;:]+$/g;
const WWW_URL_REGEX = /^www\.[^\s)]+/i;
// Basic bare-domain detector (example.com, sub.example.co.uk, with optional path)
const DOMAIN_ONLY_REGEX = /^(?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/[^[\s)]]*)?$/i;
// Detect bare domain occurrence inside text (avoid matching within words by requiring start or whitespace/paren)
const DOMAIN_DETECT_REGEX = /(?:^|[\s(])((?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/[^[\s)]]*)?)/i;
const isDebugRelSourcesEnabled = () =>
  typeof window !== 'undefined' && window.__CMG_DEBUG_REL_SOURCES === true;
const isProbablyHttpUrl = (value) => {
  if (value == null) return false;
  if (typeof value !== 'string') return false;
  return /^https?:\/\//i.test(value.trim());
};
const sanitizeUrlCandidate = (value) => {
  if (value == null) return '';
  const str = String(value).trim();
  if (!str) return '';
  const match = str.match(URL_DETECT_REGEX);
  if (match && match[0]) {
    return match[0].replace(TRAILING_PUNCTUATION_REGEX, '');
  }
  if (WWW_URL_REGEX.test(str)) {
    return `https://${str.replace(TRAILING_PUNCTUATION_REGEX, '')}`;
  }
  // Bare domain like example.com or sub.example.org/abc → prefix https://
  const trimmed = str.replace(TRAILING_PUNCTUATION_REGEX, '');
  if (DOMAIN_ONLY_REGEX.test(trimmed)) {
    return `https://${trimmed}`;
  }
  return '';
};

const extractFirstUrlFromValue = (value) => {
  if (value == null) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return sanitizeUrlCandidate(value);
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = extractFirstUrlFromValue(entry);
      if (found) return found;
    }
    return '';
  }
  if (typeof value === 'object') {
    for (const entry of Object.values(value)) {
      const found = extractFirstUrlFromValue(entry);
      if (found) return found;
    }
    return '';
  }
  return '';
};

const deriveRelationshipSourceUrl = (...values) => {
  for (const value of values) {
    const possibleUrl = extractFirstUrlFromValue(value);
    if (possibleUrl) return possibleUrl;
  }
  return '';
};

const deriveOperaName = (opera = {}, fallback = 'Unknown Opera') => {
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

const SOURCE_LINK_STYLE = {
  color: '#2563eb',
  textDecoration: 'underline',
  overflowWrap: 'anywhere',
  wordBreak: 'break-word',
  display: 'inline-block'
};

const DEFAULT_BIRTH_RANGE = [1534, 2005];
const DEFAULT_DEATH_RANGE = [1575, 2025];

const renderRelationshipSourceLink = (...values) => {
  const findExplicitPair = () => {
    for (const entry of values) {
      if (!entry || typeof entry !== 'object') continue;
      const textKeys = [
        'text',
        'label',
        'title',
        'relationshipSourceDisplay',
        'teacher_rel_source_text'
      ];
      const urlKeys = [
        'url',
        'href',
        'link',
        'sourceUrl',
        'teacher_rel_source_url'
      ];

      let textCandidate = '';
      for (const key of textKeys) {
        if (entry[key] !== undefined) {
          textCandidate = normalizeSourceValue(entry[key]);
          if (textCandidate) break;
        }
      }

      let urlCandidate = '';
      for (const key of urlKeys) {
        if (entry[key] !== undefined) {
          const extracted = extractFirstUrlFromValue(entry[key]);
          if (extracted) {
            urlCandidate = extracted;
            break;
          }
          if (typeof entry[key] === 'string') {
            const trimmed = entry[key].trim();
            if (trimmed) {
              urlCandidate = trimmed;
              break;
            }
          }
        }
      }

      if (textCandidate || urlCandidate) {
        return { text: textCandidate, url: urlCandidate };
      }
    }
    return null;
  };

  const explicitPair = findExplicitPair();
  const stopPropagation = (event) => {
    if (event && typeof event.stopPropagation === 'function') {
      event.stopPropagation();
    }
  };
  const linkEventHandlers = {
    onMouseDown: stopPropagation,
    onMouseUp: stopPropagation,
    onClick: stopPropagation,
    onPointerDown: stopPropagation,
    onPointerUp: stopPropagation
  };
  const collectCandidateUrls = () => {
    const candidates = [];
    const pushCandidate = (candidate) => {
      if (!candidate) return;
      const extracted = extractFirstUrlFromValue(candidate);
      if (extracted) {
        candidates.push(extracted);
        return;
      }
      if (typeof candidate === 'string') {
        const trimmed = candidate.trim();
        if (trimmed) {
          candidates.push(trimmed);
        }
      }
    };

    if (explicitPair?.url) {
      pushCandidate(explicitPair.url);
    }

    for (const value of values) {
      pushCandidate(value);
      if (value && typeof value === 'object') {
        const secondaryUrl =
          value.url ??
          value.href ??
          value.link ??
          value.sourceUrl ??
          value.teacher_rel_source_url;
        if (secondaryUrl) {
          pushCandidate(secondaryUrl);
        }
      }
    }
    return candidates;
  };

  const urlCandidates = collectCandidateUrls();
  const url = urlCandidates.find(isProbablyHttpUrl) || '';
  const raw = explicitPair?.text ?? deriveRelationshipSourceText(...values);
  const text = typeof raw === 'string' ? raw.trim() : raw != null ? String(raw).trim() : '';
  const textContainsUrl = URL_DETECT_REGEX.test(text || '') || DOMAIN_DETECT_REGEX.test(text || '');
  if (isDebugRelSourcesEnabled()) {
    const debugEntry = {
      values,
      explicitPair,
      urlCandidates,
      url,
      text,
      textContainsUrl
    };
    if (typeof window !== 'undefined') {
      window.__CMG_REL_SOURCE_LOGS = window.__CMG_REL_SOURCE_LOGS || [];
      window.__CMG_REL_SOURCE_LOGS.push(debugEntry);
    }
    try {
      // eslint-disable-next-line no-console
      console.debug('[cmg] Relationship source render', debugEntry);
    } catch (_) {}
  }

  if (url) {
    let display = text || url;
    if (textContainsUrl && text) {
      const strippedVariants = [url, url.replace(/^https?:\/\//i, '')];
      let cleaned = text;
      strippedVariants.forEach((variant) => {
        if (!variant) return;
        const escaped = variant.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        cleaned = cleaned.replace(new RegExp(escaped, 'gi'), '').trim();
      });
      if (cleaned) {
        display = cleaned;
      }
    }
    return (
      <a
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        style={SOURCE_LINK_STYLE}
        onMouseOver={(e) => {
          if (e?.target) e.target.style.color = '#1d4ed8';
        }}
        onMouseOut={(e) => {
          if (e?.target) e.target.style.color = '#2563eb';
        }}
        {...linkEventHandlers}
      >
        {display}
      </a>
    );
  }

  if (!text) return null;

  // Match either explicit http(s) URLs or bare domains
  const urlRegex = /(https?:\/\/[^\s)]+)|((?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/[^[\s)]]*)?)/gi;
  const nodes = [];
  let lastIndex = 0;
  let match;

  while ((match = urlRegex.exec(text)) !== null) {
    const { index } = match;
    if (index > lastIndex) {
      nodes.push(text.slice(lastIndex, index));
    }
    const matchedText = match[1] || match[2];
    const cleaned = (matchedText || '').replace(TRAILING_PUNCTUATION_REGEX, '');
    const needsScheme = !/^https?:\/\//i.test(cleaned);
    const url = needsScheme ? `https://${cleaned}` : cleaned;
    const trailing = matchedText.slice(url.length);
    const anchor = (
      <a
        key={`${url}-${index}`}
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        style={SOURCE_LINK_STYLE}
        onMouseOver={(e) => {
          if (e?.target) e.target.style.color = '#1d4ed8';
        }}
        onMouseOut={(e) => {
          if (e?.target) e.target.style.color = '#2563eb';
        }}
        {...linkEventHandlers}
      >
        {url}
      </a>
    );
    nodes.push(anchor);
    if (trailing) {
      nodes.push(trailing);
    }
    lastIndex = index + matchedText.length;
  }

  if (lastIndex < text.length) {
    nodes.push(text.slice(lastIndex));
  }

  if (nodes.length === 0) {
    return text;
  }

  return nodes.map((node, idx) =>
    typeof node === 'string'
      ? <React.Fragment key={`text-${idx}`}>{node}</React.Fragment>
      : node
  );
};

const renderPathPanelContent = ({
  isMobile,
  pathFromRef,
  pathToRef,
  pathFromValRef,
  pathToValRef,
  pathInfo,
  pathListRef,
  handleClearPath,
  onFindPath,
  renderRelationshipSourceLink,
  onClose
}) => {
  const showInputs = !isMobile || !pathInfo;
  return (
    <>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <div style={{ fontWeight: 600, color: '#1f2937' }}>Find path</div>
        <button
          onClick={onClose}
          style={{
            background: 'none',
            border: 'none',
            fontSize: 18,
            cursor: 'pointer',
            color: '#666',
            padding: isMobile ? '6px' : 0
          }}
        >
          ×
        </button>
      </div>
      {showInputs && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 8 }}>
          <div>
            <label style={{ display: 'block', fontSize: 12, color: '#6b7280', marginBottom: 4 }}>From (Person full name)</label>
            <input
              ref={pathFromRef}
              defaultValue=""
              onInput={(e) => { pathFromValRef.current = e.target.value; }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  e.stopPropagation();
                  onFindPath();
                }
              }}
              autoComplete="off"
              autoCorrect="off"
              autoCapitalize="off"
              spellCheck={false}
              inputMode="text"
              name="cmg-path-from"
              data-lpignore="true"
              data-1p-ignore
              style={{
                width: '100%',
                padding: isMobile ? '12px 14px' : '6px 8px',
                border: '2px solid #3e96e2',
                borderRadius: isMobile ? 12 : 4,
                fontSize: isMobile ? '16px' : '14px'
              }}
            />
          </div>
          <div>
            <label style={{ display: 'block', fontSize: 12, color: '#6b7280', marginBottom: 4 }}>To (Person full name)</label>
            <input
              ref={pathToRef}
              defaultValue=""
              onInput={(e) => { pathToValRef.current = e.target.value; }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  e.stopPropagation();
                  onFindPath();
                }
              }}
              autoComplete="off"
              autoCorrect="off"
              autoCapitalize="off"
              spellCheck={false}
              inputMode="text"
              name="cmg-path-to"
              data-lpignore="true"
              data-1p-ignore
              style={{
                width: '100%',
                padding: isMobile ? '12px 14px' : '6px 8px',
                border: '2px solid #3e96e2',
                borderRadius: isMobile ? 12 : 4,
                fontSize: isMobile ? '16px' : '14px'
              }}
            />
          </div>
          <div style={{ display: 'flex', gap: 8, flexDirection: isMobile ? 'column' : 'row' }}>
            <button
              onClick={onFindPath}
              style={{
                backgroundColor: '#2563eb',
                color: 'white',
                border: '2px solid #3e96e2',
                padding: isMobile ? '12px 16px' : '6px 10px',
                borderRadius: isMobile ? 12 : 4,
                cursor: 'pointer',
                flex: isMobile ? 1 : 'initial',
                fontSize: isMobile ? '16px' : '14px'
              }}
            >
              Find path
            </button>
            {!isMobile && (
              <button
                aria-label="Clear path"
                title="Clear path"
                onClick={handleClearPath}
                style={{
                  backgroundColor: '#f9fafb',
                  color: '#111827',
                  border: '2px solid #3e96e2',
                  padding: '6px 10px',
                  borderRadius: 4,
                  cursor: 'pointer',
                  flex: 'initial',
                  fontSize: '14px'
                }}
              >
                Clear path
              </button>
            )}
          </div>
        </div>
      )}
      {pathInfo && (
        <div
          ref={pathListRef}
          style={{
            marginTop: 10,
            maxHeight: 200,
            overflowY: 'auto',
            fontSize: 12,
            color: '#374151',
            borderTop: '1px solid #eee',
            paddingTop: 8,
            overscrollBehavior: 'contain',
            WebkitOverflowScrolling: 'touch'
          }}
          onWheelCapture={(e) => {
            e.stopPropagation();
          }}
          onMouseEnter={() => {
            try { window.__cmg_disableZoomWhileScrolling && window.__cmg_disableZoomWhileScrolling(true); } catch (_) {}
          }}
          onMouseLeave={() => {
            try { window.__cmg_disableZoomWhileScrolling && window.__cmg_disableZoomWhileScrolling(false); } catch (_) {}
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Path summary</div>
          <div>Nodes: {pathInfo.nodes.length}, Links: {pathInfo.links.length}</div>
          {Array.isArray(pathInfo.steps) && pathInfo.steps.length > 0 && (
            <div style={{ marginTop: 8 }}>
              {pathInfo.steps.map((step, idx) => {
                const hasSource = !!(step.relationshipSourceDisplay || step.sourceInfo || step.sourceUrl);
                return (
                  <div
                    key={idx}
                    style={{
                      padding: '6px 8px',
                      borderRadius: 4,
                      marginBottom: 4,
                      border: '2px solid #3e96e2',
                      background: '#fff',
                      cursor: 'default'
                    }}
                  >
                    <div>
                      <strong>{step.source?.name || step.source?.id}</strong> — {step.label}
                      {step.type === 'premiered' && step.role && (
                        <> (Role: {step.role})</>
                      )}
                      {' '}→ <strong>{step.target?.name || step.target?.id}</strong>
                    </div>
                    {hasSource && (
                      <div style={{ marginTop: 4 }}>
                        <strong>Source:</strong>{' '}
                        {renderRelationshipSourceLink(
                          step.relationshipSourceDisplay,
                          step.sourceUrl,
                          step.sourceInfo
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
          {isMobile && (
            <button
              aria-label="Clear path"
              title="Clear path"
              onClick={handleClearPath}
              style={{
                marginTop: 12,
                width: '100%',
                backgroundColor: '#f9fafb',
                color: '#111827',
                border: '2px solid #3e96e2',
                padding: '12px 16px',
                borderRadius: 12,
                cursor: 'pointer',
                fontSize: '16px'
              }}
            >
              Clear path
            </button>
          )}
        </div>
      )}
    </>
  );
};

const normalizeLinks = (links = []) =>
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

const computeCenteredTransform = (
  nodes = [],
  containerWidth = 0,
  containerHeight = 0,
  padding = 80
) => {
  const positionedNodes = Array.isArray(nodes)
    ? nodes.filter((node) => Number.isFinite(node?.x) && Number.isFinite(node?.y))
    : [];
  const width = Math.max(1, Number(containerWidth) || 0);
  const height = Math.max(1, Number(containerHeight) || 0);
  if (!positionedNodes.length || width <= 0 || height <= 0) return null;
  const xs = positionedNodes.map((node) => node.x);
  const ys = positionedNodes.map((node) => node.y);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const paddingValue = Math.max(0, Number(padding) || 0);
  const paddedWidth = Math.max(1, (maxX - minX) + paddingValue * 2);
  const paddedHeight = Math.max(1, (maxY - minY) + paddingValue * 2);
  const scaleX = width / paddedWidth;
  const scaleY = height / paddedHeight;
  const scale = Math.min(Math.max(Math.min(scaleX, scaleY), 0.1), 4);
  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;
  return d3.zoomIdentity
    .translate(width / 2, height / 2)
    .scale(scale)
    .translate(-centerX, -centerY);
};

// Debug logging utilities for layout issues
const isLayoutDebug = () => (typeof window !== 'undefined' && window.__CMG_DEBUG_LAYOUT === true);
const debugLog = (kind, data) => {
  try {
    if (typeof window !== 'undefined') {
      window.__CMG_DEBUG_EVENTS = window.__CMG_DEBUG_EVENTS || [];
      window.__CMG_DEBUG_EVENTS.push({ t: Date.now(), kind, data });
    }
    if (isLayoutDebug()) {
      // eslint-disable-next-line no-console
      console.debug(`[cmg-layout] ${kind}`, data);
    }
  } catch (_) {}
};

// Compute the bounding box and centroid of currently positioned nodes
const computeGraphBBox = (nodes = []) => {
  const positioned = (Array.isArray(nodes) ? nodes : []).filter(
    (n) => Number.isFinite(n?.x) && Number.isFinite(n?.y)
  );
  if (positioned.length === 0) {
    return {
      minX: 0,
      maxX: 0,
      minY: 0,
      maxY: 0,
      cx: 0,
      cy: 0,
      width: 0,
      height: 0,
      radius: 0
    };
  }
  const xs = positioned.map((n) => n.x);
  const ys = positioned.map((n) => n.y);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const width = Math.max(0, maxX - minX);
  const height = Math.max(0, maxY - minY);
  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  const radius = Math.max(width, height) / 2;
  return { minX, maxX, minY, maxY, width, height, cx, cy, radius };
};

// Stable pseudo-random angle from a string key
const stableAngleFromString = (key = '') => {
  const s = String(key || '0');
  let h = 0;
  for (let i = 0; i < s.length; i += 1) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  const frac = (h % 3600) / 10; // 0..360
  return (frac * Math.PI) / 180;
};

// Choose a spawn center outside the current bbox along the vector from centroid -> anchor
const computeSpawnOutsideBBox = (nodes = [], anchor = { x: 0, y: 0, key: '' }, margin = 280, bounds) => {
  const { cx, cy, radius } = computeGraphBBox(nodes);
  let dx = (Number(anchor?.x) || 0) - cx;
  let dy = (Number(anchor?.y) || 0) - cy;
  const len = Math.hypot(dx, dy);
  if (!Number.isFinite(dx) || !Number.isFinite(dy) || len < 1e-3) {
    const ang = stableAngleFromString(anchor?.key || `${Date.now()}`);
    dx = Math.cos(ang);
    dy = Math.sin(ang);
  } else {
    dx /= len; dy /= len;
  }
  const dist = (Number.isFinite(radius) ? radius : 0) + Math.max(160, Number(margin) || 0);
  let x = cx + dx * dist;
  let y = cy + dy * dist;
  // Optionally clamp into visible bounds to avoid being marked as out-of-bounds & re-positioned
  if (bounds && Number.isFinite(bounds.width) && Number.isFinite(bounds.height)) {
    const pad = Number.isFinite(bounds.pad) ? bounds.pad : 50;
    const w = Math.max(200, bounds.width);
    const h = Math.max(200, bounds.height);
    x = Math.max(pad, Math.min(w - pad, x));
    y = Math.max(pad, Math.min(h - pad, y));
  }
  return { x, y };
};

// Compute a reasonable ring radius for placing a cluster of `count` nodes
// Attempts to keep edges visually compact while avoiding label/shape overlap
const computeRingRadius = (count, minR = 110, maxR = 220, spacing = 88) => {
  const n = Math.max(1, Number(count) || 0);
  // Spacing-based radius so circumference roughly fits `n` nodes with diameter+gap ~ spacing
  const spacingBased = (n * spacing) / (2 * Math.PI);
  // Gentle linear growth to avoid too small for modest n
  const seed = 90 + n * 8;
  const proposed = Math.max(spacingBased, seed);
  const capped = (Number.isFinite(maxR) && maxR > 0) ? Math.min(maxR, proposed) : proposed;
  return Math.max(minR, capped);
};

const getExpansionRingConfig = (count) => {
  const n = Math.max(1, Number(count) || 0);
  const win = (typeof window !== 'undefined') ? window : {};
  const overrideMin = Number.isFinite(win?.__CMG_EXPAND_RING_MIN) ? Number(win.__CMG_EXPAND_RING_MIN) : null;
  const overrideSpacing = Number.isFinite(win?.__CMG_EXPAND_RING_SPACING) ? Number(win.__CMG_EXPAND_RING_SPACING) : null;
  let overrideMax = null;
  if (win && typeof win.__CMG_EXPAND_RING_MAX === 'function') {
    try { overrideMax = win.__CMG_EXPAND_RING_MAX(n); } catch (_) { overrideMax = null; }
  } else if (Number.isFinite(win?.__CMG_EXPAND_RING_MAX)) {
    overrideMax = Number(win.__CMG_EXPAND_RING_MAX);
  }

  const min = Math.max(100, overrideMin ?? 180);
  const spacing = Math.max(60, overrideSpacing ?? 120);
  const defaultMax = 280 + n * 12;
  const computedMax = Number.isFinite(overrideMax) ? overrideMax : defaultMax;
  const max = Math.max(min + 40, computedMax);

  return { min, max, spacing };
};

const toTitleCase = (str = '') =>
  str
    .split(' ')
    .filter(Boolean)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');

const formatRelationshipTypeLabel = (value) => {
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

const isPersonOperaPair = (typeA, typeB) => {
  const normalizedA = typeof typeA === 'string' ? typeA.trim().toLowerCase() : '';
  const normalizedB = typeof typeB === 'string' ? typeB.trim().toLowerCase() : '';
  if (!normalizedA || !normalizedB) return false;
  return (
    (normalizedA === 'person' && normalizedB === 'opera') ||
    (normalizedA === 'opera' && normalizedB === 'person')
  );
};

const createLinkContextMenuState = () => ({
  show: false,
  x: 0,
  y: 0,
  role: '',
  sourceValues: [],
  sourceText: '',
  sourceUrl: ''
});

const buildLinkContextSource = (link) => {
  if (!link) {
    return {
      sourceValues: [],
      sourceText: '',
      sourceUrl: '',
      baseValues: []
    };
  }

  const baseValues = [
    link.teacher_rel_source_text,
    link.relationshipSourceDisplay,
    link.relationship_source_display,
    link.sourceInfo,
    link.teacher_rel_source,
    link.relationship_source,
    link.relationshipSource,
    link.source,
    link.meta?.source,
    link.opera_source_text,
    link.opera_source_url,
    link.teacher_rel_source_url,
    link.sourceUrl,
    link.meta?.sourceUrl
  ];

  const derivedSourceText = deriveRelationshipSourceText(...baseValues);
  const derivedSourceUrl = deriveRelationshipSourceUrl(
    link.teacher_rel_source_url,
    link.sourceUrl,
    ...baseValues
  );

  const sourceValues = [
    { text: derivedSourceText, url: derivedSourceUrl },
    { text: link.teacher_rel_source_text, url: link.teacher_rel_source_url },
    { text: link.opera_source_text, url: link.opera_source_url },
    ...baseValues
  ];

  return {
    sourceValues,
    sourceText: derivedSourceText,
    sourceUrl: derivedSourceUrl,
    baseValues
  };
};

const normalizeDetailsRelationshipSources = (details = {}) => {
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

const ClassicalMusicGenealogy = () => {
  // Minimal ToS/Disclaimer route to support Auth0 Post-Login Redirect Action
  const isDisclaimerRoute = (typeof window !== 'undefined') && (window.location.pathname.replace(/\/+$/, '') === '/disclaimer');
  if (isDisclaimerRoute) {
    const AUTH0_DOMAIN = (import.meta && import.meta.env && import.meta.env.VITE_AUTH0_DOMAIN) || '';
    let state = '';
    let redirectToken = '';
    let nonce = '';
    try {
      const params = new URLSearchParams(window.location.search || '');
      state = params.get('state') || '';
      redirectToken = params.get('redirect_token') || '';
      nonce = params.get('nonce') || '';
    } catch (_) {}
    const canContinue = Boolean(AUTH0_DOMAIN && state);
    const continueAction = canContinue ? `https://${AUTH0_DOMAIN}/continue` : '';
    const containerStyle = {
      minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
      background: '#f3f4f6', padding: 16
    };
    const cardStyle = {
      maxWidth: 820, width: '100%', background: '#fff', border: '1px solid #e5e7eb',
      borderRadius: 12, padding: 24, boxShadow: '0 10px 24px rgba(15, 23, 42, 0.12)'
    };
    return (
      <div style={containerStyle}>
        <div style={cardStyle}>
          <h1 style={{ marginTop: 0, marginBottom: 10 }}>The Aspen Grove of Opera Singers, Disclaimer</h1>
          <p style={{ color: '#374151' }}>
            Please review and accept this disclaimer to continue. By selecting Agree & Continue you
            acknowledge the extent of the site's current contents and the limitations expressed in this disclaimer.
          </p>
          {!AUTH0_DOMAIN && (
            <div style={{ background: '#fff7ed', border: '1px solid #fdba74', color: '#9a3412', padding: 10, borderRadius: 8, marginBottom: 12 }}>
              Missing VITE_AUTH0_DOMAIN. Add your Auth0 tenant domain (e.g., dev-xxxxx.us.auth0.com) to frontend/.env.
            </div>
          )}
          {!state && (
            <div style={{ background: '#fef2f2', border: '1px solid #fecaca', color: '#991b1b', padding: 10, borderRadius: 8, marginBottom: 12 }}>
              This page must be opened from the Auth0 login redirect. Missing state.
            </div>
          )}
          <div style={{ display: 'flex', gap: 12, marginTop: 16 }}>
            <form action={continueAction} method="GET" style={{ margin: 0 }}>
              <input type="hidden" name="state" value={state} />
              <input type="hidden" name="accepted" value="1" />
              {redirectToken ? (<input type="hidden" name="redirect_token" value={redirectToken} />) : null}
              {nonce ? (<input type="hidden" name="nonce" value={nonce} />) : null}
              <button
                type="submit"
                disabled={!canContinue}
                style={{
                  background: canContinue ? '#2563eb' : '#93c5fd', color: '#fff', border: 'none', padding: '10px 16px',
                  borderRadius: 8, fontWeight: 600, cursor: canContinue ? 'pointer' : 'not-allowed'
                }}
              >
                Agree & Continue
              </button>
            </form>
            <button
              type="button"
              onClick={() => { try { window.history.back(); } catch (_) {} }}
              style={{ background: '#fff', color: '#111', border: '1px solid #d1d5db', padding: '10px 16px', borderRadius: 8, fontWeight: 600 }}
            >
              Go Back
            </button>
          </div>
        </div>
      </div>
    );
  }
const viewport = useViewport();
const { width: viewportWidth, height: viewportHeight, isTablet, isPhone } = viewport;
const isMobileViewport = !!isPhone;
const viewportIsPhone = !!isPhone;
const viewportIsTablet = !!isTablet;
const isHeaderMobile = !!isPhone || (viewportWidth > 0 && viewportWidth <= 600);
  const debouncedViewportHeight = useDebounce(viewportHeight, 150);
  // Stabilize mobile viewport height to avoid background jumps when browser chrome shows/hides
  useEffect(() => {
    try {
      const setStableVh = () => {
        const vh = Math.max(320, Math.floor((window.innerHeight || 0))) * 0.01; // floor for stability
        document.documentElement.style.setProperty('--cmg-vh', `${vh}px`);
      };
      setStableVh();
      window.addEventListener('resize', setStableVh, { passive: true });
      window.addEventListener('orientationchange', setStableVh, { passive: true });
      return () => {
        window.removeEventListener('resize', setStableVh);
        window.removeEventListener('orientationchange', setStableVh);
      };
    } catch (_) {}
  }, []);
  const backgroundMinHeight = isMobileViewport ? 'calc(var(--cmg-vh, 1vh) * 100)' : '100vh';
  const backgroundAttachmentMode = isMobileViewport ? 'fixed' : 'fixed';

  const [token, setToken] = useState('');
  const [userEmail, setUserEmail] = useState('');
  const [initialResetToken, setInitialResetToken] = useState('');
  const [pendingTosToken, setPendingTosToken] = useState('');
  const [pendingTosEmail, setPendingTosEmail] = useState('');
  const [pendingTosRedirect, setPendingTosRedirect] = useState('');
  const [currentView, setCurrentView] = useState('search');
  const [searchType, setSearchType] = useState('singers');
  const [originalSearchType, setOriginalSearchType] = useState('singers');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [originalSearchResults, setOriginalSearchResults] = useState([]);
  const [hasExecutedSearch, setHasExecutedSearch] = useState(false);
  const [selectedItem, setSelectedItem] = useState(null);
  const [itemDetails, setItemDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [networkData, setNetworkData] = useState({ nodes: [], links: [] });
  useEffect(() => {
    try {
      if (typeof window !== 'undefined') {
        // Update both legacy and new snapshots for console helpers
        window.__cmg_lastNetwork = networkData;
        try {
          window.__CMG_lastNetwork = {
            nodes: Array.isArray(networkData?.nodes) ? networkData.nodes.map(n => ({ ...n })) : [],
            links: Array.isArray(networkData?.links) ? networkData.links.map(l => ({ ...l })) : []
          };
        } catch (_) {}
        window.dumpLatestNetwork = (options = {}) => {
          const graph = window.__cmg_lastNetwork;
          if (!graph || !Array.isArray(graph?.nodes) || !Array.isArray(graph?.links)) {
            console.warn('[cmg-debug] No network captured yet.');
            return null;
          }
          const includeNodes = options.includeNodes !== false;
          const includeLinks = options.includeLinks !== false;
          console.log(
            `[cmg-debug] Latest network snapshot: ${graph.nodes.length} nodes, ${graph.links.length} links`
          );
          const summary = {
            nodeCount: graph.nodes.length,
            linkCount: graph.links.length
          };
          if (includeNodes) {
            const nodeSample = graph.nodes.slice(0, 50).map(node => ({
              id: node?.id ?? '',
              name: node?.name ?? '',
              type: node?.type ?? '',
              x: Number.isFinite(node?.x) ? Math.round(node.x) : null,
              y: Number.isFinite(node?.y) ? Math.round(node.y) : null
            }));
            console.table(nodeSample);
            summary.nodes = nodeSample;
          }
          if (includeLinks) {
            const linkSample = graph.links.slice(0, 100).map(link => ({
              source: resolveLinkEndpointId(link?.source),
              target: resolveLinkEndpointId(link?.target),
              type: link?.type ?? '',
              label: link?.label ?? link?.relationshipSourceDisplay ?? '',
              role: link?.role ?? ''
            }));
            console.table(linkSample);
            summary.links = linkSample;
          }
          return summary;
        };
        window.dumpNetworkOnError = () => {
          const snapshot = window.__cmg_lastNetworkOnError;
          if (!snapshot) {
            console.warn('[cmg-debug] No error snapshot captured.');
            return null;
          }
          console.log(
            `[cmg-debug] Last error snapshot: ${snapshot.nodes?.length || 0} nodes, ${snapshot.links?.length || 0} links`
          );
          window.__cmg_lastNetwork = snapshot;
          return window.dumpLatestNetwork();
        };
      }
    } catch (_) {}
  }, [networkData]);
  useEffect(() => {
    try {
      const nodes = Array.isArray(networkData?.nodes) ? networkData.nodes : [];
      const placeholders = nodes
        .filter(node => isPlaceholderName(node?.id ?? node?.name))
        .map(node => node?.id ?? node?.name);
      if (placeholders.length > 0) {
        console.warn('[cmg-debug] Placeholder nodes detected after sanitization', placeholders);
      }
      const links = Array.isArray(networkData?.links) ? networkData.links : [];
      const badLinks = [];
      links.forEach(link => {
        const sourceId = resolveLinkEndpointId(link?.source);
        const targetId = resolveLinkEndpointId(link?.target);
        if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) {
          badLinks.push({ sourceId, targetId, type: link?.type });
        }
      });
      if (badLinks.length > 0) {
        console.warn('[cmg-debug] Links referencing placeholder endpoints detected', badLinks);
      }
    } catch (_) {}
  }, [networkData]);
  const [shouldRunSimulation, setShouldRunSimulation] = useState(false);
  const [contextMenu, setContextMenu] = useState({ show: false, x: 0, y: 0, node: null });
  const [linkContextMenu, setLinkContextMenu] = useState(createLinkContextMenuState);
  const [visualizationHeight, setVisualizationHeight] = useState(550);
  // Visualization height adapts for smaller viewports; defaults to 550px on desktop
  const [selectedNode, setSelectedNode] = useState(null);
  // Align Saved view token + Open with Save/Export + Logout
  const saveExportBtnRef = useRef(null);
  const logoutBtnRef = useRef(null);
  const openBtnBelowRef = useRef(null);
  const [savedInputBelowWidth, setSavedInputBelowWidth] = useState(160);
  const [rightGroupWidthPx, setRightGroupWidthPx] = useState(0);
  const [expandSubmenu, setExpandSubmenu] = useState(null);
  const [profileCard, setProfileCard] = useState({ show: false, data: null });
  const relationshipCountsUnavailableRef = useRef(false);
  const pathApiUnavailableRef = useRef(false);
  const [actualCounts, setActualCounts] = useState({});
  const [fetchingCounts, setFetchingCounts] = useState({});
  const [failedFetches, setFailedFetches] = useState({}); // Track nodes that failed to fetch
  const [currentCenterNode, setCurrentCenterNode] = useState(null); // Track current center to prevent re-triggering
  const [isExpansionSimulation, setIsExpansionSimulation] = useState(false); // Track if simulation is for expansion
  const [filtersVersion, setFiltersVersion] = useState(0); // Bump to force viz refresh on Apply
  const [showFilterPanel, setShowFilterPanel] = useState(false); // Control filter panel visibility
  const toggleFilterPanel = (force) => {
    setShowFilterPanel((prev) => (typeof force === 'boolean' ? force : !prev));
  };
  const [selectedVoiceTypes, setSelectedVoiceTypes] = useState(new Set()); // Selected voice type filters
  const [selectedBirthplaces, setSelectedBirthplaces] = useState(new Set()); // Selected birthplace filters
  const [birthYearRange, setBirthYearRange] = useState([...DEFAULT_BIRTH_RANGE]); // Birth year range filter
  const [deathYearRange, setDeathYearRange] = useState([...DEFAULT_DEATH_RANGE]); // Death year range filter
  const [birthRangeIsUserSet, setBirthRangeIsUserSet] = useState(false);
  const [deathRangeIsUserSet, setDeathRangeIsUserSet] = useState(false);
  const birthRangeIsUserSetRef = useRef(birthRangeIsUserSet);
  const deathRangeIsUserSetRef = useRef(deathRangeIsUserSet);
  useEffect(() => {
    birthRangeIsUserSetRef.current = birthRangeIsUserSet;
  }, [birthRangeIsUserSet]);
  useEffect(() => {
    deathRangeIsUserSetRef.current = deathRangeIsUserSet;
  }, [deathRangeIsUserSet]);
  // Reverted: only force-directed layout
  const [filterSectionsOpen, setFilterSectionsOpen] = useState({ voice: false, birth: false, death: false, birthplaces: false });
  // Disable global click outside handlers while any path input is focused
  const [pathInputFocused, setPathInputFocused] = useState(false);
  // Path panel toggle (default off)
  const [showPathPanel, setShowPathPanel] = useState(false);
  const [showMobileToolbarMenu, setShowMobileToolbarMenu] = useState(false);
  const [showHeaderMenu, setShowHeaderMenu] = useState(false);
  const [showSupportPanel, setShowSupportPanel] = useState(false);
  const [justLoggedIn, setJustLoggedIn] = useState(false);
  const pathFromRef = useRef(null);
  const pathToRef = useRef(null);
  const pathFromValRef = useRef('');
  const pathToValRef = useRef('');
  const pathOverlayRef = useRef({ addedNodeIds: new Set(), addedLinkKeys: new Set() });
  const prePathNetworkRef = useRef(null);
  const pathPanelRef = useRef(null);
  const pathListRef = useRef(null);
  const pathPreviousViewRef = useRef(null);
  const nodeClickTimeoutRef = useRef(null);
const helperMessageTimeoutRef = useRef(null);
const pendingHelperMessageRef = useRef(null);
  const lastTappedNodeIdRef = useRef(null);
  const suppressNextClickRef = useRef(false);
  const [pathInfo, setPathInfo] = useState(null);
  const [helperMessage, setHelperMessage] = useState('');
  const handleClearPath = () => {
    try { setPathInfo(null); } catch (_) {}
    try { if (pathFromRef.current) pathFromRef.current.value = ''; } catch (_) {}
    try { if (pathToRef.current) pathToRef.current.value = ''; } catch (_) {}
    try { pathFromValRef.current = ''; } catch (_) {}
    try { pathToValRef.current = ''; } catch (_) {}
    if (prePathNetworkRef.current) {
      const snapshot = prePathNetworkRef.current;
      pathOverlayRef.current.addedNodeIds = new Set();
      pathOverlayRef.current.addedLinkKeys = new Set();
      try {
        setNetworkData(
          sanitizeGraphData({
            nodes: snapshot.nodes.map((n) => ({ ...n, isPath: false, wasAddedByPath: false })),
            links: snapshot.links.map((l) => ({ ...l, isPath: false, wasAddedByPath: false }))
          })
        );
      } catch (_) {}
      prePathNetworkRef.current = null;
      try { setShouldRunSimulation(false); } catch (_) {}
    } else {
      setNetworkData((prev) => {
        const addedNodeIds = new Set(pathOverlayRef.current.addedNodeIds || []);
        const addedLinkKeys = new Set(pathOverlayRef.current.addedLinkKeys || []);
        const remainingNodes = prev.nodes
          .filter((n) => !addedNodeIds.has(n.id) && !n.wasAddedByPath)
          .map((n) => ({ ...n, isPath: false, wasAddedByPath: false }));
        const remainingLinks = prev.links
          .filter((l) => {
            const key = `${typeof l.source === 'string' ? l.source : l.source?.id}-${typeof l.target === 'string' ? l.target : l.target?.id}-${l.type}`;
            return !addedLinkKeys.has(key) && !l.wasAddedByPath;
          })
          .map((l) => ({ ...l, isPath: false, wasAddedByPath: false }));
        pathOverlayRef.current.addedNodeIds = new Set();
        pathOverlayRef.current.addedLinkKeys = new Set();
        return sanitizeGraphData({ nodes: remainingNodes, links: remainingLinks });
      });
    }
  };

const svgRef = useRef(null);
const centerOnNodeRef = useRef(null);
  const simulationRef = useRef(null);
  const submenuTimeoutRef = useRef(null);
  // Group-drag tracking for path overlay clusters
  const dragGroupIdsRef = useRef(new Set());
  const dragGroupInitialPosRef = useRef(new Map());
  const dragLeaderInitialPosRef = useRef({ x: 0, y: 0 });
  const dragActiveRef = useRef(false);
  // UI-visible zoom transform accessible outside D3 effect
  const uiZoomRef = useRef(d3.zoomIdentity);
  // History navigation (Back/Forward)
  const historyRef = useRef({ past: [], future: [] });
  const [historyCounts, setHistoryCounts] = useState({ past: 0, future: 0 });
  const [savedViews, setSavedViews] = useState([]);
const [isSavingView, setIsSavingView] = useState(false);
const [saveLabel, setSaveLabel] = useState('');
const [loadToken, setLoadToken] = useState('');
const [isLoadingView, setIsLoadingView] = useState(false);
  const [showSaveExportMenu, setShowSaveExportMenu] = useState(false);
  const isLoadingViewRef = useRef(false);
  const dragSuppressClickRef = useRef(false);
  const dragStartPosRef = useRef({ x: 0, y: 0 });
  const longPressClickSuppressRef = useRef(false);
  // Temporary halo effect for search result cards after a search
  const [showResultsHalo, setShowResultsHalo] = useState(false);
  const resultsHaloTimeoutRef = useRef(null);
  const [rateLimitedUntil, setRateLimitedUntil] = useState(0);
  const rateLimitedUntilRef = useRef(0);
  const rateLimitClearTimeoutRef = useRef(null);
  const rateLimitIntervalRef = useRef(null);
  const rateLimitMessageTokenRef = useRef(0);
  const rateLimitCooldownTimeoutRef = useRef(null);
  const [showSavedViewDialog, setShowSavedViewDialog] = useState(false);
  const [savedViewToken, setSavedViewToken] = useState('');
  const [savedViewLabel, setSavedViewLabel] = useState('');
  // Cache person details fetched during expansions/path overlays so nodes can be enriched immediately
  const personCacheRef = useRef(new Map());
  const isSearchingRef = useRef(false);
  const supportPanelLoginFlagRef = useRef(false);
  const sessionRestoredRef = useRef(false);
  const sessionPersistReadyRef = useRef(false);
  const filtersResetRef = useRef(false);
  const headerContainerRef = useRef(null);
  const [headerWidth, setHeaderWidth] = useState(null);

  // Consume auth token handed off via URL (from backend /post-auth)
  useEffect(() => {
    try {
      const current = new URL(window.location.href);
      let tokenFrom = current.searchParams.get('authToken');
      // Also support pending ToS token delivered via URL hash or query
      let pendingFrom = current.searchParams.get('pendingTosToken');
      let pendingEmailFrom = current.searchParams.get('email');
      if (!tokenFrom) {
        const hashParams = new URLSearchParams((window.location.hash || '').replace(/^#/, ''));
        tokenFrom = hashParams.get('authToken');
        if (!pendingFrom) pendingFrom = hashParams.get('pendingTosToken');
        if (!pendingEmailFrom) pendingEmailFrom = hashParams.get('email');
        if (tokenFrom) {
          hashParams.delete('authToken');
          hashParams.delete('pendingTosToken');
          hashParams.delete('email');
          const newHash = hashParams.toString();
          const newUrl = `${current.pathname}${current.search}${newHash ? `#${newHash}` : ''}`;
          window.history.replaceState({}, '', newUrl);
        }
      } else {
        current.searchParams.delete('authToken');
        current.searchParams.delete('pendingTosToken');
        current.searchParams.delete('email');
        const newUrl = `${current.pathname}${current.search}${current.hash}`;
        window.history.replaceState({}, '', newUrl);
      }
      if (tokenFrom) {
        setToken(tokenFrom);
        try { localStorage.setItem('token', tokenFrom); } catch (_) {}
        try { localStorage.setItem(TOKEN_LOGIN_TS_KEY, String(Date.now())); } catch (_) {}
        // Derive email from the token payload so header can greet the user
        try {
          const parts = String(tokenFrom).split('.');
          if (parts.length >= 2) {
            const base64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
            const json = JSON.parse(atob(base64));
            const emailInToken = (json && typeof json.email === 'string') ? json.email.trim().toLowerCase() : '';
            if (emailInToken) {
              setUserEmail(emailInToken);
              try { localStorage.setItem('userEmail', emailInToken); } catch (_) {}
            }
          }
        } catch (_) {}
        // If asked to show support after login (from a pre-redirect flow), honor it now
        try {
          if (localStorage.getItem('cmgShowSupportAfterLogin') === '1') {
            setShowSupportPanel(true);
            localStorage.removeItem('cmgShowSupportAfterLogin');
          }
        } catch (_) {}
        setError('');
      }
      if (pendingFrom) {
        try { setPendingTosToken(pendingFrom); } catch (_) {}
        if (pendingEmailFrom) {
          try { setPendingTosEmail(pendingEmailFrom); localStorage.setItem('userEmail', pendingEmailFrom); } catch (_) {}
        }
        try { setShowTerms(true); } catch (_) {}
      }
    } catch (_) {}
  }, []);

  // Option A: Backend-managed Auth0 session. Provide helpers to redirect to backend and sync a local token.
  const localResolveApiBase = () => {
    try {
      if (typeof window !== 'undefined') {
        const override = window.__CMG_API_BASE;
        if (typeof override === 'string' && override.trim()) {
          return override.trim().replace(/\/$/, '');
        }
      }
    } catch (_) {}
    let envBase = '';
    try { if (typeof import.meta !== 'undefined' && import.meta.env && typeof import.meta.env.VITE_API_BASE === 'string') envBase = import.meta.env.VITE_API_BASE; } catch (_) {}
    envBase = (envBase || '').trim();
    if (envBase) return envBase.replace(/\/$/, '');
    if (typeof window !== 'undefined') {
      const { protocol, hostname } = window.location;
      if (hostname === 'localhost' || hostname === '127.0.0.1') return 'http://localhost:3001';
      return `${protocol}//${hostname}`;
    }
    return 'http://localhost:3001';
  };

  const redirectToAuth0Login = () => {
    try {
      const base = localResolveApiBase();
      const url = `${base}/login?returnTo=${encodeURIComponent(window.location.origin)}`;
      window.location.href = url;
    } catch (_) {}
  };

  const syncSessionToken = useCallback(async () => {
    try {
      const base = localResolveApiBase();
      const resp = await fetch(`${base}/session/token`, { credentials: 'include' });
      if (resp.ok) {
        const data = await resp.json();
        if (data && typeof data.token === 'string') {
          setToken(data.token);
          try { localStorage.setItem('token', data.token); } catch (_) {}
          try { localStorage.setItem(TOKEN_LOGIN_TS_KEY, String(Date.now())); } catch (_) {}
          try { if (data.email) { setUserEmail(data.email); localStorage.setItem('userEmail', data.email); } } catch (_) {}
          setError('');
          return true;
        }
        return false;
      }
      // Handle ToS required case (403) to show in‑app disclaimer without Action redirects
      if (resp.status === 403) {
        let data = {};
        try { data = await resp.json(); } catch (_) {}
        const pendingTokenValue = typeof data.pendingToken === 'string' ? data.pendingToken : '';
        if (data && data.requiresTos && pendingTokenValue) {
          try { if (typeof data.email === 'string') { setUserEmail(data.email); localStorage.setItem('userEmail', data.email); } } catch (_) {}
          setPendingTosToken(pendingTokenValue);
          setPendingTosEmail(typeof data.email === 'string' ? data.email : '');
          // No page redirect; keep user in app after ToS modal
          // Surface the ToS modal
          try { setShowTerms(true); } catch (_) {}
        }
      }
      return false;
    } catch (_) {
      return false;
    }
  }, []);

  useEffect(() => {
    if (!token) { syncSessionToken(); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const clearStoredToken = () => {
    try { localStorage.removeItem('token'); } catch (_) {}
    try { localStorage.removeItem(TOKEN_LOGIN_TS_KEY); } catch (_) {}
    try { localStorage.removeItem('userEmail'); } catch (_) {}
    setPendingTosToken('');
    setPendingTosEmail('');
    setPendingTosRedirect('');
    try { setUserEmail(''); } catch (_) {}
    try {
      const base = localResolveApiBase();
      window.location.replace(`${base}/logout`);
    } catch (_) {}
  };

const hasSearchResults = Array.isArray(searchResults) && searchResults.length > 0;

const isSaveExportEligible = hasExecutedSearch && Array.isArray(networkData?.nodes) && networkData.nodes.length > 0;
const [isExporting, setIsExporting] = useState(false);

useEffect(() => {
  if (!isSaveExportEligible && showSaveExportMenu) {
    setShowSaveExportMenu(false);
  }
}, [isSaveExportEligible, showSaveExportMenu]);

const ensureNetworkView = () => {
  if (currentView !== 'network') {
    setCurrentView('network');
  }
};

const openPathPanel = () => {
  if (pathPreviousViewRef.current == null && currentView !== 'network') {
    pathPreviousViewRef.current = currentView;
  }
  ensureNetworkView();
  setShowSaveExportMenu(false);
  if (currentView === 'network') {
    setShowPathPanel(true);
  } else {
    setTimeout(() => {
      setShowPathPanel(true);
    }, 0);
  }
};

const togglePathPanel = () => {
  if (showPathPanel) {
    closePathPanel();
  } else {
    openPathPanel();
  }
};

const closePathPanel = () => {
  setShowSaveExportMenu(false);
  setShowPathPanel(false);
  pathPreviousViewRef.current = null;
};

const runPathFind = () => {
  if (typeof window !== 'undefined' && typeof window.__cmg_runFindPath === 'function') {
    window.__cmg_runFindPath();
  }
};

useEffect(() => {
  if (showPathPanel && pathFromRef.current && !viewportIsPhone) {
    try {
      pathFromRef.current.focus();
    } catch (_) {}
  }
}, [showPathPanel, viewportIsPhone]);

useEffect(() => {
  }, [showPathPanel, currentView]);

  useEffect(() => {
    // Expose simple debug helpers in the browser console
    try {
      if (typeof window !== 'undefined') {
        // Keep a lightweight snapshot of the latest network so console helpers
        // can read state even outside React closures.
        try {
          window.__CMG_lastNetwork = {
            nodes: Array.isArray(networkData?.nodes) ? networkData.nodes.map(n => ({ ...n })) : [],
            links: Array.isArray(networkData?.links) ? networkData.links.map(l => ({ ...l })) : []
          };
        } catch (_) {}
        window.__CMG_setLayoutDebug = (on) => (window.__CMG_DEBUG_LAYOUT = !!on);
        window.__CMG_setLongEdgeThreshold = (px) => (window.__CMG_LONG_EDGE_THRESHOLD = Number(px) || 260);
        // Define helpers if missing; operate on the latest snapshot
        if (typeof window.__CMG_dumpLongEdges !== 'function') {
          window.__CMG_dumpLongEdges = (threshold = (window.__CMG_LONG_EDGE_THRESHOLD || 260)) => {
            const snap = window.__CMG_lastNetwork || {};
            const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
            const links = Array.isArray(snap.links) ? snap.links : [];
            const idTo = new Map(nodes.map(n => [n.id, n]));
            const rows = [];
            links.forEach(l => {
              const s = (typeof l.source === 'string' ? idTo.get(l.source) : idTo.get(l?.source?.id) || l.source);
              const t = (typeof l.target === 'string' ? idTo.get(l.target) : idTo.get(l?.target?.id) || l.target);
              if (!s || !t) return;
              const len = Math.hypot((Number(t.x)||0)-(Number(s.x)||0), (Number(t.y)||0)-(Number(s.y)||0));
              if (Number.isFinite(len) && len >= threshold) rows.push({ s: s.id, t: t.id, type: l.type, len: Math.round(len) });
            });
            rows.sort((a,b) => b.len - a.len);
            try { console.table(rows.slice(0, 50)); } catch (_) { console.log(rows.slice(0, 50)); }
            return rows;
          };
        }
        if (typeof window.__CMG_findNodeIdByName !== 'function') {
          window.__CMG_findNodeIdByName = (name) => {
            const snap = window.__CMG_lastNetwork || {};
            const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
            const nm = String(name || '').trim().toLowerCase();
            const n = nodes.find(x => String(x?.name||'').toLowerCase() === nm);
            return n ? n.id : null;
          };
        }
        window.__CMG_debugTrackNode = (id) => {
          window.__CMG_DEBUG_TRACK_NODE = String(id || '').trim();
          return window.__CMG_DEBUG_TRACK_NODE;
        };
      }
    } catch (_) {}
    return () => {
      if (nodeClickTimeoutRef.current) {
        clearTimeout(nodeClickTimeoutRef.current);
        nodeClickTimeoutRef.current = null;
      }
      if (helperMessageTimeoutRef.current) {
        clearTimeout(helperMessageTimeoutRef.current);
        helperMessageTimeoutRef.current = null;
      }
      lastTappedNodeIdRef.current = null;
      suppressNextClickRef.current = false;
    };
  }, []);

  const renderSaveExportFields = ({ containerStyle = {}, isMobileLayout = false } = {}) => {
  const disabledSave = !isSaveExportEligible || !token || isSavingView;
  const disabledExport = !isSaveExportEligible || isExporting;
  const actionButtonBase = {
      padding: '8px 12px',
      backgroundColor: '#ffffff',
      color: '#374151',
      border: '2px solid #3e96e2',
      borderRadius: 8,
      fontSize: '16px',
      height: '48px',
      display: 'inline-flex',
      alignItems: 'center',
      boxSizing: 'border-box',
      justifyContent: 'center',
      width: isMobileLayout ? '100%' : 'auto'
    };
  return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8, width: '100%', ...containerStyle }}>
        <input
          placeholder="Optional label"
          value={saveLabel}
          onChange={e => setSaveLabel(e.target.value)}
          style={{ padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: 8, height: '48px', boxSizing: 'border-box', fontSize: '16px' }}
        />
        <button
          onClick={saveCurrentView}
          disabled={disabledSave}
          style={{
            ...actionButtonBase,
            cursor: disabledSave ? 'not-allowed' : 'pointer',
            opacity: disabledSave ? 0.6 : 1
          }}
        >
          {isSavingView ? 'Saving…' : 'Save View'}
        </button>
        <button
          onClick={() => { exportAsCSV(); setShowSaveExportMenu(false); }}
          disabled={disabledExport}
          style={{
            ...actionButtonBase,
            cursor: disabledExport ? 'not-allowed' : 'pointer',
            opacity: disabledExport ? 0.6 : 1
          }}
        >
          {isExporting ? 'Exporting…' : 'Export text file'}
        </button>
      </div>
    );
};

const renderSaveExportToggle = ({
  containerStyle = {},
  buttonStyle = {},
    menuStyle = {},
    align = 'right',
    isMobileLayout = false
  } = {}) => {
    const justify = align === 'left' ? { left: 0, right: 'auto' } : { right: 0, left: 'auto' };
    return (
      <div style={{ position: 'relative', ...containerStyle }}>
        <button
          ref={saveExportBtnRef}
          onMouseDown={(e) => {
            if (isSaveExportEligible) {
              e.stopPropagation();
              try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {}
            }
          }}
          onClick={(e) => {
            if (!isSaveExportEligible) return;
            e.stopPropagation();
            setShowSaveExportMenu((v) => !v);
          }}
          disabled={!isSaveExportEligible}
          style={{
            padding: '8px 16px',
            backgroundColor: showSaveExportMenu ? '#f3f4f6' : '#ffffff',
            color: '#374151',
            border: '2px solid #3e96e2',
            borderRadius: '8px',
            cursor: isSaveExportEligible ? 'pointer' : 'not-allowed',
            fontSize: '16px',
            lineHeight: '20px',
            height: '48px',
            display: 'inline-flex',
            alignItems: 'center',
            boxSizing: 'border-box',
            opacity: isSaveExportEligible ? 1 : 0.6,
            ...buttonStyle
          }}
        >
          Save/Export ▾
        </button>
        {showSaveExportMenu && (
          <div
            style={{
              position: 'absolute',
              top: '110%',
              border: '2px solid #3e96e2',
              borderRadius: 8,
              boxShadow: '0 8px 20px rgba(0,0,0,0.12)',
              padding: 12,
              minWidth: isMobileLayout ? 'min(280px, 80vw)' : 260,
              backgroundColor: 'white',
              zIndex: 1000,
              ...justify,
              ...menuStyle
            }}
            onMouseLeave={() => setShowSaveExportMenu(false)}
            onMouseDown={(e) => { e.stopPropagation(); try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {} }}
            onClick={(e) => e.stopPropagation()}
          >
            {renderSaveExportFields({ isMobileLayout })}
          </div>
        )}
      </div>
    );
  };

  useLayoutEffect(() => {
    const node = headerContainerRef.current;
    if (!node) return;

    const updateWidth = () => {
      try {
        const rect = node.getBoundingClientRect();
        const width = rect?.width ?? node.offsetWidth;
        if (Number.isFinite(width)) {
          const rounded = Math.round(width);
          setHeaderWidth(prev => (prev === rounded ? prev : rounded));
        }
      } catch (_) {}
    };

    updateWidth();

    if (typeof ResizeObserver !== 'undefined') {
      const observer = new ResizeObserver(() => updateWidth());
      observer.observe(node);
      return () => observer.disconnect();
    }

    if (typeof window !== 'undefined') {
      const resizeHandler = () => updateWidth();
      window.addEventListener('resize', resizeHandler);
      return () => {
        window.removeEventListener('resize', resizeHandler);
      };
    }

    return undefined;
  }, []);

  useEffect(() => {
    const baseHeight = 550;
    const viewportH = debouncedViewportHeight || (typeof window !== 'undefined' ? window.innerHeight : 0);

    let nextHeight = baseHeight;
    if (isPhone) {
      nextHeight = Math.max(360, Math.round((viewportH || baseHeight) * 0.65));
    } else if (isTablet) {
      nextHeight = Math.max(420, Math.round((viewportH || baseHeight) * 0.72));
    }

    if (Number.isFinite(nextHeight) && nextHeight !== visualizationHeight) {
      setVisualizationHeight(nextHeight);
    }
  }, [isPhone, isTablet, debouncedViewportHeight, visualizationHeight]);

  useEffect(() => {
    if (!isMobileViewport && showMobileToolbarMenu) {
      setShowMobileToolbarMenu(false);
    }
  }, [isMobileViewport, showMobileToolbarMenu]);

  // Prevent body scroll from stealing touch scroll when filter sheet is open on mobile
  useEffect(() => {
    try {
      if (typeof document !== 'undefined' && isMobileViewport) {
        const prev = document.body.style.overflow;
        if (showFilterPanel) {
          document.body.style.overflow = 'hidden';
        } else {
          document.body.style.overflow = prev || '';
        }
        return () => { document.body.style.overflow = prev || ''; };
      }
    } catch (_) {}
  }, [isMobileViewport, showFilterPanel]);

  useEffect(() => {
    if (!isHeaderMobile && showHeaderMenu) {
      setShowHeaderMenu(false);
    }
  }, [isHeaderMobile, showHeaderMenu]);

  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const originalBg = document.body.style.backgroundColor;
    document.body.style.backgroundColor = '#0f172a';
    return () => {
      document.body.style.backgroundColor = originalBg;
    };
  }, []);

  useEffect(() => {
    if (!isSaveExportEligible && showSaveExportMenu) {
      setShowSaveExportMenu(false);
    }
  }, [isSaveExportEligible, showSaveExportMenu]);

  useEffect(() => {
    if (currentView !== 'network' && showMobileToolbarMenu) {
      setShowMobileToolbarMenu(false);
    }
  }, [currentView, showMobileToolbarMenu]);

  // Centralized unauthorized handler: clears token and prompts re-login
  const handleUnauthorized = (resp) => {
    try {
      if (resp && (resp.status === 401 || resp.status === 403)) {
        setError('Session expired. Please log in again.');
        setToken('');
        clearStoredToken();
        setHasExecutedSearch(false);
        return true;
      }
    } catch (_) {}
    return false;
  };

  const createSnapshot = () => {
    // Normalize links to id strings for stability
    const nodesSnap = networkData.nodes.map(n => ({ ...n }));
    const linksSnap = networkData.links.map(l => ({
      ...l,
      source: (typeof l.source === 'string' ? l.source : l.source?.id),
      target: (typeof l.target === 'string' ? l.target : l.target?.id)
    }));
    // Deep copy itemDetails as cards depend on it
    let detailsSnap = null;
    try { detailsSnap = itemDetails ? JSON.parse(JSON.stringify(itemDetails)) : null; } catch (_) { detailsSnap = null; }
    let zoom = null;
    try {
      const z = (window.__cmg_zoomTransform || uiZoomRef?.current || null);
      zoom = z && typeof z.k === 'number' && typeof z.x === 'number' && typeof z.y === 'number'
        ? { k: z.k, x: z.x, y: z.y }
        : null;
    } catch (_) { zoom = null; }
    let searchResultsSnap = [];
    let originalSearchResultsSnap = [];
    let pathInfoSnap = null;
    try { searchResultsSnap = JSON.parse(JSON.stringify(searchResults || [])); } catch (_) { searchResultsSnap = []; }
    try {
      originalSearchResultsSnap = JSON.parse(JSON.stringify(originalSearchResults || []));
    } catch (_) {
      originalSearchResultsSnap = searchResultsSnap;
    }
    try { pathInfoSnap = pathInfo ? JSON.parse(JSON.stringify(pathInfo)) : null; } catch (_) { pathInfoSnap = null; }
    const selectedVoiceTypesSnap = Array.from(selectedVoiceTypes || []);
    const selectedBirthplacesSnap = Array.from(selectedBirthplaces || []);
    const birthRangeSnap = Array.isArray(birthYearRange) ? [...birthYearRange] : [...DEFAULT_BIRTH_RANGE];
    const deathRangeSnap = Array.isArray(deathYearRange) ? [...deathYearRange] : [...DEFAULT_DEATH_RANGE];
  return {
    snapshotVersion: 2,
    nodes: nodesSnap,
    links: linksSnap,
      currentView,
      searchType,
      searchQuery,
      searchResults: searchResultsSnap,
      originalSearchResults: originalSearchResultsSnap,
      selectedVoiceTypes: selectedVoiceTypesSnap,
      selectedBirthplaces: selectedBirthplacesSnap,
    birthYearRange: birthRangeSnap,
    deathYearRange: deathRangeSnap,
    showFilterPanel,
    showPathPanel,
    pathInfo: pathInfoSnap,
    hasExecutedSearch,
    selectedNodeId: selectedNode ? selectedNode.id : null,
      currentCenterNode,
      visualizationHeight,
      itemDetails: detailsSnap,
      selectedItem: selectedItem ? { ...selectedItem } : null,
      zoom };
  };

  const applySnapshot = (snap, options = {}) => {
    const { restoreFilters = true } = options;
    if (!snap) return;
    const clonedNodes = snap.nodes.map(n => ({ ...n }));
    const clonedLinks = snap.links.map(l => ({ ...l }));
    // Normalize link endpoints to string ids to avoid stale object refs
    const normalizedLinks = normalizeLinks(
      clonedLinks.map(l => ({
        ...l,
        source: (typeof l.source === 'string' ? l.source : (l.source && l.source.id) || l.source),
        target: (typeof l.target === 'string' ? l.target : (l.target && l.target.id) || l.target)
      }))
    );
    setNetworkData(sanitizeGraphData({ nodes: clonedNodes, links: normalizedLinks }));
    // If the snapshot contains explicit positions, preserve layout and build a dormant sim
    const hasPositions = Array.isArray(clonedNodes) && clonedNodes.length > 0 && clonedNodes.every(n => Number.isFinite(n.x) && Number.isFinite(n.y));
    try { setShouldRunSimulation(!hasPositions); } catch (_) {}
    // Force D3 to rebuild with the new data and reattach forces/drag regardless of counts
    try { setFiltersVersion(v => v + 1); } catch (_) {}
    try { setCurrentView(snap.currentView); } catch (_) {}
    try { setSearchType(snap.searchType); } catch (_) {}
    try { setSelectedNode(null); } catch (_) {}
    try { setCurrentCenterNode(snap.currentCenterNode || null); } catch (_) {}
    // Fixed height; ignore saved visualizationHeight from snapshots
    // Reapply zoom transform immediately if available
    try {
      const z = snap.zoom;
      const dz = (z && typeof z.k === 'number' && typeof z.x === 'number' && typeof z.y === 'number')
        ? d3.zoomIdentity.translate(z.x, z.y).scale(z.k)
        : d3.zoomIdentity;
      uiZoomRef.current = dz;
      try { window.__cmg_zoomTransform = dz; } catch (_) {}
      const svgSel = d3.select(svgRef.current);
      svgSel.property('__zoom', dz);
      svgSel.select('g').attr('transform', dz);
    } catch (_) {}
    // Restore detail cards state
    try {
      setItemDetails(snap.itemDetails ? normalizeDetailsRelationshipSources(snap.itemDetails) : null);
    } catch (_) {}
    try { setSelectedItem(snap.selectedItem || null); } catch (_) {}
    try {
      if (typeof snap.searchQuery === 'string') {
        setSearchQuery(snap.searchQuery);
      }
    } catch (_) {}
    try {
      if (Array.isArray(snap.searchResults)) {
        setSearchResults(snap.searchResults);
      }
    } catch (_) {}
    try {
      if (Array.isArray(snap.originalSearchResults)) {
        setOriginalSearchResults(snap.originalSearchResults);
      } else if (Array.isArray(snap.searchResults)) {
        setOriginalSearchResults(snap.searchResults);
      }
    } catch (_) {}
    try {
      if (typeof snap.hasExecutedSearch === 'boolean') {
        setHasExecutedSearch(snap.hasExecutedSearch);
      }
    } catch (_) {}
    if (restoreFilters) {
      try {
        const voices = Array.isArray(snap.selectedVoiceTypes) ? snap.selectedVoiceTypes : [];
        setSelectedVoiceTypes(new Set(voices));
      } catch (_) {}
      try {
        const places = Array.isArray(snap.selectedBirthplaces) ? snap.selectedBirthplaces : [];
        setSelectedBirthplaces(new Set(places));
      } catch (_) {}
      try {
        if (Array.isArray(snap.birthYearRange) && snap.birthYearRange.length === 2) {
          setBirthYearRange(snap.birthYearRange);
        }
      } catch (_) {}
      try {
        if (Array.isArray(snap.deathYearRange) && snap.deathYearRange.length === 2) {
          setDeathYearRange(snap.deathYearRange);
        }
      } catch (_) {}
      try { setShowFilterPanel(!!snap.showFilterPanel); } catch (_) {}
    } else {
      try { setSelectedVoiceTypes(new Set()); } catch (_) {}
      try { setSelectedBirthplaces(new Set()); } catch (_) {}
      try { setBirthYearRange([...DEFAULT_BIRTH_RANGE]); } catch (_) {}
      try { setDeathYearRange([...DEFAULT_DEATH_RANGE]); } catch (_) {}
      try { setShowFilterPanel(false); } catch (_) {}
      setTimeout(() => {
        try { resetDateRanges(); } catch (_) {}
      }, 0);
    }
    try { setShowPathPanel(!!snap.showPathPanel); } catch (_) {}
    try { setPathInfo(snap.pathInfo || null); } catch (_) {}
  };

  // Utility: sleep
  const sleep = (ms) => new Promise(res => setTimeout(res, ms));

  const RATE_LIMIT_MIN_WAIT_MS = 1000;
  const RATE_LIMIT_DEFAULT_WAIT_MS = 2000;
  const isRateLimitMessage = (msg) => typeof msg === 'string' && msg.startsWith('Too many requests –');
  const formatRateLimitWaitMessage = (untilTs) => {
    if (!untilTs) return 'Too many requests – please try again shortly.';
    const msRemaining = Math.max(0, untilTs - Date.now());
    const secs = Math.max(1, Math.ceil(msRemaining / 1000));
    return `Too many requests – please try again in ${secs}s`;
  };
  const scheduleRateLimitCooldown = (rawWaitMs = RATE_LIMIT_DEFAULT_WAIT_MS, { suppressMessage = false } = {}) => {
    const waitMs = Math.max(RATE_LIMIT_MIN_WAIT_MS, rawWaitMs || RATE_LIMIT_DEFAULT_WAIT_MS);
    const nowTs = Date.now();
    const currentUntil = rateLimitedUntilRef.current || 0;
    const proposedUntil = nowTs + waitMs;
    const untilTs = Math.max(currentUntil, proposedUntil);
    rateLimitedUntilRef.current = untilTs;
    try { setRateLimitedUntil(untilTs); } catch (_) {}
    if (rateLimitCooldownTimeoutRef.current) {
      clearTimeout(rateLimitCooldownTimeoutRef.current);
      rateLimitCooldownTimeoutRef.current = null;
    }
    const cooldownWait = Math.max(0, untilTs - nowTs);
    rateLimitCooldownTimeoutRef.current = setTimeout(() => {
      if (rateLimitedUntilRef.current <= untilTs) {
        rateLimitedUntilRef.current = 0;
        try { setRateLimitedUntil(0); } catch (_) {}
      }
    }, cooldownWait + 50);
    if (suppressMessage) {
      return { waitMs, untilTs, message: formatRateLimitWaitMessage(untilTs) };
    }
    if (rateLimitClearTimeoutRef.current) {
      clearTimeout(rateLimitClearTimeoutRef.current);
      rateLimitClearTimeoutRef.current = null;
    }
    if (rateLimitIntervalRef.current) {
      clearInterval(rateLimitIntervalRef.current);
      rateLimitIntervalRef.current = null;
    }
    const token = (rateLimitMessageTokenRef.current || 0) + 1;
    rateLimitMessageTokenRef.current = token;
    const updateCountdownMessage = () => {
      if (rateLimitMessageTokenRef.current !== token) return;
      const currentUntil = rateLimitedUntilRef.current || 0;
      if (!currentUntil || Date.now() >= currentUntil) {
        rateLimitedUntilRef.current = 0;
        try { setRateLimitedUntil(0); } catch (_) {}
        try {
          setError((prev) => (isRateLimitMessage(prev) ? '' : prev));
        } catch (_) {}
        if (rateLimitIntervalRef.current) {
          clearInterval(rateLimitIntervalRef.current);
          rateLimitIntervalRef.current = null;
        }
        return;
      }
      try { setError(formatRateLimitWaitMessage(currentUntil)); } catch (_) {}
    };
    updateCountdownMessage();
    rateLimitIntervalRef.current = setInterval(updateCountdownMessage, 1000);
    rateLimitClearTimeoutRef.current = setTimeout(() => {
      if (rateLimitMessageTokenRef.current !== token) return;
      rateLimitClearTimeoutRef.current = null;
      updateCountdownMessage();
    }, cooldownWait + 150);
    return { waitMs, untilTs, message: formatRateLimitWaitMessage(untilTs) };
  };
  const handleRateLimitResponse = (response, fallbackMs = RATE_LIMIT_DEFAULT_WAIT_MS, options = {}) => {
    if (!response || response.status !== 429) return null;
    const headers = response.headers && typeof response.headers.get === 'function' ? response.headers : null;
    const retryAfterHeader = headers ? (headers.get('Retry-After') || headers.get('retry-after')) : null;
    const rateLimitResetHeader = headers ? (headers.get('RateLimit-Reset') || headers.get('ratelimit-reset')) : null;
    let waitMs = fallbackMs;
    if (retryAfterHeader) {
      const parsed = Number(retryAfterHeader);
      if (Number.isFinite(parsed) && parsed >= 0) {
        waitMs = parsed * 1000;
      }
    } else if (rateLimitResetHeader) {
      const parsed = Number(rateLimitResetHeader);
      if (Number.isFinite(parsed) && parsed >= 0) {
        waitMs = Math.max(waitMs, parsed * 1000);
      }
    }
    return scheduleRateLimitCooldown(waitMs, options);
  };
  useEffect(() => {
    return () => {
      if (rateLimitClearTimeoutRef.current) {
        clearTimeout(rateLimitClearTimeoutRef.current);
        rateLimitClearTimeoutRef.current = null;
      }
      if (rateLimitIntervalRef.current) {
        clearInterval(rateLimitIntervalRef.current);
        rateLimitIntervalRef.current = null;
      }
      if (rateLimitCooldownTimeoutRef.current) {
        clearTimeout(rateLimitCooldownTimeoutRef.current);
        rateLimitCooldownTimeoutRef.current = null;
      }
    };
  }, []);

  // Fetch with retry and exponential backoff (handles 429 with Retry-After)
  const fetchWithRetry = async (url, options = {}, { retries = 2, baseDelay = 500 } = {}) => {
    let attempt = 0;
    let lastErr;
    let lastStatus = null;
    let lastRateLimitMessage = '';
    while (attempt <= retries) {
      try {
        // Global cooldown if we've recently been rate-limited
        const now = Date.now();
        const until = rateLimitedUntilRef.current || 0;
        if (until && now < until) {
          await sleep(Math.min(until - now, 10000));
        }
        const resp = await fetch(url, options);
        lastStatus = resp.status;
        if (resp.status === 429) {
          const fallbackDelay = baseDelay * Math.pow(2, attempt);
          const info = handleRateLimitResponse(resp, fallbackDelay);
          lastRateLimitMessage = info?.message || formatRateLimitWaitMessage(rateLimitedUntilRef.current);
          await sleep(info?.waitMs || fallbackDelay || RATE_LIMIT_DEFAULT_WAIT_MS);
          attempt += 1;
          continue;
        }
        return resp;
      } catch (e) {
        lastErr = e;
        await sleep(baseDelay * Math.pow(2, attempt));
        attempt += 1;
      }
    }
    if (lastErr) throw lastErr;
    if (lastStatus === 429) throw new Error(lastRateLimitMessage || formatRateLimitWaitMessage(rateLimitedUntilRef.current || (Date.now() + RATE_LIMIT_DEFAULT_WAIT_MS)));
    throw new Error('Request failed');
  };

  // Simple concurrency limiter for an array of async tasks
  const runWithLimit = async (tasks, limit = 3) => {
    const results = [];
    let idx = 0;
    const workers = new Array(Math.min(limit, tasks.length)).fill(0).map(async () => {
      while (true) {
        const i = idx++;
        if (i >= tasks.length) break;
        results[i] = await tasks[i]();
      }
    });
    await Promise.all(workers);
    return results;
  };

  // Fetch and cache details for a single person name
  const fetchAndCachePersonDetails = async (fullName) => {
    if (!fullName) return null;
    const cache = personCacheRef.current;
    if (cache.has(fullName)) return cache.get(fullName);
    try {
      const resp = await fetch(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ singerName: fullName, depth: 1 })
      });
      if (handleRateLimitResponse(resp)) return null;
      const data = await resp.json();
      if (resp.ok && data && data.center) {
        cache.set(fullName, data);
        return data;
      }
    } catch (_) {}
    return null;
  };

  // Enrich specified person nodes in the current network with details and sources
  const enrichPersonNodes = async (personNames) => {
    try {
      const unique = Array.from(new Set((personNames || []).filter(Boolean)));
      if (!unique.length) return;
      // fetch missing details in parallel
      await Promise.all(unique.map(nm => fetchAndCachePersonDetails(nm)));
      const cache = personCacheRef.current;
      setNetworkData(prev => {
        const namesSet = new Set(unique);
        // Mutate in place to preserve node object identity for D3 simulation
        (prev.nodes || []).forEach(n => {
          if (n.type === 'person' && namesSet.has(n.name)) {
            const d = cache.get(n.name);
            const c = d && d.center ? d.center : null;
            if (c) {
              const birthVal = (c.birth_year ?? (c.birth && (c.birth.low ?? c.birth))) || null;
              const deathVal = (c.death_year ?? (c.death && (c.death.low ?? c.death))) || null;
              const birthplaceVal = c.birthplace || c.citizen || c.birthplace || null;
              n.voiceType = n.voiceType || c.voice_type || n.voiceType;
              n.birthYear = n.birthYear || birthVal || n.birthYear;
              n.deathYear = n.deathYear || deathVal || n.deathYear;
              n.birthplace = n.birthplace || birthplaceVal || n.birthplace;
              n.spelling_source = n.spelling_source || c.spelling_source || n.spelling_source || null;
              n.voice_type_source = n.voice_type_source || c.voice_type_source || n.voice_type_source || null;
              n.dates_source = n.dates_source || c.dates_source || n.dates_source || null;
              n.birthplace_source = n.birthplace_source || c.birthplace_source || null;
            }
          }
        });
        // Return same array reference to keep D3 simulation pointers valid
        return { ...prev, nodes: prev.nodes };
      });
    } catch (_) {}
  };

  const pushHistory = (label) => {
    try {
      const snap = createSnapshot();
      historyRef.current.past.push(snap);
      historyRef.current.future = [];
      setHistoryCounts({ past: historyRef.current.past.length, future: 0 });
    } catch (_) {}
  };
  // Save current snapshot to backend
  const saveCurrentView = async () => {
    try {
      setIsSavingView(true);
      // Build a lean snapshot that strips simulation fields and large transient data
      const round = (v) => (Number.isFinite(v) ? Math.round(v) : undefined);
      const slimNode = (n) => {
        const base = {
          id: n.id,
          name: n.name,
          type: n.type,
          voiceType: n.voiceType,
          birthYear: n.birthYear,
          deathYear: n.deathYear,
          birthplace: n.birthplace || n.citizen || undefined,
          spelling_source: n.spelling_source,
          voice_type_source: n.voice_type_source || n.voiceType_source || n.voiceTypeSource,
          dates_source: n.dates_source || n.datesSource,
          birthplace_source: n.birthplace_source || n.birthplaceSource
        };
        // Preserve coarse positions so layout looks familiar when reloaded
        const withPos = {
          ...base,
          x: round(n.x),
          y: round(n.y)
        };
        return Object.fromEntries(Object.entries(withPos).filter(([, v]) => v !== undefined && v !== null));
      };
      // Save all relevant relationship source fields for each link
      const REL_SOURCE_FIELDS = [
        'relationshipSourceDisplay',
        'relationship_source_display',
        'teacher_rel_source',
        'teacher_rel_source_text',
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
        'label',
        'opera_source_text',
        'opera_source_url',
        'teacher_rel_source_url',
        'sourceUrl',
        'meta'
      ];
      const slimLink = (l) => {
        const base = {
          source: (typeof l.source === 'string' ? l.source : (l.source && l.source.id) || l.source),
          target: (typeof l.target === 'string' ? l.target : (l.target && l.target.id) || l.target),
          label: l.label,
          role: l.role,
          type: l.type
        };
        // Copy all relevant relationship source fields if present
        REL_SOURCE_FIELDS.forEach(field => {
          if (l[field] !== undefined) base[field] = l[field];
        });
        return base;
      };
      const slimGraph = {
        nodes: (networkData.nodes || []).map(slimNode),
        links: (networkData.links || []).map(slimLink)
      };
      const zoom = (window.__cmg_zoomTransform || uiZoomRef.current || d3.zoomIdentity);
      const snapshot = {
        version: 2,
        graph: slimGraph,
        ui: { zoom: { k: zoom.k, x: round(zoom.x), y: round(zoom.y) }, visualizationHeight },
        view: {
          currentView,
          searchType,
          selectedNodeId: selectedNode ? selectedNode.id : null,
          currentCenterNode,
          hasExecutedSearch,
          filters: {
            selectedVoiceTypes: Array.from(selectedVoiceTypes || []),
            selectedBirthplaces: Array.from(selectedBirthplaces || []),
            birthYearRange,
            deathYearRange
          }
        },
        // Keep minimal details to avoid re-fetch on reload; omit giant lists if present
        details: { itemDetails: itemDetails ? { ...itemDetails, family: itemDetails.family } : null, selectedItem },
        meta: { savedAt: new Date().toISOString(), label: saveLabel || '' }
      };
      const resp = await fetch(`${API_BASE}/views`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ snapshot, label: saveLabel || '' })
      });
      const rateInfo = handleRateLimitResponse(resp);
      if (rateInfo) throw new Error(rateInfo.message);
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || 'Failed');
      setSaveLabel('');
      // refresh list
      try { await refreshSavedViews(); } catch (_) {}
      setSavedViewToken(data.token || '');
      setSavedViewLabel(saveLabel || '');
      setShowSavedViewDialog(true);
    } catch (e) {
      setError(e.message || 'Failed to save view');
    } finally {
      setIsSavingView(false);
    }
  };

  const refreshSavedViews = async () => {
    try {
      const resp = await fetch(`${API_BASE}/views`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const rateInfo = handleRateLimitResponse(resp);
      if (rateInfo) throw new Error(rateInfo.message);
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || 'Failed');
      setSavedViews(data.views || []);
    } catch (e) {
      // non-fatal
    }
  };

  const loadViewByToken = async (tokenToLoad, options = {}) => {
    try {
      const resp = await fetchWithRetry(`${API_BASE}/views/${encodeURIComponent(tokenToLoad)}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      }, { retries: 2, baseDelay: 600 });
      const textResp = await resp.text();
      let data;
      try { data = textResp ? JSON.parse(textResp) : {}; } catch (_) { data = { error: textResp || 'Invalid response' }; }
      if (!resp.ok) throw new Error(data.error || `Failed (${resp.status})`);
      if (!data.snapshot) throw new Error('Invalid snapshot');

      const snapshot = data.snapshot || {};
      const graph = snapshot.graph || {};
      const view = snapshot.view || {};
      const filters = view.filters || {};
      const details = snapshot.details || {};
      const ui = snapshot.ui || {};
      const center = details.itemDetails?.center || null;
      const centerName = center?.full_name || view.currentCenterNode || snapshot.currentCenterNode || '';
      const searchQueryFromSnapshot = snapshot.searchQuery || view.searchQuery || centerName || '';
      const snapshotSearchResults = Array.isArray(snapshot.searchResults) ? snapshot.searchResults : [];
      const viewSearchResults = Array.isArray(view.searchResults) ? view.searchResults : [];
      const derivedSearchResults = snapshotSearchResults.length ? snapshotSearchResults
        : (viewSearchResults.length ? viewSearchResults
        : (centerName ? [{ name: centerName, properties: center ? { ...center } : {} }] : []));

      const snapshotToApply = {
        nodes: (graph.nodes || snapshot.nodes || []).map(n => ({ ...n })),
        // Restore all relationship source fields for each link
        links: (graph.links || snapshot.links || []).map(l => ({ ...l })),
        currentView: view.currentView || snapshot.currentView || 'network',
        searchType: view.searchType || snapshot.searchType || 'singers',
        searchQuery: searchQueryFromSnapshot,
        searchResults: derivedSearchResults,
        originalSearchResults: snapshot.originalSearchResults || derivedSearchResults,
        selectedNodeId: view.selectedNodeId ?? snapshot.selectedNodeId ?? null,
        currentCenterNode: (view.currentCenterNode ?? snapshot.currentCenterNode ?? centerName) || null,
        selectedVoiceTypes: snapshot.selectedVoiceTypes || filters.selectedVoiceTypes || [],
        selectedBirthplaces: snapshot.selectedBirthplaces || filters.selectedBirthplaces || [],
        birthYearRange: snapshot.birthYearRange || filters.birthYearRange || [...DEFAULT_BIRTH_RANGE],
        deathYearRange: snapshot.deathYearRange || filters.deathYearRange || [...DEFAULT_DEATH_RANGE],
        showFilterPanel: snapshot.showFilterPanel ?? false,
        showPathPanel: snapshot.showPathPanel ?? false,
        pathInfo: snapshot.pathInfo || null,
        itemDetails: details.itemDetails || snapshot.itemDetails || null,
        selectedItem: details.selectedItem || snapshot.selectedItem || null,
        zoom: (ui.zoom || snapshot.zoom || null),
        visualizationHeight: ui.visualizationHeight || snapshot.visualizationHeight || visualizationHeight,
        ui,
        hasExecutedSearch: view.hasExecutedSearch ?? true
      };

      if (options.centerOnLoad) {
        const containerWidth =
          Number(viewportWidth) ||
          (typeof window !== 'undefined' ? window.innerWidth : 0) ||
          1;
        const containerHeight =
          Number(visualizationHeight) ||
          Number(viewportHeight) ||
          (typeof window !== 'undefined' ? window.innerHeight : 0) ||
          1;
        const fitTransform = computeCenteredTransform(
          snapshotToApply.nodes,
          containerWidth,
          containerHeight,
          isHeaderMobile ? 48 : 80
        );
        if (fitTransform) {
          const plainTransform = { k: fitTransform.k, x: fitTransform.x, y: fitTransform.y };
          snapshotToApply.zoom = plainTransform;
          snapshotToApply.ui = { ...(snapshotToApply.ui || {}), zoom: plainTransform };
        }
      }

      applySnapshot(snapshotToApply);
      setHasExecutedSearch(snapshotToApply.hasExecutedSearch ?? true);

      if (options.treatAsSearch) {
        setCurrentView('network');
        setSearchQuery(snapshotToApply.searchQuery || (snapshotToApply.itemDetails?.center?.full_name || ''));
        setProfileCard({ show: false, data: null });
      } else {
        setCurrentView(snapshotToApply.currentView || 'network');
      }

      setHistoryCounts({
        past: historyRef.current.past.length,
        future: historyRef.current.future.length
      });
    } catch (e) {
      setError(e.message || 'Failed to load view');
    }
  };

const attemptLoadSavedView = async () => {
    if (!token || !loadToken || isLoadingViewRef.current) return;
    try {
      setIsLoadingView(true);
      isLoadingViewRef.current = true;
      await loadViewByToken(loadToken, { treatAsSearch: true });
    } finally {
      setIsLoadingView(false);
      isLoadingViewRef.current = false;
    }
  };

  // Auto-load from ?view=TOKEN if present
  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      const view = params.get('view');
      if (view && token) {
        setLoadToken(view);
        loadViewByToken(view, { treatAsSearch: true });
      }
    } catch (_) {}
  }, [token]);

  // Export helpers (JSON and CSV)
  const downloadBlob = (blob, filename) => {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = filename; a.style.display = 'none';
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  };

  // Ensure family data is present for a person response; merge if missing
  const enrichWithFamily = async (data, personName) => {
    try {
      const hasFamily = !!(data?.family && data.family.length) || !!(data?.center?.family && data.center.family.length);
      if (hasFamily || !personName) return data;
      const resp = await fetchWithRetry(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ singerName: personName, depth: 1 })
      }, { retries: 2, baseDelay: 600 });
      const text = await resp.text();
      let more; try { more = text ? JSON.parse(text) : {}; } catch (_) { more = {}; }
      const familyList = (more?.family || more?.center?.family || []);
      if (familyList && familyList.length) {
        return { ...data, family: Array.isArray(data.family) && data.family.length ? data.family : familyList };
      }
    } catch (_) {}
    return data;
  };

  /* const exportAsJSON = () => {
    // Disabled per request: JSON export commented out
    const snapshot = {};
  }; */

  const toCSV = (rows, headers) => {
    const escape = (v) => {
      const s = v == null ? '' : String(v);
      if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
      return s;
    };
    const lines = [headers.join(',')];
    rows.forEach(r => {
      lines.push(headers.map(h => escape(r[h])).join(','));
    });
    return lines.join('\n');
  };
  const exportAsCSV = async () => {
    setIsExporting(true);
    const valToText = (v) => {
      if (v == null) return '';
      if (Array.isArray(v)) return v.filter(Boolean).join('; ');
      if (typeof v === 'object') {
        // Favor common string-like props; otherwise JSON
        return v.full_name || v.opera_name || v.title || v.name || JSON.stringify(v);
      }
      return String(v);
    };
    const SOURCE_FIELD_KEYS = {
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
    const pickSourceValue = (entity, key) => {
      if (!entity) return '';
      const candidates = SOURCE_FIELD_KEYS[key] || [];
      for (const candidate of candidates) {
        if (Object.prototype.hasOwnProperty.call(entity, candidate)) {
          const value = entity[candidate];
          if (value !== undefined && value !== null && value !== '') return value;
        }
      }
      return '';
    };

    // Collect names/ids to fetch for export-only enrichment (no UI changes)
    const personNamesToFetch = new Set();
    const operaNamesToFetch = new Set();
    const bookTitlesToFetch = new Set();
    const operaTypedIdsToFetch = new Set(); // e.g. 'opera:654' or 'opera:great scott'

    (networkData.nodes || []).forEach(n => {
      if (n.type === 'person' && n.name) {
        const hasAnySource =
          pickSourceValue(n, 'spelling') ||
          pickSourceValue(n, 'voiceType') ||
          pickSourceValue(n, 'dates') ||
          pickSourceValue(n, 'birthplace');
        const hasDetails = n.voiceType || n.birthYear || n.deathYear || n.birthplace || n.citizen || n.birthplace;
        if (!hasAnySource || !hasDetails) personNamesToFetch.add(n.name);
      } else if (n.type === 'opera' && n.name) {
        operaNamesToFetch.add(n.name);
      } else if (n.type === 'book' && n.name) {
        bookTitlesToFetch.add(n.name);
      }
    });

    (networkData.links || []).forEach(l => {
      const lbl = (l.label || '').toLowerCase();
      const sId = typeof l.source === 'string' ? l.source : (l.source?.id || '');
      const tId = typeof l.target === 'string' ? l.target : (l.target?.id || '');
      const sNode = (networkData.nodes || []).find(x => x.id === sId);
      const tNode = (networkData.nodes || []).find(x => x.id === tId);
      if ((lbl.startsWith('premiered') || lbl === 'composed')) {
        if (sNode?.type === 'opera' && sNode.name) operaNamesToFetch.add(sNode.name);
        if (tNode?.type === 'opera' && tNode.name) operaNamesToFetch.add(tNode.name);
      }
      if ((lbl === 'authored' || lbl === 'edited')) {
        if (sNode?.type === 'book' && sNode.name) bookTitlesToFetch.add(sNode.name);
        if (tNode?.type === 'book' && tNode.name) bookTitlesToFetch.add(tNode.name);
      }
      if (sNode?.type === 'person' && sNode.name) personNamesToFetch.add(sNode.name);
      if (tNode?.type === 'person' && tNode.name) personNamesToFetch.add(tNode.name);

      // Capture any typed opera ids present directly on links (when opera node isn't in graph)
      if (typeof sId === 'string' && sId.startsWith('opera:')) operaTypedIdsToFetch.add(sId);
      if (typeof tId === 'string' && tId.startsWith('opera:')) operaTypedIdsToFetch.add(tId);
    });

    // Fetch details in parallel (scoped to export)
    const personDetails = new Map();
    const operaDetails = new Map(); // keyed by opera name (original case)
    const bookDetails = new Map();
    const operaNameByTypedId = new Map(); // keyed by typed id e.g. 'opera:654' => 'Rigoletto'
    try {
      const tasks = [];
      for (const full_name of Array.from(personNamesToFetch)) {
        tasks.push(async () => {
          try {
            const resp = await fetch(`${API_BASE}/singer/network`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ singerName: full_name, depth: 1 })
            });
            if (handleRateLimitResponse(resp, undefined, { suppressMessage: true })) return;
            const data = await resp.json();
            if (resp.ok && data && data.center) personDetails.set(full_name, data);
          } catch (_) {}
        });
      }
      for (const operaName of Array.from(operaNamesToFetch)) {
        tasks.push(async () => {
          try {
            const resp = await fetch(`${API_BASE}/opera/details`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ operaName })
            });
            if (handleRateLimitResponse(resp, undefined, { suppressMessage: true })) return;
            const data = await resp.json();
            if (resp.ok && data) operaDetails.set(operaName, data);
          } catch (_) {}
        });
      }
      for (const title of Array.from(bookTitlesToFetch)) {
        tasks.push(async () => {
          try {
            const resp = await fetch(`${API_BASE}/book/details`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ bookTitle: title })
            });
            if (handleRateLimitResponse(resp, undefined, { suppressMessage: true })) return;
            const data = await resp.json();
            if (resp.ok && data) bookDetails.set(title, data);
          } catch (_) {}
        });
      }
      for (const typedId of Array.from(operaTypedIdsToFetch)) {
        tasks.push(async () => {
          try {
            const raw = String(typedId.slice(6) || '').trim();
            if (!raw) return;
            const resp = await fetch(`${API_BASE}/opera/details`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ operaId: raw, opera_id: raw, operaName: raw })
            });
            if (handleRateLimitResponse(resp, undefined, { suppressMessage: true })) return;
            const data = await resp.json();
            const name = data?.opera?.opera_name;
            if (resp.ok && name) {
              operaNameByTypedId.set(typedId, name);
              if (!operaDetails.has(name)) operaDetails.set(name, data);
            }
          } catch (_) {}
        });
      }
      await runWithLimit(tasks, 4);
    } catch (_) {}
    const getNodeSources = (n) => {
      const empty = { spellingSource: '', voiceTypeSource: '', datesSource: '', birthplaceSource: '' };
      // Prefer direct properties on node
      const fromNode = {
        spellingSource: pickSourceValue(n, 'spelling'),
        voiceTypeSource: pickSourceValue(n, 'voiceType'),
        datesSource: pickSourceValue(n, 'dates'),
        birthplaceSource: pickSourceValue(n, 'birthplace')
      };
      if (fromNode.spellingSource || fromNode.voiceTypeSource || fromNode.datesSource || fromNode.birthplaceSource) {
        return {
          spellingSource: valToText(fromNode.spellingSource),
          voiceTypeSource: valToText(fromNode.voiceTypeSource),
          datesSource: valToText(fromNode.datesSource),
          birthplaceSource: valToText(fromNode.birthplaceSource)
        };
      }
      // Fall back to fetched person details, then itemDetails center/lists by name match
      const name = n.name || n.id;
      const pd = personDetails.get(name);
      if (pd && pd.center) {
        const c = pd.center;
        return {
          spellingSource: valToText(pickSourceValue(c, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(c, 'voiceType')),
          datesSource: valToText(pickSourceValue(c, 'dates')),
          birthplaceSource: valToText(pickSourceValue(c, 'birthplace'))
        };
      }
      const center = (itemDetails && itemDetails.center) ? itemDetails.center : null;
      const matchByName = (arr) => (arr || []).find(x => (x && (x.full_name === name || x.name === name)));
      if (center && (center.full_name === name)) {
        return {
          spellingSource: valToText(pickSourceValue(center, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(center, 'voiceType')),
          datesSource: valToText(pickSourceValue(center, 'dates')),
          birthplaceSource: valToText(pickSourceValue(center, 'birthplace'))
        };
      }
      const t = matchByName(itemDetails?.teachers);
      if (t) {
        return {
          spellingSource: valToText(pickSourceValue(t, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(t, 'voiceType')),
          datesSource: valToText(pickSourceValue(t, 'dates')),
          birthplaceSource: valToText(pickSourceValue(t, 'birthplace'))
        };
      }
      const s = matchByName(itemDetails?.students);
      if (s) {
        return {
          spellingSource: valToText(pickSourceValue(s, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(s, 'voiceType')),
          datesSource: valToText(pickSourceValue(s, 'dates')),
          birthplaceSource: valToText(pickSourceValue(s, 'birthplace'))
        };
      }
      const f = matchByName(itemDetails?.family);
      if (f) {
        return {
          spellingSource: valToText(pickSourceValue(f, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(f, 'voiceType')),
          datesSource: valToText(pickSourceValue(f, 'dates')),
          birthplaceSource: valToText(pickSourceValue(f, 'birthplace'))
        };
      }
      // For non-person nodes or if nothing found, return empty
      return empty;
    };

    const nameById = (id) => {
      const n = (networkData.nodes || []).find(x => x.id === id);
      return (n && (n.name || n.opera_name || n.title || n.id)) || id;
    };
    const getCenterName = () => {
      if (itemDetails?.center?.full_name) return itemDetails.center.full_name;
      if (itemDetails?.opera?.opera_name) return itemDetails.opera.opera_name;
      if (itemDetails?.book?.title) return itemDetails.book.title;
      return '';
    };
    // Relationship source resolver will be defined after export-only fetches
    // Nodes CSV (persons/operas/books): user-friendly headers; omit id
    const nodeHeaders = ['Name','Type','Voice Type','Birth year','Death year','Birthplace','Spelling source','Voice type source','Dates source','Birthplace source'];
    const getNodeValues = (n) => {
      const center = (itemDetails && itemDetails.center) ? itemDetails.center : null;
      const name = n.name || n.id;
      const matchByName = (arr) => (arr || []).find(x => (x && (x.full_name === name || x.name === name)));
      // Start with existing node values
      let voiceType = n.voiceType || '';
      let birthYear = n.birthYear || '';
      let deathYear = n.deathYear || '';
      let birthplace = n.birthplace || n.citizen || '';
      // If missing, try center person
      if ((!voiceType || !birthYear || !deathYear || !birthplace) && center && center.full_name === name) {
        voiceType = voiceType || center.voice_type || '';
        birthYear = birthYear || center.birth_year || '';
        deathYear = deathYear || center.death_year || '';
        birthplace = birthplace || center.birthplace || center.citizen || '';
      }
      // Then try teachers/students/family entries
      if (!voiceType || !birthYear || !deathYear || !birthplace) {
        const t = matchByName(itemDetails?.teachers);
        const s = matchByName(itemDetails?.students);
        const f = matchByName(itemDetails?.family);
        const src = t || s || f || null;
        if (src) {
          voiceType = voiceType || src.voice_type || '';
          birthYear = birthYear || src.birth_year || '';
          deathYear = deathYear || src.death_year || '';
          birthplace = birthplace || src.birthplace || src.citizen || '';
        }
      }
      return { voiceType, birthYear, deathYear, birthplace };
    };
    const nodeRows = (networkData.nodes || []).map(n => {
      const { voiceType, birthYear, deathYear, birthplace } = getNodeValues(n);
      const src = getNodeSources(n);
      return {
        'Name': n.name,
        'Type': n.type,
        'Voice Type': voiceType,
        'Birth year': birthYear,
        'Death year': deathYear,
        'Birthplace': birthplace,
        'Spelling source': src.spellingSource,
        'Voice type source': src.voiceTypeSource,
        'Dates source': src.datesSource,
        'Birthplace source': src.birthplaceSource
      };
    });
    // Relationship source resolver (local to export)
    const getRelationshipSource = (l) => {
      if (l.sourceInfo) return valToText(l.sourceInfo);
      const centerName = getCenterName();
      const sName = nameById(typeof l.source === 'string' ? l.source : (l.source?.id || ''));
      const tName = nameById(typeof l.target === 'string' ? l.target : (l.target?.id || ''));
      const label = (l.label || '').toLowerCase();

      if (label === 'taught') {
        if (centerName && sName === centerName) {
          const m = (itemDetails?.students || []).find(x => x?.full_name === tName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        } else if (centerName && tName === centerName) {
          const m = (itemDetails?.teachers || []).find(x => x?.full_name === sName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        }
      }
      if (label === 'family' || label === 'parent' || label === 'spouse' || label === 'sibling' || label === 'grandparent') {
        if (centerName && sName === centerName) {
          const m = (itemDetails?.family || []).find(x => x?.full_name === tName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        } else if (centerName && tName === centerName) {
          const m = (itemDetails?.family || []).find(x => x?.full_name === sName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        }
      }
      if (label.startsWith('premiered')) {
        if (itemDetails?.premieredRoles && centerName && sName === centerName) {
          const m = (itemDetails.premieredRoles || []).find(x => x?.opera_name === tName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'composed') {
        if (itemDetails?.works?.composedOperas && centerName && sName === centerName) {
          const m = (itemDetails.works.composedOperas || []).find(x => x?.title === tName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'authored') {
        if (itemDetails?.book?.title && tName === (itemDetails.book.title)) {
          const m = (itemDetails?.authors || []).find(x => x?.author === sName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'edited') {
        if (itemDetails?.book?.title && tName === (itemDetails.book.title)) {
          const m = (itemDetails?.editors || []).find(x => x?.editor === sName);
          if (m && m.source) return valToText(m.source);
        }
      }
      return valToText(l.relationship_source || l.teacher_rel_source || '');
    };
    // Relationship source resolver (local; uses fetched maps)
    const getRelationshipSource2 = (l) => {
      if (l.sourceInfo) return valToText(l.sourceInfo);
      const centerName = getCenterName();
      const sName = nameById(typeof l.source === 'string' ? l.source : (l.source?.id || ''));
      const tName = nameById(typeof l.target === 'string' ? l.target : (l.target?.id || ''));
      const label = (l.label || '').toLowerCase();

      if (label === 'taught') {
        if (centerName && sName === centerName) {
          const fromItems = (itemDetails?.students || []);
          const fromFetched = (personDetails.get(sName)?.students || []);
          const m = [...fromItems, ...fromFetched].find(x => x?.full_name === tName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        } else if (centerName && tName === centerName) {
          const fromItems = (itemDetails?.teachers || []);
          const fromFetched = (personDetails.get(tName)?.teachers || []);
          const m = [...fromItems, ...fromFetched].find(x => x?.full_name === sName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        }
      }
      if (label === 'family' || label === 'parent' || label === 'spouse' || label === 'sibling' || label === 'grandparent') {
        if (centerName && sName === centerName) {
          const fromItems = (itemDetails?.family || []);
          const fromFetched = (personDetails.get(sName)?.family || []);
          const m = [...fromItems, ...fromFetched].find(x => x?.full_name === tName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        } else if (centerName && tName === centerName) {
          const fromItems = (itemDetails?.family || []);
          const fromFetched = (personDetails.get(tName)?.family || []);
          const m = [...fromItems, ...fromFetched].find(x => x?.full_name === sName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        }
      }
      if (label.startsWith('premiered')) {
        if (centerName && sName === centerName) {
          const roles = (itemDetails?.premieredRoles || personDetails.get(sName)?.premieredRoles || []);
          const m = roles.find(x => x?.opera_name === tName);
          if (m && m.source) return valToText(m.source);
          // also try opera details by singer
          const od = operaDetails.get(tName);
          if (od && Array.isArray(od.premieredRoles)) {
            const r = od.premieredRoles.find(x => x?.singer === sName);
            if (r && r.source) return valToText(r.source);
          }
        }
      }
      if (label === 'composed') {
        if (centerName && sName === centerName) {
          const comps = (itemDetails?.works?.composedOperas || personDetails.get(sName)?.works?.composedOperas || []);
          const m = comps.find(x => x?.title === tName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'authored') {
        const det = bookDetails.get(tName);
        if (det && Array.isArray(det.authors)) {
          const m = det.authors.find(x => x?.author === sName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'edited') {
        const det = bookDetails.get(tName);
        if (det && Array.isArray(det.editors)) {
          const m = det.editors.find(x => x?.editor === sName);
          if (m && m.source) return valToText(m.source);
        }
      }
      return valToText(l.relationship_source || l.teacher_rel_source || '');
    };

    // Helper to derive a human-friendly display for a node endpoint (object or typed id string)
    const toTitleCase = (s) => String(s || '')
      .split(' ')
      .map(w => w ? (w.charAt(0).toUpperCase() + w.slice(1)) : '')
      .join(' ');
    const displayForEndpoint = (endpoint) => {
      if (endpoint && typeof endpoint === 'object') {
        return endpoint.name || endpoint.opera_name || endpoint.title || endpoint.full_name || valToText(endpoint);
      }
      const id = String(endpoint || '').trim();
      if (!id) return '';
      const colon = id.indexOf(':');
      if (colon > 0) {
        const type = id.slice(0, colon).toLowerCase();
        const raw = id.slice(colon + 1);
        if (type === 'opera') {
          return operaNameByTypedId.get(id) || toTitleCase(raw);
        }
        if (type === 'book') {
          // Best-effort: try to recover title from existing nodes or title-case the raw part
          const byNode = nameById(id);
          if (byNode && byNode !== id) return byNode;
          return toTitleCase(raw);
        }
        if (type === 'person') {
          const byNode = nameById(id);
          if (byNode && byNode !== id) return byNode;
          return toTitleCase(raw);
        }
      }
      // Non-typed id or already a name
      return nameById(id);
    };

    // Relationships CSV: remove 'type', include only relationship source (resolved from itemDetails/fetched or link)
    const linkHeaders = ['source','label','target','role','relationshipSource'];
    const linkRows = (networkData.links || []).map(l => {
      const relSrc = l.teacher_rel_source_url ? valToText(l.teacher_rel_source_url) : getRelationshipSource2(l);
      const sourceDisplay = displayForEndpoint(l.source);
      const targetDisplay = displayForEndpoint(l.target);
      return {
        source: sourceDisplay,
        label: l.label || '',
        target: targetDisplay,
        role: l.role || '',
        relationshipSource: relSrc
      };
    });

    const nodesCSV = toCSV(nodeRows, nodeHeaders);
    const linksCSV = toCSV(linkRows, linkHeaders);

    const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
    downloadBlob(new Blob([nodesCSV], { type: 'text/csv;charset=utf-8' }), `nodes-${ts}.csv`);
    downloadBlob(new Blob([linksCSV], { type: 'text/csv;charset=utf-8' }), `links-${ts}.csv`);
    setIsExporting(false);
    showHelperMessage('Export complete. Check your downloads.', 2400);
  };

  const goBack = () => {
    const past = historyRef.current.past;
    if (!past.length) return;
    // Move current to future
    const current = createSnapshot();
    historyRef.current.future.push(current);
    const snap = past.pop();
    applySnapshot(snap);
    setHistoryCounts({ past: past.length, future: historyRef.current.future.length });
  };
  const goForward = () => {
    const future = historyRef.current.future;
    if (!future.length) return;
    // Move current to past
    const current = createSnapshot();
    historyRef.current.past.push(current);
    const snap = future.pop();
    applySnapshot(snap);
    setHistoryCounts({ past: historyRef.current.past.length, future: future.length });
  };

  const resolveApiBase = () => {
    try {
      if (typeof window !== 'undefined') {
        const override = window.__CMG_API_BASE;
        if (typeof override === 'string' && override.trim()) {
          return override.trim().replace(/\/$/, '');
        }
      }
    } catch (_) {}

    let envBase = '';
    if (typeof import.meta !== 'undefined' && import.meta.env && typeof import.meta.env.VITE_API_BASE === 'string') {
      envBase = import.meta.env.VITE_API_BASE;
    } else if (typeof process !== 'undefined' && process?.env?.REACT_APP_API_BASE) {
      envBase = process.env.REACT_APP_API_BASE;
    }

    envBase = (envBase || '').trim();
    if (envBase) {
      return envBase.replace(/\/$/, '');
    }

    if (typeof window !== 'undefined') {
      const { protocol, hostname } = window.location;
      if (hostname === 'localhost' || hostname === '127.0.0.1') {
        return 'http://localhost:3001';
      }
      return `${protocol}//${hostname}`;
    }

    return 'http://localhost:3001';
  };

  const API_BASE = resolveApiBase();
  // Voice types configuration with colors
  const VOICE_TYPES = [
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
    { name: 'Composer', color: '#7c8b23' }, // Pale blue
    { name: 'Conductor', color: '#7c8b23' }, // Pale blue
    { name: 'Instrumentalist', color: '#7c8b23' }, // Pale blue
    { name: 'Opera director', color: '#7c8b23' }, // Pale blue
    
    // Professional Roles - Education
    { name: 'Teacher, other', color: '#7c8b23' }, // Pale blue
    { name: 'Vocal coach', color: '#7c8b23' }, // Pale blue
    { name: 'Speech Language Pathologist', color: '#7c8b23' }, // Pale blue
    
    // Professional Roles - Literary/Creative
    { name: 'Librettist', color: '#7c8b23' }, // Pale blue
    { name: 'Critic', color: '#7c8b23' }, // Pale blue
    { name: 'Actor', color: '#7c8b23' }, // Pale blue
    { name: 'Inventor', color: '#7c8b23' }, // Pale blue
    
    // Other/Special Categories
    { name: 'Non-singing', color: '#7c8b23' }, // Pale blue
    { name: 'Unknown', color: '#7c8b23' } // Pale blue
  ];
  const TYPE_FILTER_COLORS = {
    Opera: '#8b5cf6',
    Book: '#14b8a6'
  };

  // Enhanced filter setter functions
  const updateSelectedVoiceTypes = (newSelection) => {
    setSelectedVoiceTypes(newSelection);
  };

  const updateBirthYearRange = (newRange, { userInitiated = false } = {}) => {
    setBirthYearRange(prev => {
      const next = Array.isArray(newRange) ? [...newRange] : prev;
      return next;
    });
    if (userInitiated) {
      birthRangeIsUserSetRef.current = true;
      setBirthRangeIsUserSet(true);
    }
  };

  const updateDeathYearRange = (newRange, { userInitiated = false } = {}) => {
    setDeathYearRange(prev => {
      const next = Array.isArray(newRange) ? [...newRange] : prev;
      return next;
    });
    if (userInitiated) {
      deathRangeIsUserSetRef.current = true;
      setDeathRangeIsUserSet(true);
    }
  };

  const parseYearValue = (value) => {
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

  const normalizePersonNode = (node) => {
    if (!node || node.type !== 'person') return node;
    let normalized = node;
    const ensureClone = () => {
      if (normalized === node) {
        normalized = { ...node };
      }
    };
    const props = (normalized.properties && typeof normalized.properties === 'object')
      ? normalized.properties
      : null;
    const firstDefined = (...values) => {
      for (const value of values) {
        if (value !== null && value !== undefined) {
          return value;
        }
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
      if (voiceText) {
        ensureClone();
        normalized.voiceType = voiceText;
      }
    }

    const existingBirthplace = canonicalizePlaceText(normalized.birthplace);
    const birthplaceCandidate = firstDefined(
      normalized.birth_place,
      normalized.citizen,
      props && props.birthplace,
      props && props.birth_place,
      props && props.birthplace_display,
      props && props.birthplace_label,
      props && props.place_of_birth,
      props && props.birthplace_text,
      props && props.birth_location,
      props && props.citizen
    );
    const birthplaceText = existingBirthplace || canonicalizePlaceText(birthplaceCandidate);
    if (birthplaceText && birthplaceText !== normalized.birthplace) {
      ensureClone();
      normalized.birthplace = birthplaceText;
    }
    const citizenCandidate = firstDefined(normalized.citizen, props && props.citizen);
    const citizenText = canonicalizePlaceText(citizenCandidate);
    if (citizenText && citizenText !== normalized.citizen) {
      ensureClone();
      normalized.citizen = citizenText;
    }

    const birthCandidate = firstDefined(
      normalized.birthYear,
      normalized.birth_year,
      normalized.birth,
      props && props.birthYear,
      props && props.birth_year,
      props && props.birth,
      props && props.birthDate,
      props && props.birth_date,
      props && props.date_of_birth,
      props && props.birthyear,
      props && props.birth_low,
      props && props.birth_high
    );
    const deathCandidate = firstDefined(
      normalized.deathYear,
      normalized.death_year,
      normalized.death,
      props && props.deathYear,
      props && props.death_year,
      props && props.death,
      props && props.deathDate,
      props && props.death_date,
      props && props.date_of_death,
      props && props.deathyear,
      props && props.death_low,
      props && props.death_high
    );
    const birthYear = parseYearValue(birthCandidate);
    const deathYear = parseYearValue(deathCandidate);
    if (Number.isFinite(birthYear) && normalized.birthYear !== birthYear) {
      ensureClone();
      normalized.birthYear = birthYear;
    }
    if (Number.isFinite(deathYear) && normalized.deathYear !== deathYear) {
      ensureClone();
      normalized.deathYear = deathYear;
    }
    return normalized;
  };

  const extendDateRangesForNodes = (nodesList = [], options = {}) => {
    const { resetUserRangeFlags = false } = options;
    if (!Array.isArray(nodesList) || nodesList.length === 0) return;
    let [birthMin, birthMax] = birthYearRange;
    let [deathMin, deathMax] = deathYearRange;
    let birthChanged = false;
    let deathChanged = false;

    nodesList.forEach((node) => {
      if (node && node.type === 'person') {
        const normalized = normalizePersonNode(node);
        if (normalized && normalized !== node) {
          Object.assign(node, normalized);
        }
        const birthValue =
          node.birthYear ??
          node.birth_year ??
          (node.birth ? (node.birth.year ?? node.birth.low ?? node.birth.high ?? node.birth) : null);
        const deathValue =
          node.deathYear ??
          node.death_year ??
          (node.death ? (node.death.year ?? node.death.low ?? node.death.high ?? node.death) : null);

        const birthYear = parseYearValue(birthValue);
        if (!Number.isNaN(birthYear)) {
          if (birthYear < birthMin) { birthMin = birthYear; birthChanged = true; }
          if (birthYear > birthMax) { birthMax = birthYear; birthChanged = true; }
        }

        const deathYear = parseYearValue(deathValue);
        if (!Number.isNaN(deathYear)) {
          if (deathYear < deathMin) { deathMin = deathYear; deathChanged = true; }
          if (deathYear > deathMax) { deathMax = deathYear; deathChanged = true; }
        }
      }
    });

    const allowBirthUpdate = resetUserRangeFlags || !birthRangeIsUserSetRef.current;
    const allowDeathUpdate = resetUserRangeFlags || !deathRangeIsUserSetRef.current;

    if (birthChanged && allowBirthUpdate) {
      updateBirthYearRange([birthMin, birthMax], { userInitiated: false });
      if (resetUserRangeFlags && (birthRangeIsUserSetRef.current || birthRangeIsUserSet)) {
        birthRangeIsUserSetRef.current = false;
        setBirthRangeIsUserSet(false);
      }
    }
    if (deathChanged && allowDeathUpdate) {
      updateDeathYearRange([deathMin, deathMax], { userInitiated: false });
      if (resetUserRangeFlags && (deathRangeIsUserSetRef.current || deathRangeIsUserSet)) {
        deathRangeIsUserSetRef.current = false;
        setDeathRangeIsUserSet(false);
      }
    }
  };

  // Filter helper functions
  const toggleVoiceTypeFilter = (voiceType) => {
    const newSelection = new Set(selectedVoiceTypes);
    if (newSelection.has(voiceType)) {
      newSelection.delete(voiceType);
    } else {
      newSelection.add(voiceType);
    }
    updateSelectedVoiceTypes(newSelection);
  };

  const normalizePlaceName = (value) => {
    const canonical = canonicalizePlaceText(value);
    return canonical ? canonical.toLowerCase() : '';
  };

  const toggleBirthplaceFilter = (birthplace) => {
    const key = normalizePlaceName(birthplace);
    if (!key) return;
    const newSelection = new Set(selectedBirthplaces);
    if (newSelection.has(key)) {
      newSelection.delete(key);
    } else {
      newSelection.add(key);
    }
    setSelectedBirthplaces(newSelection);
  };

  const computeRangesFromNodes = (nodesList = []) => {
    const defaults = {
      birthRange: [...DEFAULT_BIRTH_RANGE],
      deathRange: [...DEFAULT_DEATH_RANGE]
    };
    const personNodes = Array.isArray(nodesList)
      ? nodesList
          .filter(node => node && node.type === 'person')
          .map(node => {
            const normalized = normalizePersonNode(node);
            if (normalized && normalized !== node) {
              Object.assign(node, normalized);
              return normalized;
            }
            return node;
          })
      : [];

    if (personNodes.length === 0) {
      return defaults;
    }

    const birthYears = personNodes
      .map(node => node.birthYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year, 10));

    const deathYears = personNodes
      .map(node => node.deathYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year, 10));

    const birthRange = birthYears.length > 0
      ? [Math.min(...birthYears), Math.max(...birthYears)]
      : defaults.birthRange;

    const deathRange = deathYears.length > 0
      ? [Math.min(...deathYears), Math.max(...deathYears)]
      : defaults.deathRange;

    return { birthRange, deathRange };
  };

  const resetFiltersForNodeSet = (nodesList) => {
    updateSelectedVoiceTypes(new Set());
    setSelectedBirthplaces(new Set());
    const { birthRange, deathRange } = computeRangesFromNodes(nodesList ?? networkData.nodes);
    updateBirthYearRange(birthRange, { userInitiated: false });
    updateDeathYearRange(deathRange, { userInitiated: false });
    birthRangeIsUserSetRef.current = false;
    setBirthRangeIsUserSet(false);
    deathRangeIsUserSetRef.current = false;
    setDeathRangeIsUserSet(false);
  };

  const clearFiltersForNewSearch = (nodesList = []) => {
    const nextNodes = (Array.isArray(nodesList) && nodesList.length > 0) ? nodesList : networkData.nodes;
    resetFiltersForNodeSet(nextNodes);
    setShowFilterPanel(false);
  };

  const clearAllFilters = () => {
    resetFiltersForNodeSet();
  };

  // Helper function to get date ranges from current network data
  const getDateRanges = () => {
    const personNodes = networkData.nodes
      .filter(node => node.type === 'person')
      .map(node => {
        const normalized = normalizePersonNode(node);
        if (normalized && normalized !== node) {
          Object.assign(node, normalized);
          return normalized;
        }
        return node;
      });
    
    const birthYears = personNodes
      .map(node => node.birthYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year));
    
    const deathYears = personNodes
      .map(node => node.deathYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year));
    
    const minBirth = birthYears.length > 0 ? Math.min(...birthYears) : DEFAULT_BIRTH_RANGE[0];
    const maxBirth = birthYears.length > 0 ? Math.max(...birthYears) : DEFAULT_BIRTH_RANGE[1];
    const minDeath = deathYears.length > 0 ? Math.min(...deathYears) : DEFAULT_DEATH_RANGE[0];
    const maxDeath = deathYears.length > 0 ? Math.max(...deathYears) : DEFAULT_DEATH_RANGE[1];
    
    return {
      birthRange: [minBirth, maxBirth],
      deathRange: [minDeath, maxDeath]
    };
  };
  const getVisibleBirthplaces = () => {
    const personNodes = networkData.nodes.filter(node => node.type === 'person');
    const counts = new Map(); // normalized -> { name, count }
    personNodes.forEach(node => {
      const normalized = normalizePersonNode(node);
      if (normalized && normalized !== node) {
        Object.assign(node, normalized);
      }
      if (!isNodeVisibleWithoutBirthplaceFilter(node)) return;
      const props = (node && typeof node.properties === 'object') ? node.properties : null;
      const placeValue =
        node.birthplace ??
        node.citizen ??
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
      if (!key) return;
      if (!counts.has(key)) counts.set(key, { name: canonical, count: 0 });
      counts.get(key).count += 1;
    });
    return Array.from(counts.values()).sort((a, b) => a.name.localeCompare(b.name));
  };

  // Helper: visibility excluding voice-type filter (used for voice-type counts)
  const isNodeVisibleWithoutVoiceFilter = (node) => {
    if (node.type === 'person') {
      const normalized = normalizePersonNode(node);
      if (normalized && normalized !== node) {
        Object.assign(node, normalized);
      }
      // Birthplace filter
      if (selectedBirthplaces.size > 0) {
        const props = (node && typeof node.properties === 'object') ? node.properties : null;
        const placeRaw =
          node.birthplace ??
          node.citizen ??
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
        const match = place && selectedBirthplaces.has(place.toLowerCase());
        if (!match) return false;
      }

      if (birthRangeIsUserSet && node.birthYear) {
        const birthYear = parseInt(node.birthYear);
        if (!isNaN(birthYear)) {
          if (birthYear < birthYearRange[0] || birthYear > birthYearRange[1]) {
            return false;
          }
        }
      }

      if (deathRangeIsUserSet && node.deathYear) {
        const deathYear = parseInt(node.deathYear);
        if (!isNaN(deathYear)) {
          if (deathYear < deathYearRange[0] || deathYear > deathYearRange[1]) {
            return false;
          }
        }
      }
    }
    return true;
  };

  // Helper: visibility excluding birthplace filter (used for birthplace counts)
  const isNodeVisibleWithoutBirthplaceFilter = (node) => {
    if (node.type === 'person') {
      const normalized = normalizePersonNode(node);
      if (normalized && normalized !== node) {
        Object.assign(node, normalized);
      }
      // Voice type filter
      if (selectedVoiceTypes.size > 0) {
        const voiceTypeMatch = !node.voiceType ? 
          selectedVoiceTypes.has('Unknown') : 
          selectedVoiceTypes.has(node.voiceType);
        if (!voiceTypeMatch) return false;
      }

      if (birthRangeIsUserSet && node.birthYear) {
        const birthYear = parseInt(node.birthYear);
        if (!isNaN(birthYear)) {
          if (birthYear < birthYearRange[0] || birthYear > birthYearRange[1]) {
            return false;
          }
        }
      }

      if (deathRangeIsUserSet && node.deathYear) {
        const deathYear = parseInt(node.deathYear);
        if (!isNaN(deathYear)) {
          if (deathYear < deathYearRange[0] || deathYear > deathYearRange[1]) {
            return false;
          }
        }
      }
    }
    return true;
  };

  // Derive dynamic voice types from currently visualized person nodes
  const getVisibleVoiceTypes = () => {
    const personNodes = networkData.nodes.filter(node => node.type === 'person');
    const counts = new Map();
    const colors = new Map();
    const register = (name, color) => {
      const nextCount = (counts.get(name) || 0) + 1;
      counts.set(name, nextCount);
      if (color && !colors.has(name)) {
        colors.set(name, color);
      }
    };
    const voiceColorMap = {};
    VOICE_TYPES.forEach(v => { voiceColorMap[v.name] = v.color; });
    personNodes.forEach(node => {
      const normalized = normalizePersonNode(node);
      if (normalized && normalized !== node) {
        Object.assign(node, normalized);
      }
      if (!isNodeVisibleWithoutVoiceFilter(node)) return;
      const vtRaw = node.voiceType && String(node.voiceType).trim();
      const voiceName = vtRaw && vtRaw.length > 0 ? vtRaw : 'Unknown';
      register(voiceName, voiceColorMap[voiceName] || '#6B7280');
    });
    networkData.nodes.forEach((node) => {
      if (node.type === 'opera') {
        register('Opera', TYPE_FILTER_COLORS.Opera);
      } else if (node.type === 'book') {
        register('Book', TYPE_FILTER_COLORS.Book);
      }
    });
    return Array.from(counts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([name, count]) => ({
        name,
        color: colors.get(name) || voiceColorMap[name] || '#6B7280',
        count
      }));
  };

  const resetDateRanges = () => {
    const { birthRange, deathRange } = getDateRanges();
    updateBirthYearRange(birthRange, { userInitiated: false });
    updateDeathYearRange(deathRange, { userInitiated: false });
    birthRangeIsUserSetRef.current = false;
    setBirthRangeIsUserSet(false);
    deathRangeIsUserSetRef.current = false;
    setDeathRangeIsUserSet(false);
  };

  const isNodeVisible = (node) => {
    // For person nodes, check all applicable filters
    if (node.type === 'person') {
      const normalized = normalizePersonNode(node);
      if (normalized && normalized !== node) {
        Object.assign(node, normalized);
      }
      // Voice type filter
      if (selectedVoiceTypes.size > 0) {
        const rawVoice = node.voiceType && String(node.voiceType).trim();
        const voiceName = rawVoice && rawVoice.length > 0 ? rawVoice : 'Unknown';
        if (!selectedVoiceTypes.has(voiceName)) return false;
      }

      // Birthplace filter
      if (selectedBirthplaces.size > 0) {
        const place = node.birthplace || node.citizen || null;
        const match = place && selectedBirthplaces.has(normalizePlaceName(place));
        if (!match) return false;
      }
      
      if (birthRangeIsUserSet && node.birthYear) {
        const birthYear = parseInt(node.birthYear);
        if (!isNaN(birthYear)) {
          if (birthYear < birthYearRange[0] || birthYear > birthYearRange[1]) {
            return false;
          }
        }
      }
      
      if (deathRangeIsUserSet && node.deathYear) {
        const deathYear = parseInt(node.deathYear);
        if (!isNaN(deathYear)) {
          if (deathYear < deathYearRange[0] || deathYear > deathYearRange[1]) {
            return false;
          }
        }
      }
    }
    if (selectedVoiceTypes.size > 0) {
      if (node.type === 'opera' && !selectedVoiceTypes.has('Opera')) {
        return false;
      }
      if (node.type === 'book' && !selectedVoiceTypes.has('Book')) {
        return false;
      }
    }

    if ((birthRangeIsUserSetRef.current || deathRangeIsUserSetRef.current || birthRangeIsUserSet || deathRangeIsUserSet || selectedBirthplaces.size > 0) && node.type !== 'person') {
      return false;
    }

    // Show opera and book nodes by default (could add filters for these later)
    return true;
  };

  const isLinkVisible = (link) => {
    // A link is only visible if both its source and target nodes are visible
    const sourceNode = typeof link.source === 'string' ? 
      networkData.nodes.find(n => n.id === link.source) : link.source;
    const targetNode = typeof link.target === 'string' ? 
      networkData.nodes.find(n => n.id === link.target) : link.target;
    
    return sourceNode && targetNode && isNodeVisible(sourceNode) && isNodeVisible(targetNode);
  };
  const getFilterCounts = () => {
    const totalNodes = networkData.nodes.length;
    const visibleNodes = networkData.nodes.filter(isNodeVisible).length;
    return { totalNodes, visibleNodes };
  };

  useEffect(() => {
    try {
      let incomingResetToken = '';
      const params = new URLSearchParams(window.location.search);
      incomingResetToken = params.get('resetToken') || params.get('token') || '';
      if (!incomingResetToken) {
        const hash = window.location.hash || '';
        if (hash.startsWith('#')) {
          const hashParams = new URLSearchParams(hash.slice(1));
          incomingResetToken = hashParams.get('resetToken') || hashParams.get('token') || '';
        }
      }
      if (incomingResetToken) {
        setInitialResetToken(incomingResetToken);
        clearStoredToken();
        setToken('');
        setHasExecutedSearch(false);
      }
    } catch (_) {}
  }, []);

  // Initialize token on component mount
  useEffect(() => {
    let savedToken = null;
    let loginTs = null;
    try { savedToken = localStorage.getItem('token'); } catch (_) {}
    try { const rawTs = localStorage.getItem(TOKEN_LOGIN_TS_KEY); loginTs = rawTs ? parseInt(rawTs, 10) : null; } catch (_) {}
    if (!savedToken) return;
    const now = Date.now();
    const isExpired = !Number.isFinite(loginTs) || now - loginTs > LOGIN_MAX_AGE_MS;
    if (isExpired) {
      clearStoredToken();
      setError('Session expired. Please log in again.');
      setToken('');
      setHasExecutedSearch(false);
      return;
    }
    setToken(savedToken);
    try {
      const savedEmail = localStorage.getItem('userEmail');
      if (typeof savedEmail === 'string') setUserEmail(savedEmail);
    } catch (_) {}
    // If a support panel reopen was requested before a redirect, honor it now
    try {
      if (localStorage.getItem('cmgShowSupportAfterLogin') === '1') {
        setShowSupportPanel(true);
        localStorage.removeItem('cmgShowSupportAfterLogin');
      }
    } catch (_) {}
  }, []);

  useEffect(() => {
    if (!showFilterPanel) {
      setFilterSectionsOpen({ voice: false, birth: false, death: false, birthplaces: false });
    }
  }, [showFilterPanel]);

  useEffect(() => {
    if (!token) {
      supportPanelLoginFlagRef.current = false;
      setJustLoggedIn(false);
      try { setShowSupportPanel(false); } catch (_) {}
      return;
    }
    if (!justLoggedIn) return;
    if (supportPanelLoginFlagRef.current) return;
    supportPanelLoginFlagRef.current = true;
    try { setShowSupportPanel(false); } catch (_) {}
    const timer = setTimeout(() => {
      try { setShowSupportPanel(true); } catch (_) {}
      setJustLoggedIn(false);
    }, 180);
    return () => { clearTimeout(timer); };
  }, [token, justLoggedIn]);

  useEffect(() => {
    if (!token) {
      sessionRestoredRef.current = false;
      sessionPersistReadyRef.current = false;
      try { localStorage.removeItem(SESSION_SNAPSHOT_KEY); } catch (_) {}
      try { localStorage.removeItem(SESSION_SNAPSHOT_FILTERLESS_KEY); } catch (_) {}
      filtersResetRef.current = false;
      return;
    }
    if (sessionRestoredRef.current) {
      sessionPersistReadyRef.current = true;
      return;
    }
    let snapshotLoaded = false;
    try {
      const raw = localStorage.getItem(SESSION_SNAPSHOT_KEY);
      if (raw) {
        const snap = JSON.parse(raw);
        if (snap && typeof snap === 'object') {
          applySnapshot(snap, { restoreFilters: false });
          snapshotLoaded = true;
          filtersResetRef.current = true;
          try { localStorage.setItem(SESSION_SNAPSHOT_FILTERLESS_KEY, '1'); } catch (_) {}
        }
      }
    } catch (_) {}
    if (!snapshotLoaded && !filtersResetRef.current) {
      try {
        const marker = localStorage.getItem(SESSION_SNAPSHOT_FILTERLESS_KEY);
        if (marker !== '1') {
          clearAllFilters();
          localStorage.setItem(SESSION_SNAPSHOT_FILTERLESS_KEY, '1');
          filtersResetRef.current = true;
        }
      } catch (_) {}
    }
    sessionRestoredRef.current = true;
    sessionPersistReadyRef.current = true;
  }, [token]);

  useEffect(() => {
    if (!token || !sessionPersistReadyRef.current) return;
    try {
      const snap = createSnapshot();
      snap.snapshotVersion = 2;
      snap.searchQuery = searchQuery;
      try { snap.searchResults = JSON.parse(JSON.stringify(searchResults || [])); } catch (_) { snap.searchResults = []; }
      try { snap.originalSearchResults = JSON.parse(JSON.stringify(originalSearchResults || [])); } catch (_) { snap.originalSearchResults = snap.searchResults || []; }
      snap.selectedVoiceTypes = Array.from(selectedVoiceTypes || []);
      snap.selectedBirthplaces = Array.from(selectedBirthplaces || []);
      snap.birthYearRange = Array.isArray(birthYearRange) ? [...birthYearRange] : snap.birthYearRange;
      snap.deathYearRange = Array.isArray(deathYearRange) ? [...deathYearRange] : snap.deathYearRange;
      snap.showFilterPanel = !!showFilterPanel;
      snap.showPathPanel = !!showPathPanel;
      try { snap.pathInfo = pathInfo ? JSON.parse(JSON.stringify(pathInfo)) : null; } catch (_) { snap.pathInfo = null; }
      localStorage.setItem(SESSION_SNAPSHOT_KEY, JSON.stringify(snap));
      localStorage.setItem(SESSION_SNAPSHOT_FILTERLESS_KEY, '1');
    } catch (_) {}
  }, [
    token,
    networkData.nodes.length,
    networkData.links.length,
    currentView,
    searchType,
    searchQuery,
    searchResults,
    originalSearchResults,
    showFilterPanel,
    showPathPanel,
    selectedVoiceTypes,
    selectedBirthplaces,
    birthYearRange,
    deathYearRange,
    pathInfo,
    filtersVersion
  ]);

  // Filter application will be handled manually through button clicks or direct calls
  // No automatic reactive filter application to avoid infinite loops



  // Close context menu when clicking elsewhere (never recenter or modify zoom)
  useEffect(() => {
    const handleClick = (event) => {
      // Don't interfere with form inputs or if user is in the auth form
      if (event.target.tagName === 'INPUT' || 
          event.target.tagName === 'TEXTAREA' || 
          event.target.closest('form') ||
          !token) {
        return;
      }
      
      // Explicitly re-apply current zoom to avoid any external listeners causing recenter
      try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {}
      setContextMenu({ show: false, x: 0, y: 0, node: null });
      setLinkContextMenu(createLinkContextMenuState());
      setExpandSubmenu(null);
      // Clear any pending submenu timeout
      if (submenuTimeoutRef.current) {
        clearTimeout(submenuTimeoutRef.current);
      }
    };
    
    // Only add listener if user is logged in
    if (token && !pathInputFocused) {
      document.addEventListener('click', handleClick);
    }
    
    return () => document.removeEventListener('click', handleClick);
  }, [token, pathInputFocused]);

  // Fetch actual counts when context menu opens for any node that doesn't have them cached
  useEffect(() => {
    if (contextMenu.show && contextMenu.node) {
      const nodeId = contextMenu.node.id;
      // Fetch immediately so the Expand state is accurate at open
      if (!actualCounts[nodeId] && !fetchingCounts[nodeId] && !failedFetches[nodeId]) {
          setFetchingCounts(prev => ({ ...prev, [nodeId]: true }));
          fetchActualCounts(contextMenu.node)
            .catch(() => {
              setFailedFetches(prev => ({ ...prev, [nodeId]: true }));
            })
            .finally(() => {
              setFetchingCounts(prev => ({ ...prev, [nodeId]: false }));
            });
      }
    }
  }, [contextMenu.show, contextMenu.node?.id]);

  // Prefetch actual expandable relationship counts for newly added nodes to avoid delay on right-click
  const prevNodeIdsRef = useRef(new Set());
  useEffect(() => {
    if (!token || loading || isSearchingRef.current || currentView !== 'network') return;
    const cooldown = rateLimitedUntilRef.current || 0;
    if (cooldown && Date.now() < cooldown) return;
    const currentIds = new Set(networkData.nodes.map(n => n.id));
    const isExpansion = Boolean(isExpansionSimulation || shouldRunSimulation);
    const newlyAddedIds = [];
    currentIds.forEach((id) => {
      if (!prevNodeIdsRef.current.has(id)) newlyAddedIds.push(id);
    });
    prevNodeIdsRef.current = currentIds;

    if (newlyAddedIds.length === 0 || isExpansion) return;

    const fetchingIds = new Set(Object.keys(fetchingCounts).filter(k => fetchingCounts[k]));
    const PREFETCH_LIMIT = 2;
    const STAGGER_MS = 600;
    newlyAddedIds.slice(0, PREFETCH_LIMIT).forEach((id, index) => {
      if (actualCounts[id] || fetchingIds.has(id) || failedFetches[id]) return;
      const node = networkData.nodes.find(n => n.id === id);
      if (!node) return;
      setFetchingCounts(prev => ({ ...prev, [id]: true }));
      // Stagger requests slightly to avoid bursts
      setTimeout(() => {
        fetchActualCounts(node)
          .catch((err) => {
            // Don't permanently mark failed on rate limit; allow retry later
            if (err && err.status && err.status !== 429) {
              setFailedFetches(prev => ({ ...prev, [id]: true }));
            }
          })
          .finally(() => {
            setFetchingCounts(prev => ({ ...prev, [id]: false }));
          });
      }, index * STAGGER_MS);
    });
  }, [networkData.nodes, token, loading, currentView]);

  // Resize functionality removed; fixed height

  // Close context menu when clicking/right-clicking outside (never recenter or modify zoom)
  useEffect(() => {
    const handleClickOutside = (event) => {
      // For left-clicks, avoid interfering with active form inputs; for right-clicks, always allow closing
      const isFormTarget = event.target.tagName === 'INPUT' || event.target.tagName === 'TEXTAREA' || event.target.closest('form');
      if (event.type === 'click' && (isFormTarget || !token)) return;
      // Explicitly re-apply current zoom to avoid any external listeners causing recenter
      try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {}
      if (contextMenu.show && !event.target.closest('.context-menu')) {
        setContextMenu({ show: false, x: 0, y: 0, node: null });
      }
      if (linkContextMenu.show && !event.target.closest('.context-menu')) {
        setLinkContextMenu(createLinkContextMenuState());
      }
    };

    // Add listeners if any context menu is showing and user is logged in
    const shouldBind = (contextMenu.show || linkContextMenu.show) && token && !pathInputFocused;
    if (shouldBind) {
      document.addEventListener('click', handleClickOutside);
      document.addEventListener('contextmenu', handleClickOutside);
    }

    return () => {
      document.removeEventListener('click', handleClickOutside);
      document.removeEventListener('contextmenu', handleClickOutside);
    };
  }, [contextMenu.show, linkContextMenu.show, token, pathInputFocused]);

  // Disable global click outside handlers while any path input is focused
  useEffect(() => {
    const onFocus = () => setPathInputFocused(true);
    const onBlur = () => setTimeout(() => setPathInputFocused(false), 0);
    const inputs = [pathFromRef.current, pathToRef.current].filter(Boolean);
    inputs.forEach(inp => {
      inp && inp.addEventListener('focus', onFocus);
      inp && inp.addEventListener('blur', onBlur);
    });
    return () => {
      inputs.forEach(inp => {
        inp && inp.removeEventListener('focus', onFocus);
        inp && inp.removeEventListener('blur', onBlur);
      });
    };
  }, [showPathPanel]);

  // When path panel opens, focus the first input so Tab continues within the panel
  useEffect(() => {
    if (showPathPanel) {
      setTimeout(() => {
        try { pathFromRef.current && pathFromRef.current.focus(); } catch (_) {}
      }, 0);
    }
  }, [showPathPanel]);

  // Freeze simulation and node positions while any context menu is open
  useEffect(() => {
    const sim = simulationRef.current;
    const anyMenuOpen = contextMenu.show || linkContextMenu.show;
    if (anyMenuOpen) {
      // Lock current positions
      if (networkData && Array.isArray(networkData.nodes)) {
        networkData.nodes.forEach(n => {
          n.fx = n.x;
          n.fy = n.y;
        });
      }
      if (sim) {
        try { sim.stop(); } catch (_) {}
      }
    } else {
      // Release locks; do not auto-resume to avoid snap-backs after clicks
      if (networkData && Array.isArray(networkData.nodes)) {
        networkData.nodes.forEach(n => { n.fx = null; n.fy = null; });
      }
    }
    // No re-render needed; positions are frozen/unfrozen without relayout
  }, [contextMenu.show, linkContextMenu.show, shouldRunSimulation]);

  // Compute alignment for Saved view token below row
  useLayoutEffect(() => {
    try {
      // Measure placeholder text to size the input exactly like a button: text width + horizontal padding + borders
      let desiredWidth = 240;
      // Removed text measurement for paste input (no longer displayed)
      const finalWidth = Math.max(200, desiredWidth);
      if (Number.isFinite(finalWidth) && finalWidth > 0) setSavedInputBelowWidth(finalWidth);
    } catch (_) {}
  }, [showSaveExportMenu, token, loadToken, currentView]);

  // Resize handler removed

  // Local registration is removed; all sign-up flows go through Auth0.

  const performSearch = async () => {
    if (!searchQuery.trim() || searchQuery.length < 2) {
      setError('Please enter at least 2 characters');
      return;
    }

    try {
      try { window.__cmg_resetZoom && window.__cmg_resetZoom(); } catch (_) {}
      clearFiltersForNewSearch([]);
      setLoading(true);
      setError('');
      // Clear any open Full information card on new search
      setProfileCard({ show: false, data: null });
      // Clear any path-finding info card and inputs
      try { setPathInfo(null); } catch (_) {}
      try { if (pathFromRef.current) pathFromRef.current.value = ''; } catch (_) {}
      try { if (pathToRef.current) pathToRef.current.value = ''; } catch (_) {}
      try { pathFromValRef.current = ''; } catch (_) {}
      try { pathToValRef.current = ''; } catch (_) {}
      // If we're currently under a global rate-limit cooldown, fail fast with a clear message
      const until = rateLimitedUntilRef.current || 0;
      const now = Date.now();
      if (until && now < until) {
        setError(formatRateLimitWaitMessage(until));
        setLoading(false);
        return;
      }
      
      // Make API call to Neo4j backend
      const endpoint = searchType === 'singers' ? '/search/singers' : 
                     searchType === 'operas' ? '/search/operas' : '/search/books';
      
      if (isSearchingRef.current) return; // drop rapid repeats
      isSearchingRef.current = true;
      const response = await fetchWithRetry(`${API_BASE}${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ query: searchQuery })
      }, { retries: 3, baseDelay: 900 });

      const text = await response.text();
      let data;
      try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
      
      if (response.ok) {
        setSearchResults(data[searchType] || []);
        setOriginalSearchResults(data[searchType] || []);
        setOriginalSearchType(searchType);
        setCurrentView('results');
        setHasExecutedSearch(true);
        // Trigger halo for search result cards (persistent for now)
        setShowResultsHalo(true);
        
        // Generate network data from search results
        generateNetworkFromSearchResults(data[searchType] || [], searchType);
      } else {
        if (handleUnauthorized(response)) return;
        setError(data.error || `Search failed (${response.status})`);
      }
    } catch (err) {
      console.error('Search error:', err);
      setError(err?.message || 'Search failed - please try again');
    } finally {
      isSearchingRef.current = false;
      setLoading(false);
    }
  };
  // Universal anti-overlap positioning system
  const positionNodesWithoutOverlap = (nodes, containerWidth = 800, containerHeight = 600) => {
    const minDistance = 120; // Minimum distance between any two nodes
    const maxAttempts = 50; // Maximum attempts to find a good position
    
    nodes.forEach((node, index) => {
      let validPosition = false;
      let attempts = 0;
      
      while (!validPosition && attempts < maxAttempts) {
        // Generate candidate position
        const margin = 100;
        const x = margin + Math.random() * (containerWidth - 2 * margin);
        const y = margin + Math.random() * (containerHeight - 2 * margin);
        
        // Check distance from all previously positioned nodes
        let hasCollision = false;
        for (let i = 0; i < index; i++) {
          const otherNode = nodes[i];
          const distance = Math.sqrt(
            Math.pow(x - otherNode.x, 2) + 
            Math.pow(y - otherNode.y, 2)
          );
          
          if (distance < minDistance) {
            hasCollision = true;
            break;
          }
        }
        
        if (!hasCollision) {
          node.x = x;
          node.y = y;
          validPosition = true;
        }
        
        attempts++;
      }
      
      // Fallback: use grid positioning if can't find collision-free spot
      if (!validPosition) {
        const cols = Math.ceil(Math.sqrt(nodes.length));
        const spacing = Math.min(
          (containerWidth - 200) / cols, 
          (containerHeight - 200) / Math.ceil(nodes.length / cols)
        );
        const col = index % cols;
        const row = Math.floor(index / cols);
        
        node.x = 100 + col * Math.max(spacing, 150);
        node.y = 100 + row * Math.max(spacing, 150);
      }
    });
  
    return nodes;
  };
  const resolveFallbackConfig = (relationshipType = '', anchorType = '') => {
    const normalizedLabel = String(relationshipType || '').trim().toLowerCase();
    const base = {
      source: null,
      type: null,
      label: ''
    };

    if (!normalizedLabel) return base;
    if (normalizedLabel.includes('taughtby')) {
      return { source: 'node', type: 'taught', label: 'taught' };
    }
    if (normalizedLabel.includes('taught')) {
      return { source: 'anchor', type: 'taught', label: 'taught' };
    }
    if (normalizedLabel.includes('parentof')) {
      return { source: 'anchor', type: 'family', label: 'parent' };
    }
    if (normalizedLabel.includes('parent')) {
      return { source: 'node', type: 'family', label: 'parent' };
    }
    if (normalizedLabel.includes('spouseof')) {
      return { source: 'node', type: 'family', label: 'spouse' };
    }
    if (normalizedLabel.includes('spouse')) {
      return { source: 'anchor', type: 'family', label: 'spouse' };
    }
    if (normalizedLabel.includes('grandparentof')) {
      return { source: 'anchor', type: 'family', label: 'grandparent' };
    }
    if (normalizedLabel.includes('grandparent')) {
      return { source: 'node', type: 'family', label: 'grandparent' };
    }
    if (normalizedLabel.includes('sibling')) {
      return { source: 'anchor', type: 'family', label: 'sibling' };
    }
    if (normalizedLabel.includes('premieredrolein') || normalizedLabel.includes('premiered')) {
      return anchorType === 'opera'
        ? { source: 'node', type: 'premiered', label: 'premiered role in' }
        : { source: 'anchor', type: 'premiered', label: 'premiered role in' };
    }
    if (normalizedLabel.includes('wrote') || normalizedLabel.includes('composed')) {
      return anchorType === 'opera'
        ? { source: 'node', type: 'wrote', label: 'wrote' }
        : { source: 'anchor', type: 'wrote', label: 'wrote' };
    }
    if (normalizedLabel.includes('authored')) {
      return anchorType === 'book'
        ? { source: 'node', type: 'authored', label: 'authored' }
        : { source: 'anchor', type: 'authored', label: 'authored' };
    }
    if (normalizedLabel.includes('edited')) {
      return anchorType === 'book'
        ? { source: 'node', type: 'edited', label: 'edited' }
        : { source: 'anchor', type: 'edited', label: 'edited' };
    }

    return base;
  };
  const attachNewNodesToAnchor = ({
    anchorId,
    anchorType,
    relationshipType,
    newNodes,
    newLinks,
    existingLinks,
    addLink
  }) => {
    if (!anchorId || !Array.isArray(newNodes) || newNodes.length === 0) return;

    const normalize = (value) => normalizeNodeId(value);
    const attachedToAnchor = new Set();
    const registerAttachment = (sourceId, targetId) => {
      const src = normalize(sourceId);
      const tgt = normalize(targetId);
      if (src === anchorId && tgt) attachedToAnchor.add(tgt);
      if (tgt === anchorId && src) attachedToAnchor.add(src);
    };

    (existingLinks || []).forEach(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source?.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target?.id;
      registerAttachment(sourceId, targetId);
    });
    (newLinks || []).forEach(link => {
      registerAttachment(link.source, link.target);
    });

    const fallbackConfig = resolveFallbackConfig(relationshipType, anchorType);
    const fallbackTypeKey = typeof fallbackConfig.type === 'string' ? fallbackConfig.type.toLowerCase() : '';
    if (!fallbackTypeKey || fallbackTypeKey === 'related') {
      return;
    }

    newNodes.forEach(node => {
      const nodeId = normalize(node?.id ?? node?.name);
      if (!nodeId || attachedToAnchor.has(nodeId)) return;
      if (isPlaceholderName(nodeId)) return;

      const sourceId = fallbackConfig.source === 'node' ? nodeId : anchorId;
      const targetId = fallbackConfig.source === 'node' ? anchorId : nodeId;
      if (!sourceId || !targetId) return;

      const linkKey = `${sourceId}|${targetId}|${fallbackTypeKey}`;
      // addLink handles deduplication via existingLinks set, so just call it
      addLink(sourceId, targetId, fallbackConfig.type, { label: fallbackConfig.label });
      attachedToAnchor.add(nodeId);
    });
  };
  const ensureNodeConnectivity = (
    nodes,
    links,
    {
      primaryId = null,
      fallbackType = null,
      fallbackLabel = ''
    } = {}
  ) => {
    const allowedFallbackTypes = new Set([
      'taught',
      'family',
      'spouse',
      'sibling',
      'parent',
      'grandparent',
      'wrote',
      'composed',
      'premiered',
      'premiered_role_in',
      'authored',
      'edited'
    ]);
    const fallbackTypeKey = typeof fallbackType === 'string' ? fallbackType.toLowerCase().trim() : '';
    if (!fallbackTypeKey || fallbackTypeKey === 'related' || !allowedFallbackTypes.has(fallbackTypeKey)) {
      return;
    }
    const normalize = (value) => {
      if (value === null || value === undefined) return '';
      return String(value).replace(/\s+/g, ' ').trim();
    };
    const idToNode = new Map();
    (nodes || []).forEach((node) => {
      const id = normalize(node?.id ?? node?.name);
      if (!id || isPlaceholderName(id)) return;
      if (!idToNode.has(id)) {
        idToNode.set(id, node);
      }
    });
    const nodeIds = nodes
      .map((node) => normalize(node?.id ?? node?.name))
      .filter(id => id && !isPlaceholderName(id));
    if (nodeIds.length === 0) return;
    const nodeIdSet = new Set(nodeIds);
    const adjacency = new Map();
    nodeIds.forEach((id) => adjacency.set(id, 0));
    const linkKeys = new Set();
    links.forEach((link) => {
      const sourceId = normalize(
        typeof link.source === 'string'
          ? link.source
          : link.source?.id ?? link.source?.name
      );
      const targetId = normalize(
        typeof link.target === 'string'
          ? link.target
          : link.target?.id ?? link.target?.name
      );
      if (!sourceId || !targetId) return;
      const key = `${sourceId}|${targetId}|${String(link.type || '').toLowerCase()}`;
      linkKeys.add(key);
      if (adjacency.has(sourceId)) {
        adjacency.set(sourceId, (adjacency.get(sourceId) || 0) + 1);
      }
      if (adjacency.has(targetId)) {
        adjacency.set(targetId, (adjacency.get(targetId) || 0) + 1);
      }
    });
    const normalizedPrimaryId = normalize(primaryId);
    const pickFallback = (nodeId) => {
      if (
        normalizedPrimaryId &&
        normalizedPrimaryId !== nodeId &&
        nodeIdSet.has(normalizedPrimaryId)
      ) {
        return normalizedPrimaryId;
      }
      for (const candidate of nodeIds) {
        if (candidate && candidate !== nodeId) {
          return candidate;
        }
      }
      return null;
    };
    nodeIds.forEach((nodeId) => {
      if (!nodeId) return;
      if ((adjacency.get(nodeId) || 0) > 0) return;
      let fallbackId = pickFallback(nodeId);
      const isSelfLink = !fallbackId || fallbackId === nodeId;
      if (!fallbackId) {
        fallbackId = nodeId;
      }
      if (!nodeIdSet.has(nodeId) || (fallbackId && !nodeIdSet.has(fallbackId))) {
        console.warn('[ensureNodeConnectivity] skipped fallback link; missing node', { nodeId, fallbackId });
        return;
      }
      const key = `${fallbackId}|${nodeId}|${fallbackTypeKey}`;
      if (linkKeys.has(key)) return;
      const sourceNode = idToNode.get(fallbackId);
      const targetNode = idToNode.get(nodeId);
      links.push({
        source: fallbackId,
        target: nodeId,
        type: fallbackType,
        label: fallbackLabel,
        relationshipSourceDisplay: fallbackLabel,
        sourceInfo: fallbackLabel,
        relationship_source: fallbackLabel,
        // Treat fallback attachments (created during expansions) as expansion-internal
        // so the main simulation keeps the cluster compact around the anchor.
        expansionInternal: true
      });
      linkKeys.add(key);
      if (!isSelfLink) {
        adjacency.set(fallbackId, (adjacency.get(fallbackId) || 0) + 1);
      }
      adjacency.set(nodeId, (adjacency.get(nodeId) || 0) + 1);
    });
  };
  const generateNetworkFromSearchResults = (results, type) => {
    const nodes = [];
    const links = [];
    
  results.forEach((item, index) => {
    if (type === 'singers') {
      const name = item.name || item.properties.full_name || `Unknown Singer ${index + 1}`;
      const typedIdRaw = item.id || (item.person_id ? `person:${item.person_id}` : '');
      const personId = normalizeNodeId(typedIdRaw || name);
      nodes.push({
        id: personId,
        name: name,
        type: 'person',
        voiceType: item.properties.voice_type,
        birthYear: (item.properties.birth_year ?? (item.properties.birth && (item.properties.birth.low ?? item.properties.birth))) || null,
        deathYear: (item.properties.death_year ?? (item.properties.death && (item.properties.death.low ?? item.properties.death))) || null,
          birthplace: item.properties.birthplace || item.properties.citizen || null,
          x: 0, // Will be positioned by anti-overlap system
          y: 0
        });
      } else if (type === 'operas') {
        const operaName = deriveOperaName(item.properties, `Unknown Opera ${index + 1}`);
        const typedIdRaw = item.id || (item.opera_id ? `opera:${item.opera_id}` : '');
        nodes.push(createOperaNodePayload({
          id: typedIdRaw || operaName,
          name: operaName,
          opera_id: item.opera_id || item.properties?.opera_id,
          opera_name: operaName,
          version: item.properties?.version,
          x: 0, // Will be positioned by anti-overlap system
          y: 0
        }));
      } else if (type === 'books') {
        const bookTitle = item.properties.title || `Unknown Book ${index + 1}`;
        const typedIdRaw = item.id || (item.book_id ? `book:${item.book_id}` : '');
        nodes.push(createBookNodePayload({
          id: typedIdRaw || bookTitle,
          name: bookTitle,
          book_id: item.book_id || item.properties?.book_id,
          title: bookTitle,
          link: item.properties?.link,
          x: 0, // Will be positioned by anti-overlap system
          y: 0
        }));
      }
  });

  // Apply anti-overlap positioning to all nodes
  positionNodesWithoutOverlap(nodes);

  setNetworkData(sanitizeGraphData({ nodes, links }));
    resetFiltersForNodeSet(nodes);
    setShowFilterPanel(false);
    setCurrentCenterNode(null); // Reset center tracking for search results
    setShouldRunSimulation(true); // Trigger simulation for search results
  };
  const generateNetworkFromDetails = (details, centerName, type, options = {}) => {
    const nodes = [];
    const links = [];
    const addedNodes = new Set(); // Track which people have been added
    
    // Helper function to add a person node only if not already added
  const addPersonNode = (person, defaultX, defaultY) => {
    const candidateName = person?.full_name || person?.name;
    const typedIdRaw = person?.id || (person?.person_id ? `person:${person.person_id}` : '');
    const normalizedId = normalizeNodeId(typedIdRaw || candidateName);
    if (!normalizedId || isPlaceholderName(normalizedId)) return null;
    const displayName = String(candidateName || normalizedId).trim() || normalizedId;
    if (!addedNodes.has(normalizedId)) {
      nodes.push({
        id: normalizedId,
        name: displayName,
        type: 'person',
        voiceType: person.voice_type,
        birthplace: person.birthplace || person.citizen || null,
        birthYear: (person.birth_year ?? (person.birth && (person.birth.low ?? person.birth))) || null,
        deathYear: (person.death_year ?? (person.death && (person.death.low ?? person.death))) || null,
        spelling_source: person.spelling_source || null,
        voice_type_source: person.voice_type_source || null,
        dates_source: person.dates_source || null,
        birthplace_source: person.birthplace_source || null,
        x: defaultX,
        y: defaultY
      });
      addedNodes.add(normalizedId);
    }
    return normalizedId;
  };
    
  const centerData = details?.center || {};
  const centerIdCandidates = [
    centerData?.id,
    centerData?.person_id ? `person:${centerData.person_id}` : '',
    type === 'operas' ? details?.opera?.id : '',
    type === 'operas' && details?.opera?.opera_id ? `opera:${details.opera.opera_id}` : '',
    type === 'books' ? details?.book?.id : '',
    type === 'books' && details?.book?.book_id ? `book:${details.book.book_id}` : ''
  ];
  const centerTypedIdRaw = centerIdCandidates.find((candidate) => typeof candidate === 'string' && candidate.trim()) || '';
  const fallbackCenterName = centerData?.full_name || centerData?.name ||
    (type === 'operas' ? details?.opera?.opera_name : type === 'books' ? details?.book?.title : '') ||
    centerName || '';
  const rawCenterName = String(fallbackCenterName || '').trim();
  const centerId = normalizeNodeId(centerTypedIdRaw || rawCenterName);
  if (!centerId || isPlaceholderName(centerId)) {
    console.warn('[generateNetworkFromDetails] Skipping network with placeholder center', { centerName });
    return;
  }
  const centerLabel = rawCenterName || centerId;

  let centerNode;
  if (type === 'singers') {
    centerNode = {
      id: centerId,
      name: centerLabel,
      type: 'person',
      isCenter: true,
      x: 400,
      y: 300
    };
    if (details.center) {
      centerNode.voiceType = details.center.voice_type;
      centerNode.birthYear = (details.center.birth_year ?? (details.center.birth && (details.center.birth.low ?? details.center.birth))) || null;
      centerNode.deathYear = (details.center.death_year ?? (details.center.death && (details.center.death.low ?? details.center.death))) || null;
      centerNode.birthplace = details.center.birthplace || details.center.citizen || null;
    }
  } else if (type === 'operas') {
    centerNode = createOperaNodePayload({
      id: centerTypedIdRaw || centerId,
      name: centerLabel,
      isCenter: true,
      x: 400,
      y: 300,
      opera_id: details?.opera?.opera_id || centerData?.opera_id,
      opera_name: details?.opera?.opera_name || centerLabel,
      version: details?.opera?.version
    });
  } else if (type === 'books') {
    centerNode = createBookNodePayload({
      id: centerTypedIdRaw || centerId,
      name: centerLabel,
      isCenter: true,
      x: 400,
      y: 300,
      book_id: details?.book?.book_id || centerData?.book_id,
      title: details?.book?.title || centerLabel,
      link: details?.book?.link || centerData?.link
    });
  } else {
    centerNode = {
      id: centerId,
      name: centerLabel,
      type: 'person',
      isCenter: true,
      x: 400,
      y: 300
    };
  }

  nodes.push(centerNode);
  addedNodes.add(centerId);

  // Add composer(s) for opera center via wrote list; fallback to single property if present
    if (type === 'operas') {
      const wroteList = Array.isArray(details.wrote) ? details.wrote : [];
    if (wroteList.length > 0) {
      wroteList.forEach((row, idx) => {
        const composerRaw = row && (row.composer || row.name || row.full_name);
        const composerId = normalizeNodeId(composerRaw);
        if (!composerId || isPlaceholderName(composerId)) return;
        const composerName = String(composerRaw || composerId).trim() || composerId;
        if (!addedNodes.has(composerId)) {
          nodes.push({ id: composerId, name: composerName, type: 'person', x: 250 + (idx * 40), y: 180 });
          addedNodes.add(composerId);
        }
        const relationshipSource = deriveRelationshipSourceText(row?.source, row?.relationship_source);
        links.push({
          source: composerId,
          target: centerId,
          type: 'wrote',
          label: 'wrote',
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: row?.relationship_source || null
        });
      });
    } else if (type === 'singers' && Array.isArray(details.works?.composedOperas) && details.works.composedOperas.length > 0) {
      details.works.composedOperas.forEach((operaEntry, idx) => {
        const fallbackLabel = `Unknown Opera ${idx + 1}`;
        const name = deriveOperaName(operaEntry, fallbackLabel);
        const typedIdRaw = operaEntry?.id || (operaEntry?.opera_id ? `opera:${operaEntry.opera_id}` : '');
        const operaId = normalizeNodeId(typedIdRaw || name);
        if (!operaId || isPlaceholderName(operaId)) return;
        const displayName = name || operaId;
        if (!addedNodes.has(operaId)) {
          nodes.push(createOperaNodePayload({
            id: typedIdRaw || displayName,
            name: displayName,
            opera_id: operaEntry?.opera_id,
            opera_name: displayName,
            version: operaEntry?.version,
            x: 150 + idx * 80,
            y: 420
          }));
          addedNodes.add(operaId);
        }
        const relationshipSource = deriveRelationshipSourceText(operaEntry?.source, operaEntry?.opera_source_text, operaEntry?.relationship_source);
        links.push({
          source: centerId,
          target: operaId,
          type: 'wrote',
          label: 'wrote',
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: operaEntry?.relationship_source || null
        });
      });
    } else if (details.opera && details.opera.composer) {
      const composerRaw = details.opera.composer;
      const composerId = normalizeNodeId(composerRaw);
      if (composerId && !isPlaceholderName(composerId)) {
        const composerName = String(composerRaw || composerId).trim() || composerId;
        if (!addedNodes.has(composerId)) {
          nodes.push({ id: composerId, name: composerName, type: 'person', x: 250, y: 180 });
          addedNodes.add(composerId);
        }
        const relationshipSource = deriveRelationshipSourceText(details.opera?.source, details.opera?.relationship_source);
        links.push({
          source: composerId,
          target: centerId,
          type: 'wrote',
          label: 'wrote',
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: details.opera?.relationship_source || null
        });
      }
    }
  } else if (options?.context === 'composer') {
    if (details.works?.composedOperas && details.works.composedOperas.length > 0) {
      details.works.composedOperas.forEach((opera, index) => {
        const fallbackLabel = `Unknown Opera ${index + 1}`;
        const operaName = deriveOperaName(opera, fallbackLabel);
        const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
        const operaId = normalizeNodeId(typedIdRaw || operaName);
        if (!operaId || isPlaceholderName(operaId)) return;
        const displayName = operaName || operaId;
        if (!addedNodes.has(operaId)) {
          nodes.push(createOperaNodePayload({
            id: typedIdRaw || displayName,
            name: displayName,
            opera_id: opera?.opera_id,
            opera_name: displayName,
            version: opera?.version,
            x: 120 + (index * 100),
            y: 420
          }));
          addedNodes.add(operaId);
        }
        const relationshipSource = deriveRelationshipSourceText(opera.opera_source_text, opera.source);
        links.push({
          source: centerId,
          target: operaId,
          type: 'wrote',
          label: 'wrote',
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: opera.relationship_source || null
        });
      });
    }
  }

  // Add teachers
  if (details.teachers) {
    details.teachers.forEach((teacher, index) => {
      const teacherId = addPersonNode(teacher, 200 + (index * 50), 150);
      if (!teacherId) return;
      const relationshipSource = deriveRelationshipSourceText(teacher.teacher_rel_source_text, teacher.relationshipSourceDisplay, teacher.teacher_rel_source, teacher.relationship_source, teacher.source);
      links.push({
        source: teacherId,
        target: centerId,
        type: 'taught',
        label: 'taught',
        relationshipSourceDisplay: relationshipSource,
        sourceInfo: relationshipSource,
        teacher_rel_source: teacher.teacher_rel_source || null,
        teacher_rel_source_text: teacher.teacher_rel_source_text || relationshipSource || null,
        teacher_rel_source_url: teacher.teacher_rel_source_url || null,
        relationship_source: teacher.relationship_source || null
      });
    });
  }

  // Add students
  if (details.students) {
    details.students.forEach((student, index) => {
      const studentId = addPersonNode(student, 200 + (index * 50), 450);
      if (!studentId) return;
      const relationshipSource = deriveRelationshipSourceText(student.teacher_rel_source_text, student.relationshipSourceDisplay, student.teacher_rel_source, student.relationship_source, student.source);
      links.push({
        source: centerId,
        target: studentId,
        type: 'taught',
        label: 'taught',
        relationshipSourceDisplay: relationshipSource,
        sourceInfo: relationshipSource,
        teacher_rel_source: student.teacher_rel_source || null,
        teacher_rel_source_text: student.teacher_rel_source_text || relationshipSource || null,
        teacher_rel_source_url: student.teacher_rel_source_url || null,
        relationship_source: student.relationship_source || null
      });
    });
  }

    // Add family (fallback: some responses may nest under center)
    const familyList = (details.family || details.center?.family || []);
    if (familyList && familyList.length > 0) {
      familyList.forEach((relative, index) => {
        const relName = relative.full_name || relative.name;
        const relativeNorm = { ...relative, full_name: relName };
        const relativeId = addPersonNode(relativeNorm, 600 + (index * 50), 200 + (index * 50));
        if (!relativeId) return;
        const relDisplay = String(relName || relativeId).trim() || relativeId;
        
        // Determine correct direction based on relationship_type from backend
        const relType = (relative.relationship_type || '').toLowerCase();
        let src = centerId;
        let tgt = relativeId;
        if ((relType.includes('parent') && !relType.includes('of')) ||
            (relType.includes('grandparent') && !relType.includes('of'))) {
          // relative is ancestor of center
          src = relativeId;
          tgt = centerId;
        } else if (relType.includes('parentof') || relType.includes('grandparentof')) {
          // center is ancestor of relative (already src=center, tgt=relative)
        } // spouse/sibling/default keep center -> relative for determinism
        
        const relationshipSource = deriveRelationshipSourceText(relative.teacher_rel_source_text, relative.relationshipSourceDisplay, relative.teacher_rel_source, relative.relationship_source, relative.source);
        links.push({
          source: src,
          target: tgt,
          type: 'family',
          label: relative.relationship_type || relDisplay,
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          teacher_rel_source: relative.teacher_rel_source || null,
          teacher_rel_source_text: relative.teacher_rel_source_text || relationshipSource || null,
          teacher_rel_source_url: relative.teacher_rel_source_url || null,
          relationship_source: relative.relationship_source || null
        });
      });
    }

  // Add works
    if (details.works) {
      // Add operas
      if (details.works.operas) {
        details.works.operas.forEach((opera, index) => {
          const fallbackLabel = `Unknown Opera ${index + 1}`;
          const operaName = deriveOperaName(opera, fallbackLabel);
          const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
          const operaId = normalizeNodeId(typedIdRaw || operaName);
          if (!operaId || isPlaceholderName(operaId)) return;
          nodes.push(createOperaNodePayload({
            id: typedIdRaw || operaId,
            name: operaName || operaId,
            opera_id: opera?.opera_id,
            opera_name: operaName || operaId,
            version: opera?.version,
            x: 100 + (index * 80),
            y: 500
          }));
          
        const relationshipSource = deriveRelationshipSourceText(opera.opera_source_text, opera.relationshipSourceDisplay, opera.source, opera.relationship_source);
        links.push({
          source: centerId,
          target: operaId,
            type: 'premiered',
            label: 'premiered role in',
            role: opera.role,
            relationshipSourceDisplay: relationshipSource,
            sourceInfo: relationshipSource,
            relationship_source: opera.relationship_source || null,
            opera_source_text: opera.opera_source_text || relationshipSource || null,
            opera_source_url: opera.opera_source_url || null
          });
        });
      }

      // Add books
      if (details.works.books) {
        details.works.books.forEach((book, index) => {
          const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
          const bookLabel = String(book.title || `Unknown Book ${index + 1}`).trim();
          const bookId = normalizeNodeId(typedIdRaw || bookLabel);
          if (!bookId || isPlaceholderName(bookId)) return;
          nodes.push(createBookNodePayload({
            id: typedIdRaw || bookId,
            name: bookLabel || bookId,
            book_id: book?.book_id,
            title: bookLabel || bookId,
            link: book?.link,
            x: 500 + (index * 80),
            y: 500
          }));
          
          const relationshipSource = null;
          links.push({
            source: centerId,
            target: bookId,
            type: 'authored',
            label: 'authored',
            relationshipSourceDisplay: relationshipSource,
            sourceInfo: relationshipSource,
            relationship_source: null
          });
        });
      }

      // Add edited books
      if (details.works.editedBooks) {
        details.works.editedBooks.forEach((book, index) => {
          const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
          const bookLabel = String(book.title || `Unknown Book ${index + 1}`).trim();
          const bookId = normalizeNodeId(typedIdRaw || bookLabel);
          if (!bookId || isPlaceholderName(bookId)) return;
          nodes.push(createBookNodePayload({
            id: typedIdRaw || bookId,
            name: bookLabel || bookId,
            book_id: book?.book_id,
            title: bookLabel || bookId,
            link: book?.link,
            x: 500 + (index * 80),
            y: 560
          }));

          const relationshipSource = deriveRelationshipSourceText(book.source, book.relationship_source);
          links.push({
            source: centerId,
            target: bookId,
            type: 'edited',
            label: 'edited',
            relationshipSourceDisplay: relationshipSource,
            sourceInfo: relationshipSource,
            relationship_source: book.relationship_source || null
          });
        });
      }

      // Add composed operas (person center) - keep 'composed' label for legacy, but also treat as 'wrote'
      if (details.works.composedOperas) {
        details.works.composedOperas.forEach((opera, index) => {
          const fallbackLabel = `Unknown Opera ${index + 1}`;
          const operaName = deriveOperaName(opera, fallbackLabel);
          const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
          const operaId = normalizeNodeId(typedIdRaw || operaName);
          if (!operaId || isPlaceholderName(operaId)) return;
          nodes.push(createOperaNodePayload({
            id: typedIdRaw || operaId,
            name: operaName || operaId,
            opera_id: opera?.opera_id,
            opera_name: operaName || operaId,
            version: opera?.version,
            x: 100 + (index * 80),
            y: 400
          }));
          
          const relationshipSource = deriveRelationshipSourceText(opera.opera_source_text, opera.relationshipSourceDisplay, opera.source, opera.relationship_source);
          links.push({
            source: centerId,
            target: operaId,
            type: 'wrote',
            label: 'wrote',
            relationshipSourceDisplay: relationshipSource,
            sourceInfo: relationshipSource,
            relationship_source: opera.relationship_source || null,
            opera_source_text: opera.opera_source_text || relationshipSource || null,
            opera_source_url: opera.opera_source_url || null
          });
        });
      }
    }

    // Add premiered roles - behavior depends on center type
  if (details.premieredRoles) {
    if (type === 'operas') {
      // For opera networks: show people who premiered roles in this opera
      details.premieredRoles.forEach((role, index) => {
        const fallbackSinger = `Unknown Singer ${index}`;
        const singerRaw = role?.singer || fallbackSinger;
        const singerId = normalizeNodeId(singerRaw);
        if (!singerId || isPlaceholderName(singerId)) return;
        const singerName = String(singerRaw || singerId).trim() || singerId;
        if (!addedNodes.has(singerId)) {
          nodes.push({
            id: singerId,
            name: singerName,
            type: 'person',
            voiceType: role.voice_type,
            x: 300 + (index * 60),
            y: 400
          });
          addedNodes.add(singerId);
        }
        
        const relationshipSource = deriveRelationshipSourceText(role.opera_source_text, role.relationshipSourceDisplay, role.source, role.relationship_source);
        links.push({
          source: singerId,
          target: centerId,
          type: 'premiered',
          label: 'premiered role in',
          role: role.role,
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: role.relationship_source || null,
          opera_source_text: role.opera_source_text || relationshipSource || null,
          opera_source_url: role.opera_source_url || null
        });
      });
    } else if (type === 'singers') {
      // For person networks: show operas the person premiered roles in
      // This data should be in works.operas, not premieredRoles
      // Skip processing premieredRoles for person networks to avoid confusion
      console.log('Skipping premieredRoles for person network - should use works.operas instead');
    }
  }

  // Add authors for books
  if (details.authors) {
    details.authors.forEach((author, index) => {
      const fallbackAuthor = `Unknown Author ${index}`;
      const authorRaw = author?.author || author?.name || fallbackAuthor;
      const authorId = normalizeNodeId(authorRaw);
      if (!authorId || isPlaceholderName(authorId)) return;
      const authorName = String(authorRaw || authorId).trim() || authorId;
      if (!addedNodes.has(authorId)) {
        nodes.push({
          id: authorId,
          name: authorName,
          type: 'person',
          voiceType: author.voice_type,
          x: 200 + (index * 60),
          y: 200
        });
        addedNodes.add(authorId);
      }
      
      const relationshipSource = null;
      links.push({
        source: authorId,
        target: centerId,
        type: 'authored',
        label: 'authored',
        relationshipSourceDisplay: relationshipSource,
        sourceInfo: relationshipSource,
        relationship_source: null
      });
    });
  }

  // Add editors for books
  if (details.editors) {
    details.editors.forEach((editor, index) => {
      const fallbackEditor = `Unknown Editor ${index}`;
      const editorRaw = editor?.editor || editor?.name || fallbackEditor;
      const editorId = normalizeNodeId(editorRaw);
      if (!editorId || isPlaceholderName(editorId)) return;
      const editorName = String(editorRaw || editorId).trim() || editorId;
      if (!addedNodes.has(editorId)) {
        nodes.push({
          id: editorId,
          name: editorName,
          type: 'person',
          voiceType: editor.voice_type,
          x: 600 + (index * 60),
          y: 200
        });
        addedNodes.add(editorId);
      }
      
      const relationshipSource = null;
      links.push({
        source: editorId,
        target: centerId,
        type: 'edited',
        label: 'edited',
        relationshipSourceDisplay: relationshipSource,
        sourceInfo: relationshipSource,
        relationship_source: null
      });
    });
  }

  // Apply anti-overlap positioning to all nodes
  positionNodesWithoutOverlap(nodes);
  ensureNodeConnectivity(nodes, links, {
    primaryId: centerId
  });

  clearFiltersForNewSearch(nodes);

  setNetworkData(sanitizeGraphData({ nodes, links }));
  setCurrentCenterNode(centerLabel); // Set the center node for this network
  setShouldRunSimulation(true); // Trigger force simulation for new network
  };

  // Function to show full information profile card
  const showFullInformation = async (node) => {
    try {
      setLoading(true);
      showHelperMessage('', 0);
      let response, data;
      
      if (node.type === 'person') {
        response = await fetchWithRetry(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 1 })
        }, { retries: 2, baseDelay: 600 });
        {
          const text = await response.text();
          let parsed; try { parsed = text ? JSON.parse(text) : {}; } catch (_) { parsed = { error: text || 'Invalid response' }; }
        if (response.ok) {
            setProfileCard({ show: true, data: parsed.center });
          } else {
            setError(parsed.error || `Failed to fetch information (${response.status})`);
          }
        }
      } else if (node.type === 'opera') {
        const { type: typedType, value: typedValue } = parseTypedId(node.id);
        const payload = { operaName: node.name };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
        }, { retries: 2, baseDelay: 600 });
        {
          const text = await response.text();
          let parsed; try { parsed = text ? JSON.parse(text) : {}; } catch (_) { parsed = { error: text || 'Invalid response' }; }
        if (response.ok) {
            setProfileCard({ show: true, data: parsed.opera });
          } else {
            setError(parsed.error || `Failed to fetch information (${response.status})`);
          }
        }
      } else if (node.type === 'book') {
        response = await fetchWithRetry(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: node.name })
        }, { retries: 2, baseDelay: 600 });
        {
          const text = await response.text();
          let parsed; try { parsed = text ? JSON.parse(text) : {}; } catch (_) { parsed = { error: text || 'Invalid response' }; }
        if (response.ok) {
            setProfileCard({ show: true, data: parsed.book });
          } else {
            setError(parsed.error || `Failed to fetch information (${response.status})`);
        }
      }
      }
    } catch (err) {
      setError('Failed to fetch profile information');
    } finally {
      setLoading(false);
    }
  };
const normalizeNodeId = (value) => {
  if (value === null || value === undefined) return '';
  return String(value)
    .normalize('NFKC')
    .replace(/[\u2010-\u2015\u2212\u2017\u00AD\u2011\uFE63\uFF0D]/g, '-') // unifies dash variants
    .replace(/[\u0000-\u001F\u007F]+/g, '')
    .replace(/\s+/g, ' ')
    .trim();
};

const buildNodeAliasKey = (value, type = '') => {
  const normalized = normalizeNodeId(value);
  if (!normalized) return '';
  const prefix = type ? String(type).toLowerCase() : '';
  return `${prefix}::${normalized.toLowerCase()}`;
};

const collectNodeAliasValues = (candidate) => {
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

const registerNodeAliases = (aliasMap, node) => {
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

const resolveAliasIdFromMap = (aliasMap, candidate) => {
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

const OPERA_FORBIDDEN_FIELDS = [
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

const BOOK_FORBIDDEN_FIELDS = [
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

const GRAPH_BASE_KEYS = new Set([
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

const stripOperaBookFields = (node) => {
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

const copyGraphBaseProps = (source, target) => {
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

const createOperaNodePayload = (input = {}) => {
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

const createBookNodePayload = (input = {}) => {
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

const parseTypedId = (value) => {
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

const resolveLinkEndpointId = (endpoint) => {
  if (endpoint === null || endpoint === undefined) return '';
  if (typeof endpoint === 'string') return normalizeNodeId(endpoint);
  if (typeof endpoint === 'object') {
    if (endpoint.id !== undefined && endpoint.id !== null) return normalizeNodeId(endpoint.id);
    if (endpoint.name !== undefined && endpoint.name !== null) return normalizeNodeId(endpoint.name);
  }
  return normalizeNodeId(endpoint);
};

const isPlaceholderName = (value) => {
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

  const mergeNodeAttributes = (base, incoming) => {
    if (!incoming) return base;
    const result = base || {};
    Object.entries(incoming).forEach(([key, value]) => {
      if (value === undefined || value === null) return;
      if (key === 'id') {
        result.id = normalizeNodeId(value);
        return;
      }
      if (key === 'name') {
        const incomingName = String(value).trim();
        const existingName = String(result.name || '').trim();
        const shouldReplace =
          !existingName ||
          /^unknown\b/i.test(existingName);
        if (incomingName && shouldReplace) {
          result.name = incomingName;
        } else if (!existingName) {
          result.name = incomingName || result.id || '';
        }
        return;
      }
      if (key === 'x' || key === 'y' || key === 'vx' || key === 'vy' || key === 'fx' || key === 'fy' || key === 'homeX' || key === 'homeY') {
        if (!Number.isFinite(result[key])) {
          result[key] = value;
        }
        return;
      }
      if (key === 'recentlyExpandedAt' || key === 'expansionBatchId') {
        result[key] = value;
        return;
      }
      if (typeof value === 'boolean') {
        if (result[key] === undefined) {
          result[key] = value;
        }
        return;
      }
      if (typeof value === 'number') {
        if (!Number.isFinite(result[key])) {
          result[key] = value;
        }
        return;
      }
      if (typeof value === 'string') {
        if (!String(result[key] ?? '').trim()) {
          result[key] = value;
        }
        return;
      }
      if (Array.isArray(value)) {
        if (!Array.isArray(result[key]) || result[key].length === 0) {
          result[key] = value;
        }
        return;
      }
      if (typeof value === 'object') {
        if (!result[key]) {
          result[key] = value;
        }
        return;
      }
      if (result[key] === undefined) {
        result[key] = value;
      }
    });
    if (result.type === 'person') {
      const normalized = normalizePersonNode(result);
      return normalized || result;
    }
    stripOperaBookFields(result);
    return result;
  };

  const finalizeNodeCandidate = (candidate) => {
    if (!candidate) return null;
    const normalizedId = normalizeNodeId(candidate.id ?? candidate.name);
    if (!normalizedId) return null;
    if (isPlaceholderName(normalizedId)) return null;
    const normalizedName = candidate.name ? String(candidate.name).trim() : normalizedId;
    let nodeObj = { ...candidate, id: normalizedId, name: normalizedName };
    if (!Number.isFinite(nodeObj.x)) nodeObj.x = undefined;
    if (!Number.isFinite(nodeObj.y)) nodeObj.y = undefined;
    if (nodeObj.type === 'person') {
      const normalized = normalizePersonNode(nodeObj);
      if (normalized && normalized !== nodeObj) {
        nodeObj = normalized;
      }
    } else {
      stripOperaBookFields(nodeObj);
    }
    return nodeObj;
  };

const sanitizeGraphData = (graph) => {
  if (!graph) return { nodes: [], links: [] };
  const nodeAccumulator = new Map();
  (graph.nodes || []).forEach(node => {
    const normalizedId = normalizeNodeId(node?.id ?? node?.name);
    if (!normalizedId || isPlaceholderName(normalizedId)) return;
    const normalizedName = String(node?.name ?? '').trim() || normalizedId;
    const base = nodeAccumulator.get(normalizedId) || { id: normalizedId, name: normalizedName };
    nodeAccumulator.set(normalizedId, mergeNodeAttributes(base, { ...node, id: normalizedId, name: normalizedName }));
  });

  const sanitizedNodes = Array.from(nodeAccumulator.values()).map(node => {
    if (!node) return node;
    if (node.type === 'person') {
      const normalized = normalizePersonNode(node);
      return normalized || node;
    }
    stripOperaBookFields(node);
    return node;
  });

  const validIds = new Set(sanitizedNodes.map(n => n.id));
  const sanitizedLinks = [];
  (graph.links || []).forEach(link => {
    const sourceId = normalizeNodeId(
      typeof link?.source === 'string'
        ? link.source
        : link?.source?.id ?? link?.source?.name
    );
    const targetId = normalizeNodeId(
      typeof link?.target === 'string'
        ? link.target
        : link?.target?.id ?? link?.target?.name
    );
    if (!sourceId || !targetId) return;
    if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return;
    if (!validIds.has(sourceId) || !validIds.has(targetId)) return;
    sanitizedLinks.push({ ...link, source: sourceId, target: targetId });
  });

  return {
    nodes: sanitizedNodes,
    links: normalizeLinks(sanitizedLinks)
  };
};
  const sanitizeIncrementalGraph = (nodes, links, { anchorId, existingNodeIds } = {}) => {
    const normalizedAnchorId = normalizeNodeId(anchorId);
    const baseNodes = Array.isArray(nodes) ? [...nodes] : [];
    const stubbedNodeIds = new Set();
    const existingIdsSet = existingNodeIds instanceof Set
      ? existingNodeIds
      : Array.isArray(existingNodeIds)
        ? new Set(existingNodeIds.map(id => normalizeNodeId(id)))
        : null;
    const baseNodeIds = new Set(
      baseNodes
        .map(node => normalizeNodeId(node?.id ?? node?.name))
        .filter(Boolean)
    );
    const ensureStubForId = (id) => {
      const normalizedId = normalizeNodeId(id);
      if (!normalizedId) return;
      if (baseNodeIds.has(normalizedId)) return;
      baseNodes.push({ id: normalizedId, name: normalizedId });
      baseNodeIds.add(normalizedId);
      stubbedNodeIds.add(normalizedId);
    };
    if (normalizedAnchorId) {
      ensureStubForId(normalizedAnchorId);
    }
    (Array.isArray(links) ? links : []).forEach(link => {
      const sourceId = normalizeNodeId(resolveLinkEndpointId(link?.source));
      const targetId = normalizeNodeId(resolveLinkEndpointId(link?.target));
      if (existingIdsSet) {
        if (existingIdsSet.has(sourceId)) {
          ensureStubForId(sourceId);
        }
        if (existingIdsSet.has(targetId)) {
          ensureStubForId(targetId);
        }
      }
    });
    const graphForSanitization = { nodes: baseNodes, links };
    const sanitized = sanitizeGraphData(graphForSanitization);
    const filteredNodes = (sanitized.nodes || []).filter(node => {
      const id = normalizeNodeId(node?.id ?? node?.name);
      if (!id) return false;
      if (stubbedNodeIds.has(id)) return false;
      if (isPlaceholderName(id)) return false;
      return true;
    });
    const filteredNodeIds = new Set(filteredNodes.map(node => normalizeNodeId(node?.id ?? node?.name)).filter(Boolean));
    const filteredLinks = (sanitized.links || []).filter(link => {
      const sourceId = normalizeNodeId(resolveLinkEndpointId(link?.source));
      const targetId = normalizeNodeId(resolveLinkEndpointId(link?.target));
      if (!sourceId || !targetId) return false;
      if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return false;
      return true;
    });
    return {
      nodes: filteredNodes,
      links: filteredLinks
    };
  };

const createLinkKey = (sourceIdRaw, targetIdRaw, type) => {
  const sourceId = normalizeNodeId(sourceIdRaw);
  const targetId = normalizeNodeId(targetIdRaw);
  if (!sourceId || !targetId) return '';
  const typeKey = String(type || '').toLowerCase();
  return JSON.stringify([sourceId, targetId, typeKey]);
};

const buildLinkKeySet = (links = []) => {
  const keys = new Set();
  (Array.isArray(links) ? links : []).forEach((link) => {
    const sourceId = resolveLinkEndpointId(link?.source);
    const targetId = resolveLinkEndpointId(link?.target);
    if (!sourceId || !targetId) return;
    const key = createLinkKey(sourceId, targetId, link?.type);
    if (key) keys.add(key);
  });
  return keys;
};

const normalizeLinkForMerge = (link) => {
  if (!link) return { key: '', link: null };
  const sourceId = resolveLinkEndpointId(link.source);
  const targetId = resolveLinkEndpointId(link.target);
  if (!sourceId || !targetId) return { key: '', link: null };
  const normalizedLink = { ...link, source: sourceId, target: targetId };
  return {
    key: createLinkKey(sourceId, targetId, link.type),
    link: normalizedLink
  };
};

  const mergeNetworkUpdates = (prev, nodesToAdd = [], linksToAdd = [], nodeUpdates) => {
    const prevNodesArray = Array.isArray(prev?.nodes) ? prev.nodes : [];
    const aliasLookup = new Map();
    prevNodesArray.forEach(node => registerNodeAliases(aliasLookup, node));
    const updatesMap = new Map();
    const registerUpdate = (key, payload) => {
      const normalizedKey = normalizeNodeId(key);
      if (!normalizedKey) return;
      const candidate = finalizeNodeCandidate({ id: normalizedKey, ...payload });
      if (!candidate) return;
      const current = updatesMap.get(normalizedKey) || { id: normalizedKey };
      updatesMap.set(normalizedKey, mergeNodeAttributes(current, candidate));
      registerNodeAliases(aliasLookup, { ...candidate, id: normalizedKey });
    };

    if (nodeUpdates) {
      if (nodeUpdates instanceof Map) {
        nodeUpdates.forEach((value, key) => registerUpdate(key, value));
      } else {
        Object.entries(nodeUpdates).forEach(([key, value]) => registerUpdate(key, value));
      }
    }

    const updatedNodes = (prev.nodes || []).map(node => {
      const key = normalizeNodeId(node.id ?? node.name);
      if (!key) {
        registerNodeAliases(aliasLookup, node);
        return node;
      }
      const patch = updatesMap.get(key);
      if (!patch) {
        registerNodeAliases(aliasLookup, node);
        return node;
      }
      const merged = mergeNodeAttributes(node, patch);
      // If caller explicitly requested a reposition, force-update coordinates and clear fixed positions
      if (patch && patch.__reposition) {
        if (Number.isFinite(patch.x)) merged.x = patch.x;
        if (Number.isFinite(patch.y)) merged.y = patch.y;
        merged.fx = null; merged.fy = null; // unpin so simulation can settle around new spot
        // Also clear userPlaced flag so forces apply normally after programmatic reposition
        if (merged.userPlaced) merged.userPlaced = false;
        merged.vx = 0; merged.vy = 0;
        if (!Number.isFinite(merged.homeX)) merged.homeX = merged.x;
        if (!Number.isFinite(merged.homeY)) merged.homeY = merged.y;
      }
      registerNodeAliases(aliasLookup, merged);
      return merged;
    });
    const updatedNodeIndex = new Map();
    updatedNodes.forEach((node, idx) => {
      const key = normalizeNodeId(node?.id ?? node?.name);
      if (key) updatedNodeIndex.set(key, idx);
    });

    const existingIds = new Set(updatedNodes.map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean));
    const pendingNewMap = new Map();

    (nodesToAdd || []).forEach(node => {
      if (!node) return;
      const resolvedId = resolveAliasIdFromMap(aliasLookup, node);
      const candidateBase = resolvedId ? { ...node, id: resolvedId } : node;
      const candidate = finalizeNodeCandidate(candidateBase);
      if (!candidate) return;
      registerNodeAliases(aliasLookup, candidate);
      if (existingIds.has(candidate.id)) {
        const idx = updatedNodeIndex.get(candidate.id);
        if (typeof idx === 'number' && idx >= 0) {
          updatedNodes[idx] = mergeNodeAttributes(updatedNodes[idx], candidate);
        }
        const existingPending = pendingNewMap.get(candidate.id);
        if (existingPending) {
          pendingNewMap.set(candidate.id, mergeNodeAttributes(existingPending, candidate));
        }
        return;
      }
      if (pendingNewMap.has(candidate.id)) {
        pendingNewMap.set(candidate.id, mergeNodeAttributes(pendingNewMap.get(candidate.id), candidate));
      } else {
        pendingNewMap.set(candidate.id, candidate);
        existingIds.add(candidate.id);
      }
    });

    const existingLinkKeys = buildLinkKeySet(prev.links);
    const mergedLinks = [...(prev.links || [])];
    const pendingLinkKeys = new Set();
    (linksToAdd || []).forEach(link => {
      const normalized = normalizeLinkForMerge(link);
      if (!normalized.key || !normalized.link) return;
      if (existingLinkKeys.has(normalized.key) || pendingLinkKeys.has(normalized.key)) return;
      pendingLinkKeys.add(normalized.key);
      mergedLinks.push(normalized.link);
    });

    const combinedNodes = [...updatedNodes, ...pendingNewMap.values()].filter(n => {
      const nodeId = normalizeNodeId(n?.id ?? n?.name);
      if (!nodeId) return false;
      if (isPlaceholderName(nodeId)) return false;
      n.id = nodeId;
      if (typeof n.name !== 'string' || !n.name.trim()) {
        n.name = nodeId;
      }
      return true;
    });
    const validIds = new Set(combinedNodes.map(n => n.id));
    const filteredLinks = [];
    (mergedLinks || []).forEach(link => {
      const sourceId = normalizeNodeId(resolveLinkEndpointId(link?.source));
      const targetId = normalizeNodeId(resolveLinkEndpointId(link?.target));
      if (!sourceId || !targetId) return;
      if (!validIds.has(sourceId) || !validIds.has(targetId)) return;
      if (link.source !== sourceId) link.source = sourceId;
      if (link.target !== targetId) link.target = targetId;
      filteredLinks.push(link);
    });

    return sanitizeGraphData({
      nodes: combinedNodes,
      links: filteredLinks
    });
  };

  useEffect(() => {
    const nodesList = Array.isArray(networkData.nodes) ? networkData.nodes : [];
    const placeholderNodes = nodesList.filter(node => {
      const id = normalizeNodeId(node?.id ?? node?.name);
      return isPlaceholderName(id);
    });
    const nodeIdSet = new Set(
      nodesList
        .map(node => normalizeNodeId(node?.id ?? node?.name))
        .filter(Boolean)
    );
    const invalidLinks = [];
    (networkData.links || []).forEach(link => {
      const sourceId = resolveLinkEndpointId(link?.source);
      const targetId = resolveLinkEndpointId(link?.target);
      if (
        !sourceId ||
        !targetId ||
        isPlaceholderName(sourceId) ||
        isPlaceholderName(targetId) ||
        !nodeIdSet.has(sourceId) ||
        !nodeIdSet.has(targetId)
      ) {
        invalidLinks.push({ sourceId, targetId });
      }
    });
    if (!placeholderNodes.length && invalidLinks.length === 0) return;
    try {
      if (placeholderNodes.length > 0) {
        console.warn('[sanitizeGraphData] Removing placeholder nodes before simulation:', placeholderNodes.map(n => n?.id ?? n?.name));
      }
      if (invalidLinks.length > 0) {
        console.warn('[sanitizeGraphData] Detected invalid links; sanitizing graph', invalidLinks);
      }
    } catch (_) {}
    setNetworkData(prev => sanitizeGraphData(prev));
  }, [networkData]);

  // Function to expand all relationships for a node
  const expandAllRelationships = async (node) => {
    try {
      if (centerOnNodeRef.current && (node?.id || node?.name)) {
        centerOnNodeRef.current(node.id || node.name, { duration: 650 });
      }
      pushHistory('expand-all');
      setLoading(true);
      showHelperMessage('', 0);
      pendingHelperMessageRef.current = null;
      const relationshipType = 'all';
      const expansionBatchId = Date.now();
      let response, data;
      
      if (node.type === 'person') {
        response = await fetch(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 5 })
        });
      } else if (node.type === 'opera') {
        const { type: typedType, value: typedValue } = parseTypedId(node.id);
        const payload = { operaName: node.name };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        response = await fetch(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
        });
      } else if (node.type === 'book') {
        response = await fetch(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: node.name })
        });
      }

      if (response && handleRateLimitResponse(response)) {
        setLoading(false);
        return;
      }
      if (response) {
        let responseText = '';
        try {
          responseText = await response.text();
        } catch (parseErr) {
          console.warn('[expandAll] Failed to read response text', parseErr);
        }
        try {
          data = responseText ? JSON.parse(responseText) : {};
        } catch (parseErr) {
          console.warn('[expandAll] Failed to parse response JSON', parseErr, { responseText });
          data = {};
        }

        if (!response.ok) {
          if (handleUnauthorized(response)) return;
          if (response.status === 404) {
            showHelperMessage('No additional related nodes.');
            setError('');
            return;
          }
          const errorMessage = (data && (data.error || data.message))
            ? String(data.error || data.message)
            : `Failed to expand (${response.status})`;
          console.warn('[expandAll] Non-OK response', { status: response.status, statusText: response.statusText, errorMessage });
          setError(errorMessage);
          return;
        }

        if (response.ok) {
          // Merge new data with existing network
          const existingNodes = new Set(
            (networkData.nodes || []).map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean)
          );
          const existingLinks = buildLinkKeySet(networkData.links);
          const aliasLookup = new Map();
          (networkData.nodes || []).forEach(nodeEntry => registerNodeAliases(aliasLookup, nodeEntry));
          const nodeUpdates = new Map();
          
          let newNodes = [];
          let newLinks = [];
          const anchorId = normalizeNodeId(node.id ?? node.name);
          const anchorX = Number.isFinite(node?.x) ? node.x : 400;
          const anchorY = Number.isFinite(node?.y) ? node.y : 300;

          const registerNode = (payload) => {
            if (!payload) return null;
            const basePayload = {
              ...payload,
              x: Number.isFinite(payload?.x) ? payload.x : anchorX,
              y: Number.isFinite(payload?.y) ? payload.y : anchorY
            };
            const resolvedId = resolveAliasIdFromMap(aliasLookup, basePayload);
            if (resolvedId) {
              basePayload.id = resolvedId;
            }
            const payloadType = (basePayload.type || '').toLowerCase();
            let preparedPayload = basePayload;
            if (payloadType === 'opera') {
              preparedPayload = createOperaNodePayload(basePayload);
            } else if (payloadType === 'book') {
              preparedPayload = createBookNodePayload(basePayload);
            }
            const candidate = finalizeNodeCandidate(preparedPayload);
            if (!candidate) return null;
            registerNodeAliases(aliasLookup, candidate);
            const key = candidate.id;
            if (existingNodes.has(key)) {
              const currentPatch = nodeUpdates.get(key) || { id: key };
              nodeUpdates.set(key, mergeNodeAttributes(currentPatch, candidate));
              return key;
            }
            existingNodes.add(key);
            newNodes.push(candidate);
            return key;
          };

          const addLink = (sourceIdRaw, targetIdRaw, type, extra = {}) => {
            const sourceId = normalizeNodeId(sourceIdRaw);
            const targetId = normalizeNodeId(targetIdRaw);
            if (!sourceId || !targetId) return;
            if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return;
            const linkKey = createLinkKey(sourceId, targetId, type);
            if (existingLinks.has(linkKey)) return;
            existingLinks.add(linkKey);
            const cleanedExtra = { ...extra };
            const teacherRelSource = typeof cleanedExtra.teacher_rel_source === 'string' ? cleanedExtra.teacher_rel_source : null;
            const relationshipSourceProp = typeof cleanedExtra.relationship_source === 'string' ? cleanedExtra.relationship_source : null;
            const relationshipSourceCandidates = Array.isArray(cleanedExtra.relationshipSourceCandidates)
              ? cleanedExtra.relationshipSourceCandidates
              : [];
            delete cleanedExtra.relationshipSourceCandidates;
            delete cleanedExtra.teacher_rel_source;
            delete cleanedExtra.relationship_source;
            delete cleanedExtra.source;
            delete cleanedExtra.sourceInfo;
            delete cleanedExtra.target;
            delete cleanedExtra.sourceDisplay;
            const linkPayload = {
              source: sourceId,
              target: targetId,
              type,
              ...cleanedExtra
            };
            const relationshipSource = deriveRelationshipSourceText(
              linkPayload.relationshipSourceDisplay,
              linkPayload.sourceInfo,
              ...relationshipSourceCandidates
            );
            linkPayload.relationshipSourceDisplay = relationshipSource;
            linkPayload.teacher_rel_source = teacherRelSource;
            linkPayload.relationship_source = relationshipSourceProp;
            linkPayload.sourceInfo = relationshipSource;
            newLinks.push(linkPayload);
            console.log('[expandAll] addLink', { sourceId, targetId, type: linkPayload.type, label: linkPayload.label, linkKey });
          };
          
          // Handle different node types and their data structures
          if (node.type === 'person') {
            // Add new nodes from the expanded data for people
              if (data.teachers) {
                data.teachers.forEach(teacher => {
                  const teacherName = teacher.full_name || teacher.name || teacher.label || 'Unknown Teacher';
                  const typedIdRaw = teacher?.id || (teacher?.person_id ? `person:${teacher.person_id}` : '');
                  const teacherId = registerNode({
                    id: typedIdRaw || teacherName,
                    name: teacherName,
                    type: 'person',
                    voiceType: teacher.voice_type,
                    spelling_source: teacher.spelling_source || null,
                    voice_type_source: teacher.voice_type_source || null,
                    dates_source: teacher.dates_source || null,
                    birthplace_source: teacher.birthplace_source || null,
                    birthYear: teacher.birth_year,
                    deathYear: teacher.death_year
                  });
                if (!teacherId) return;
                addLink(teacherId, anchorId, 'taught', {
                  label: 'taught',
                  relationshipSourceCandidates: [teacher.teacher_rel_source, teacher.relationship_source, teacher.source]
                });
              });
              // Enrich teacher nodes with full details for CSV immediately
              enrichPersonNodes((data.teachers || []).map(t => t.full_name));
            }
            
              if (data.students) {
                data.students.forEach(student => {
                  const studentName = student.full_name || student.name || student.label || 'Unknown Student';
                  const typedIdRaw = student?.id || (student?.person_id ? `person:${student.person_id}` : '');
                  const studentId = registerNode({
                    id: typedIdRaw || studentName,
                    name: studentName,
                    type: 'person',
                    voiceType: student.voice_type,
                    spelling_source: student.spelling_source || null,
                    voice_type_source: student.voice_type_source || null,
                    dates_source: student.dates_source || null,
                    birthplace_source: student.birthplace_source || null,
                    birthYear: student.birth_year,
                    deathYear: student.death_year
                  });
                if (!studentId) return;
                addLink(anchorId, studentId, 'taught', {
                  label: 'taught',
                  relationshipSourceCandidates: [student.teacher_rel_source, student.relationship_source, student.source]
                });
              });
              // Enrich student nodes with full details for CSV immediately
              enrichPersonNodes((data.students || []).map(s => s.full_name));
            }
            
            {
              const familyList = (data.family || data.center?.family || []);
              if (familyList && familyList.length > 0) familyList.forEach(relative => {
                const relName = relative.full_name || relative.name || relative.label || 'Unknown Relative';
                const typedIdRaw = relative?.id || (relative?.person_id ? `person:${relative.person_id}` : '');
                const relId = registerNode({
                  id: typedIdRaw || relName,
                  name: relName,
                  type: 'person',
                  voiceType: relative.voice_type,
                  spelling_source: relative.spelling_source || null,
                  voice_type_source: relative.voice_type_source || null,
                  dates_source: relative.dates_source || null,
                  birthplace_source: relative.birthplace_source || null,
                  birthYear: relative.birth_year,
                  deathYear: relative.death_year
                });
                if (!relId) return;
                
                const relType = (relative.relationship_type || '').toLowerCase();
                let src = anchorId;
                let tgt = relId;
                if ((relType.includes('parent') && !relType.includes('of')) ||
                    (relType.includes('grandparent') && !relType.includes('of'))) {
                  src = relId;
                  tgt = anchorId;
                }
                addLink(src, tgt, 'family', {
                  label: relative.relationship_type || 'family',
                  relationshipSourceCandidates: [relative.teacher_rel_source, relative.relationship_source, relative.source]
                });
              });
              // Enrich family person nodes for CSV immediately
              if (familyList && familyList.length > 0) enrichPersonNodes(familyList.map(r => r.full_name));
            }
            
            if (data.works) {
              if (data.works.operas) {
                data.works.operas.forEach(opera => {
                  const displayName = deriveOperaName(opera, 'Unknown Opera');
                  const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
                  const operaId = registerNode(createOperaNodePayload({
                    id: typedIdRaw || displayName,
                    name: displayName,
                    opera_id: opera?.opera_id,
                    opera_name: displayName,
                    version: opera?.version
                  }));
                  if (!operaId) return;
                  addLink(anchorId, operaId, 'premiered', {
                    label: 'premiered role in',
                    role: opera.role,
                    relationshipSourceCandidates: [opera.source, opera.relationship_source]
                  });
                });
              }

              if (Array.isArray(data.works.composedOperas) && data.works.composedOperas.length > 0) {
                data.works.composedOperas.forEach(opera => {
                  const displayName = deriveOperaName(opera, 'Unknown Opera');
                  const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
                  const operaId = registerNode(createOperaNodePayload({
                    id: typedIdRaw || displayName,
                    name: displayName,
                    opera_id: opera?.opera_id,
                    opera_name: displayName,
                    version: opera?.version
                  }));
                  if (!operaId) return;
                  addLink(anchorId, operaId, 'wrote', {
                    label: 'wrote',
                    relationshipSourceCandidates: [
                      opera.opera_source_text,
                      opera.opera_source,
                      opera.source,
                      opera.relationship_source
                    ]
                  });
                });
              }

              if (data.works.books) {
                data.works.books.forEach(book => {
                  const bookLabel = String(book.title || book.name || '').trim() || 'Unknown Book';
                  const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
                  const bookId = registerNode(createBookNodePayload({
                    id: typedIdRaw || bookLabel,
                    name: bookLabel,
                    book_id: book?.book_id,
                    title: bookLabel,
                    link: book?.link
                  }));
                  if (!bookId) return;
                  addLink(anchorId, bookId, 'authored', {
                    label: 'authored',
                    relationshipSourceCandidates: [book.source, book.relationship_source]
                  });
                });
              }
              if (data.works.editedBooks) {
                data.works.editedBooks.forEach(book => {
                  const bookLabel = String(book.title || book.name || '').trim() || 'Unknown Book';
                  const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
                  const bookId = registerNode(createBookNodePayload({
                    id: typedIdRaw || bookLabel,
                    name: bookLabel,
                    book_id: book?.book_id,
                    title: bookLabel,
                    link: book?.link
                  }));
                  if (!bookId) return;
                  addLink(anchorId, bookId, 'edited', {
                    label: 'edited',
                    relationshipSourceCandidates: [book.source, book.relationship_source]
                  });
                });
              }
            }
          } else if (node.type === 'opera') {
            if (data.premieredRoles) {
              data.premieredRoles.forEach(role => {
                const singerId = registerNode({
                  id: role.singer,
                  name: role.singer,
                  type: 'person',
                  voiceType: role.voice_type
                });
                if (!singerId) return;
                addLink(singerId, anchorId, 'premiered', {
                  label: 'premiered role in',
                  role: role.role,
                  relationshipSourceCandidates: [role.source, role.relationship_source]
                });
              });
            }
            
            let composerLinked = false;
            if (Array.isArray(data.wrote) && data.wrote.length > 0) {
              data.wrote.forEach(entry => {
                const composerName = entry.composer || entry.name;
                if (!composerName) return;
                const composerId = registerNode({
                  id: composerName,
                  name: composerName,
                  type: 'person',
                  voiceType: 'Composer'
                });
                if (!composerId) return;
                addLink(composerId, anchorId, 'wrote', {
                  label: 'wrote',
                  relationshipSourceCandidates: [
                    entry.opera_source_text,
                    entry.source,
                    entry.relationship_source,
                    entry.opera_source_url
                  ]
                });
                composerLinked = true;
              });
            }

            if (!composerLinked && data.opera && data.opera.composer) {
              const fallbackComposerId = registerNode({
                id: data.opera.composer,
                name: data.opera.composer,
                type: 'person',
                voiceType: 'Composer'
              });
              if (fallbackComposerId) {
                addLink(fallbackComposerId, anchorId, 'wrote', {
                  label: 'wrote',
                  relationshipSourceCandidates: [data.opera?.source, data.opera?.relationship_source]
                });
              }
            }
          } else if (node.type === 'book') {
            const authors = Array.isArray(data.authors) && data.authors.length > 0
              ? data.authors.map(entry => ({
                  name: entry.author,
                  voice_type: entry.voice_type,
                  relationship_source: entry.relationship_source
                }))
              : (data.book && data.book.author ? [{ name: data.book.author }] : []);
            authors.forEach(author => {
              if (!author?.name) return;
              const authorId = registerNode({
                id: author.name,
                name: author.name,
                type: 'person',
                voiceType: author.voice_type
              });
              if (!authorId) return;
              addLink(authorId, anchorId, 'authored', {
                label: 'authored',
                relationshipSourceCandidates: [
                  author.relationship_source,
                  data.book?.source,
                  data.book?.relationship_source
                ]
              });
            });

            if (Array.isArray(data.editors) && data.editors.length > 0) {
              data.editors.forEach(editor => {
                const editorName = editor.editor || editor.name;
                if (!editorName) return;
                const editorId = registerNode({
                  id: editorName,
                  name: editorName,
                  type: 'person',
                  voiceType: editor.voice_type
                });
                if (!editorId) return;
                addLink(editorId, anchorId, 'edited', {
                  label: 'edited',
                  relationshipSourceCandidates: [
                    editor.relationship_source,
                    data.book?.source,
                    data.book?.relationship_source
                  ]
                });
              });
            }
          }

          const sanitizedAdditions = sanitizeIncrementalGraph(newNodes, newLinks, {
            anchorId,
            existingNodeIds: existingNodes
          });
          newNodes = sanitizedAdditions.nodes || [];
          newLinks = sanitizedAdditions.links || [];
          newNodes.forEach(nodeEntry => registerNodeAliases(aliasLookup, nodeEntry));

          if (newNodes.length > 0) {
            newNodes.forEach(n => {
              if (!n) return;
              n.recentlyExpandedAt = expansionBatchId;
              n.expansionBatchId = expansionBatchId;
            });
          }

          attachNewNodesToAnchor({
            anchorId,
            anchorType: node.type,
            relationshipType,
            newNodes,
            newLinks,
            existingLinks: networkData.links,
            addLink
          });

          // Mark expansion-internal links so the main simulation keeps the entire batch compact
          if (newNodes.length > 0 && Array.isArray(newLinks)) {
            const addedIdSet = new Set(newNodes.map(n => n && n.id).filter(Boolean));
            let flagged = 0;
            newLinks.forEach(l => {
              const s = resolveLinkEndpointId(l?.source);
              const t = resolveLinkEndpointId(l?.target);
              if (!s || !t) return;
              // Anchor <-> newly added nodes
              if ((s === anchorId && addedIdSet.has(t)) || (t === anchorId && addedIdSet.has(s))) {
                if (!l.expansionInternal) { l.expansionInternal = true; flagged += 1; }
                return;
              }
              // Links entirely within the newly-added batch
              if (addedIdSet.has(s) && addedIdSet.has(t)) {
                if (!l.expansionInternal) { l.expansionInternal = true; flagged += 1; }
              }
            });
            debugLog('expand-flag-internal-links', { anchorId, newCount: newNodes.length, linkCount: newLinks.length, flagged });
          }

          if (newNodes.length > 0) {
            const containerEl = document.querySelector('div[style*="height:"] > svg')?.parentElement || null;
            const widthGuess = containerEl ? containerEl.clientWidth : 800;
            const heightGuess = visualizationHeight || 600;
            const spawn = computeSpawnOutsideBBox(
              networkData?.nodes || [],
              { x: anchorX, y: anchorY, key: anchorId },
              280,
              { width: widthGuess, height: heightGuess, pad: 60 }
            );
            const { min: ringMin, max: ringMax, spacing: ringSpacing } = getExpansionRingConfig(newNodes.length);
            const ringRadius = computeRingRadius(newNodes.length, ringMin, ringMax, ringSpacing);
            debugLog('expand-spawn', { anchorId, anchorX, anchorY, spawn, ringRadius, newCount: newNodes.length });
            newNodes.forEach((n, idx) => {
              if (!n) return;
              const angle = (idx / newNodes.length) * Math.PI * 2;
              n.x = spawn.x + ringRadius * Math.cos(angle);
              n.y = spawn.y + ringRadius * Math.sin(angle);
            });
            // Also move the anchor to the spawn center so the new group forms around it
            try {
              nodeUpdates.set(anchorId, {
                id: anchorId,
                x: spawn.x,
                y: spawn.y,
                __reposition: true,
                recentlyExpandedAt: expansionBatchId,
                expansionBatchId: expansionBatchId
              });
            } catch (_) {}
            extendDateRangesForNodes(newNodes);
          }

          console.log('[expandAll] newNodes:', newNodes.length, 'newLinks:', newLinks.length, 'relationship:', relationshipType);
          setNetworkData(prev => {
            const merged = mergeNetworkUpdates(prev, newNodes, newLinks, nodeUpdates);
            const fallback = resolveFallbackConfig(relationshipType, node.type);
            const ensuredLinks = Array.isArray(merged.links) ? [...merged.links] : [];
            ensureNodeConnectivity(merged.nodes, ensuredLinks, {
              primaryId: anchorId,
              fallbackType: fallback.type,
              fallbackLabel: fallback.label || ''
            });
            const next = sanitizeGraphData({ nodes: merged.nodes, links: ensuredLinks });
            console.log('[expandAll] mergedCounts -> nodes:', next.nodes?.length, 'links:', next.links?.length);
            if (isLayoutDebug()) {
              try {
                const anchor = (next.nodes || []).find(n => n && n.id === anchorId);
                const flagged = (next.links || []).reduce((acc,l)=>acc+(l.expansionInternal?1:0),0);
                debugLog('post-merge-anchor', { anchorId, x: anchor?.x, y: anchor?.y, flaggedLinks: flagged, totalLinks: next.links?.length });
              } catch (_) {}
            }
            return next;
          });
          const pendingHelperMessage = newNodes.length === 0
            ? { text: 'No additional related nodes.', duration: 3200 }
            : { text: '', duration: 0 };
          pendingHelperMessageRef.current = pendingHelperMessage;
          // Refresh counts for the expanded node to keep context menu accurate
          try {
            const updatedCounts = await fetchActualCounts(node);
            setActualCounts(prev => ({ ...prev, [node.id]: updatedCounts }));
          } catch (e) {}

          if (newNodes.length > 0) {
            setIsExpansionSimulation(true);
            setShouldRunSimulation(true);
          }

          // Keep hierarchy root unchanged to ensure additive expansion
        } else {
          setError(data.error);
        }
      }
    } catch (err) {
      setError('Failed to expand relationships');
    } finally {
      setLoading(false);
    }
  };
  // Function to expand specific relationship type
  const expandSpecificRelationship = async (node, relationshipType) => {
    try {
      if (centerOnNodeRef.current && (node?.id || node?.name)) {
        centerOnNodeRef.current(node.id || node.name, { duration: 650 });
      }
      pushHistory(`expand-${relationshipType}`);
      setLoading(true);
      const expansionBatchId = Date.now();
      showHelperMessage('', 0);
      pendingHelperMessageRef.current = null;
      let response, data;
      
      if (node.type === 'person') {
        response = await fetchWithRetry(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 2 })
        }, { retries: 2, baseDelay: 600 });
      } else if (node.type === 'opera') {
        const { type: typedType, value: typedValue } = parseTypedId(node.id);
        const payload = { operaName: node.name };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
        }, { retries: 2, baseDelay: 600 });
      } else if (node.type === 'book') {
        response = await fetchWithRetry(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: node.name })
        }, { retries: 2, baseDelay: 600 });
      }

      if (response) {
        let responseText = '';
        try {
          responseText = await response.text();
        } catch (parseErr) {
          console.warn('[expandSpecific] Failed to read response text', parseErr);
        }
        try {
          data = responseText ? JSON.parse(responseText) : {};
        } catch (parseErr) {
          console.warn('[expandSpecific] Failed to parse response JSON', parseErr, { responseText });
          data = {};
        }

        if (!response.ok) {
          if (handleUnauthorized(response)) return;
          if (response.status === 404) {
            showHelperMessage('No additional related nodes.');
            setError('');
            return;
          }
          const errorMessage = (data && (data.error || data.message))
            ? String(data.error || data.message)
            : `Failed to expand (${response.status})`;
          console.warn('[expandSpecific] Non-OK response', { status: response.status, statusText: response.statusText, errorMessage });
          setError(errorMessage);
          return;
        }

        if (response.ok) {
          // Merge new data with existing network
          const existingNodes = new Set(
            (networkData.nodes || []).map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean)
          );
          const existingLinks = buildLinkKeySet(networkData.links);
          const aliasLookup = new Map();
          (networkData.nodes || []).forEach(nodeEntry => registerNodeAliases(aliasLookup, nodeEntry));
          const nodeUpdates = new Map();
          
          console.log(`🔍 Expanding "${relationshipType}" for "${node.name}"`);
          console.log(`📊 Current network: ${networkData.nodes.length} nodes, ${networkData.links.length} links`);
          console.log(`🗂️ Existing node IDs:`, Array.from(existingNodes));
          
          let newNodes = [];
          let newLinks = [];
          const anchorId = normalizeNodeId(node.id ?? node.name);
          const anchorX = Number.isFinite(node?.x) ? node.x : 400;
          const anchorY = Number.isFinite(node?.y) ? node.y : 300;

          const registerNode = (payload) => {
            if (!payload) return null;
            const basePayload = {
              ...payload,
              x: Number.isFinite(payload?.x) ? payload.x : anchorX,
              y: Number.isFinite(payload?.y) ? payload.y : anchorY
            };
            const resolvedId = resolveAliasIdFromMap(aliasLookup, basePayload);
            if (resolvedId) {
              basePayload.id = resolvedId;
            }
            const payloadType = (basePayload.type || '').toLowerCase();
            let preparedPayload = basePayload;
            if (payloadType === 'opera') {
              preparedPayload = createOperaNodePayload(basePayload);
            } else if (payloadType === 'book') {
              preparedPayload = createBookNodePayload(basePayload);
            }
            const candidate = finalizeNodeCandidate(preparedPayload);
            if (!candidate) return null;
            registerNodeAliases(aliasLookup, candidate);
            const key = candidate.id;
            if (existingNodes.has(key)) {
              const currentPatch = nodeUpdates.get(key) || { id: key };
              nodeUpdates.set(key, mergeNodeAttributes(currentPatch, candidate));
              return key;
            }
            existingNodes.add(key);
            newNodes.push(candidate);
            return key;
          };

          const addLink = (sourceIdRaw, targetIdRaw, type, extra = {}) => {
            const sourceId = normalizeNodeId(sourceIdRaw);
            const targetId = normalizeNodeId(targetIdRaw);
            if (!sourceId || !targetId) return;
            if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return;
            const linkKey = createLinkKey(sourceId, targetId, type);
            if (existingLinks.has(linkKey)) return;
            existingLinks.add(linkKey);
            const cleanedExtra = { ...extra };
            const teacherRelSource = typeof cleanedExtra.teacher_rel_source === 'string' ? cleanedExtra.teacher_rel_source : null;
            const relationshipSourceProp = typeof cleanedExtra.relationship_source === 'string' ? cleanedExtra.relationship_source : null;
            const relationshipSourceCandidates = Array.isArray(cleanedExtra.relationshipSourceCandidates)
              ? cleanedExtra.relationshipSourceCandidates
              : [];
            delete cleanedExtra.relationshipSourceCandidates;
            delete cleanedExtra.teacher_rel_source;
            delete cleanedExtra.relationship_source;
            delete cleanedExtra.source;
            delete cleanedExtra.sourceInfo;
            const linkPayload = {
              source: sourceId,
              target: targetId,
              type,
              ...cleanedExtra
            };
            const relationshipSource = deriveRelationshipSourceText(
              linkPayload.relationshipSourceDisplay,
              linkPayload.sourceInfo,
              ...relationshipSourceCandidates
            );
            linkPayload.relationshipSourceDisplay = relationshipSource;
            linkPayload.teacher_rel_source = teacherRelSource;
            linkPayload.relationship_source = relationshipSourceProp;
            linkPayload.sourceInfo = relationshipSource;
            newLinks.push(linkPayload);
            console.log('[expandSpecific] addLink', { sourceId, targetId, type: linkPayload.type, label: linkPayload.label, linkKey });
          };
          
          // Handle specific relationship types for people
          if (node.type === 'person') {
            if (relationshipType === 'taughtBy' && data.teachers) {
              data.teachers.forEach(teacher => {
                const teacherName = teacher.full_name || teacher.name || teacher.label || 'Unknown Teacher';
                const typedIdRaw = teacher?.id || (teacher?.person_id ? `person:${teacher.person_id}` : '');
                const teacherId = registerNode({
                  id: typedIdRaw || teacherName,
                  name: teacherName,
                  type: 'person',
                  voiceType: teacher.voice_type,
                  spelling_source: teacher.spelling_source || null,
                  voice_type_source: teacher.voice_type_source || null,
                  dates_source: teacher.dates_source || null,
                  birthplace_source: teacher.birthplace_source || null,
                  birthYear: teacher.birth_year,
                  deathYear: teacher.death_year
                });
                if (!teacherId) return;
                addLink(teacherId, anchorId, 'taught', {
                  label: 'taught',
                  relationshipSourceCandidates: [teacher.teacher_rel_source, teacher.relationship_source, teacher.source]
                });
              });
              enrichPersonNodes((data.teachers || []).map(t => t.full_name));
            }
            
            if (relationshipType === 'taught' && data.students) {
              data.students.forEach(student => {
                const studentName = student.full_name || student.name || student.label || 'Unknown Student';
                const typedIdRaw = student?.id || (student?.person_id ? `person:${student.person_id}` : '');
                const studentId = registerNode({
                  id: typedIdRaw || studentName,
                  name: studentName,
                  type: 'person',
                  voiceType: student.voice_type,
                  spelling_source: student.spelling_source || null,
                  voice_type_source: student.voice_type_source || null,
                  dates_source: student.dates_source || null,
                  birthplace_source: student.birthplace_source || null,
                  birthYear: student.birth_year,
                  deathYear: student.death_year
                });
                if (!studentId) return;
                addLink(anchorId, studentId, 'taught', {
                  label: 'taught',
                  relationshipSourceCandidates: [student.teacher_rel_source, student.relationship_source, student.source]
                });
              });
              enrichPersonNodes((data.students || []).map(s => s.full_name));
            }
            
            if ((relationshipType === 'parent' || relationshipType === 'parentOf' || 
                 relationshipType === 'spouse' || relationshipType === 'spouseOf' ||
                 relationshipType === 'grandparent' || relationshipType === 'grandparentOf' ||
                 relationshipType === 'sibling') && data.family) {
              data.family.forEach(relative => {
                const relType = relative.relationship_type?.toLowerCase() || '';
                let shouldInclude = false;
                
                if (relationshipType === 'parent' && relType.includes('parent') && !relType.includes('of')) {
                  shouldInclude = true;
                } else if (relationshipType === 'parentOf' && relType.includes('parent') && relType.includes('of')) {
                  shouldInclude = true;
                } else if (relationshipType === 'spouse' && relType.includes('spouse')) {
                  shouldInclude = true;
                } else if (relationshipType === 'spouseOf' && relType.includes('spouse')) {
                  shouldInclude = true;
                } else if (relationshipType === 'grandparent' && relType.includes('grandparent') && !relType.includes('of')) {
                  shouldInclude = true;
                } else if (relationshipType === 'grandparentOf' && relType.includes('grandparent') && relType.includes('of')) {
                  shouldInclude = true;
                } else if (relationshipType === 'sibling' && relType.includes('sibling')) {
                  shouldInclude = true;
                }
                
                if (shouldInclude) {
                  const relName = relative.full_name || relative.name || relative.label || 'Unknown Relative';
                  const typedIdRaw = relative?.id || (relative?.person_id ? `person:${relative.person_id}` : '');
                  const relId = registerNode({
                    id: typedIdRaw || relName,
                    name: relName,
                    type: 'person',
                    voiceType: relative.voice_type,
                    spelling_source: relative.spelling_source || null,
                    voice_type_source: relative.voice_type_source || null,
                    dates_source: relative.dates_source || null,
                    birthplace_source: relative.birthplace_source || null,
                    birthYear: relative.birth_year,
                    deathYear: relative.death_year
                  });
                  if (!relId) return;
                  
                  const dirType = (relative.relationship_type || '').toLowerCase();
                  let src = anchorId;
                  let tgt = relId;
                  if ((dirType.includes('parent') && !dirType.includes('of')) ||
                      (dirType.includes('grandparent') && !dirType.includes('of'))) {
                    src = relId;
                    tgt = anchorId;
                  }
                  addLink(src, tgt, 'family', {
                    label: relative.relationship_type || 'family',
                    relationshipSourceCandidates: [relative.teacher_rel_source, relative.relationship_source, relative.source]
                  });
                }
              });
              enrichPersonNodes((data.family || []).map(r => r.full_name));
            }
            
            if (relationshipType === 'authored' && data.works && data.works.books) {
              data.works.books.forEach(book => {
                const bookLabel = String(book.title || book.name || '').trim() || 'Unknown Book';
                const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
                const bookId = registerNode(createBookNodePayload({
                  id: typedIdRaw || bookLabel,
                  name: bookLabel,
                  book_id: book?.book_id,
                  title: bookLabel,
                  link: book?.link
                }));
                if (!bookId) return;
                addLink(anchorId, bookId, 'authored', {
                  label: 'authored',
                  relationshipSourceCandidates: [book.source, book.relationship_source]
                });
              });
            }

            if (relationshipType === 'edited' && data.works && data.works.editedBooks) {
              data.works.editedBooks.forEach(book => {
                const bookLabel = String(book.title || book.name || '').trim() || 'Unknown Book';
                const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
                const bookId = registerNode(createBookNodePayload({
                  id: typedIdRaw || bookLabel,
                  name: bookLabel,
                  book_id: book?.book_id,
                  title: bookLabel,
                  link: book?.link
                }));
                if (!bookId) return;
                addLink(anchorId, bookId, 'edited', {
                  label: 'edited',
                  relationshipSourceCandidates: [book.source, book.relationship_source]
                });
              });
            }
            
            if (relationshipType === 'premieredRoleIn' && data.works && data.works.operas) {
              data.works.operas.forEach(opera => {
                const displayName = deriveOperaName(opera, 'Unknown Opera');
                const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
                const operaId = registerNode(createOperaNodePayload({
                  id: typedIdRaw || displayName,
                  name: displayName,
                  opera_id: opera?.opera_id,
                  opera_name: displayName,
                  version: opera?.version
                }));
                if (!operaId) return;
                addLink(anchorId, operaId, 'premiered', {
                  label: 'premiered role in',
                  role: opera.role,
                  relationshipSourceCandidates: [opera.source, opera.relationship_source]
                });
              });
            }
          } else if (node.type === 'opera') {
            // Handle singers who premiered roles in this opera
            if (relationshipType === 'premieredRoleIn' && data.premieredRoles) {
              data.premieredRoles.forEach(role => {
                const singerId = registerNode({
                  id: role.singer,
                  name: role.singer,
                  type: 'person',
                  voiceType: role.voice_type
                });
                if (!singerId) return;
                addLink(singerId, anchorId, 'premiered', {
                  label: 'premiered role in',
                  role: role.role,
                  relationshipSourceCandidates: [role.source, role.relationship_source]
                });
              });
            }
            
            // Handle composer who wrote this opera
            if (relationshipType === 'wrote' && data.opera && data.opera.composer) {
              const composerId = registerNode({
                id: data.opera.composer,
                name: data.opera.composer,
                type: 'person',
                voiceType: 'Composer'
              });
              if (composerId) {
                addLink(composerId, anchorId, 'composed', {
                  label: 'composed',
                  relationshipSourceCandidates: [data.opera?.source, data.opera?.relationship_source]
                });
              }
            }
          } else if (node.type === 'book') {
            if ((relationshipType === 'authored' || relationshipType === 'authoredBy') && data.book && data.book.author) {
              const authorId = registerNode({
                id: data.book.author,
                name: data.book.author,
                type: 'person'
              });
              if (authorId) {
                addLink(authorId, anchorId, 'authored', {
                  label: 'authored',
                  relationshipSourceCandidates: [data.book?.source, data.book?.relationship_source]
                });
              }
            }
          }
          
          const sanitizedAdditions = sanitizeIncrementalGraph(newNodes, newLinks, {
            anchorId,
            existingNodeIds: existingNodes
          });
          newNodes = sanitizedAdditions.nodes || [];
          newLinks = sanitizedAdditions.links || [];
          newNodes.forEach(nodeEntry => registerNodeAliases(aliasLookup, nodeEntry));

          if (newNodes.length > 0) {
            newNodes.forEach(n => {
              if (!n) return;
              n.recentlyExpandedAt = expansionBatchId;
              n.expansionBatchId = expansionBatchId;
            });
          }

          if (newNodes.length > 0 && anchorId) {
            const attachedToAnchor = new Set();
            (networkData.links || []).forEach(link => {
              const sourceId = normalizeNodeId(typeof link.source === 'string' ? link.source : link.source?.id);
              const targetId = normalizeNodeId(typeof link.target === 'string' ? link.target : link.target?.id);
              if (sourceId === anchorId && targetId) attachedToAnchor.add(targetId);
              if (targetId === anchorId && sourceId) attachedToAnchor.add(sourceId);
            });
            newLinks.forEach(link => {
              const sourceId = normalizeNodeId(link.source);
              const targetId = normalizeNodeId(link.target);
              if (sourceId === anchorId && targetId) attachedToAnchor.add(targetId);
              if (targetId === anchorId && sourceId) attachedToAnchor.add(sourceId);
            });

            const relationshipLabel = typeof relationshipType === 'string' && relationshipType.trim()
              ? relationshipType.trim()
              : '';
            const normalizedLabel = relationshipLabel.toLowerCase();
            const fallbackType = normalizedLabel.includes('parent') ||
              normalizedLabel.includes('sibling') ||
              normalizedLabel.includes('grandparent') ||
              normalizedLabel.includes('spouse')
              ? 'family'
              : normalizedLabel.includes('taught')
              ? 'taught'
              : normalizedLabel.includes('premiered')
              ? 'premiered'
              : normalizedLabel.includes('wrote') || normalizedLabel.includes('composed')
              ? 'wrote'
              : normalizedLabel.includes('authored')
              ? 'authored'
              : normalizedLabel.includes('edited')
              ? 'edited'
              : '';

            newNodes.forEach(n => {
              const nodeId = normalizeNodeId(n.id);
              if (!nodeId || attachedToAnchor.has(nodeId)) return;
              if (!fallbackType) return;
              addLink(anchorId, nodeId, fallbackType, { label: relationshipLabel || fallbackType });
              attachedToAnchor.add(nodeId);
            });

            // Mark expansion-internal links so global simulation keeps the entire batch compact
            if (Array.isArray(newLinks) && newNodes.length > 0) {
              const addedIdSet = new Set(newNodes.map(n => n && n.id).filter(Boolean));
              newLinks.forEach(l => {
                const s = resolveLinkEndpointId(l?.source);
                const t = resolveLinkEndpointId(l?.target);
                if (!s || !t) return;
                // Anchor <-> newly added nodes
                if ((s === anchorId && addedIdSet.has(t)) || (t === anchorId && addedIdSet.has(s))) {
                  l.expansionInternal = true;
                  return;
                }
                // Links entirely within the newly-added batch
                if (addedIdSet.has(s) && addedIdSet.has(t)) {
                  l.expansionInternal = true;
                }
              });
            }

            const simNodeMap = new Map();
            const simNodes = [];
            const register = (id, x, y, pin = false) => {
              const simNode = { id, x, y };
              if (pin) {
                simNode.fx = x;
                simNode.fy = y;
              }
              simNodes.push(simNode);
              simNodeMap.set(id, simNode);
              return simNode;
            };

            const containerEl = document.querySelector('div[style*="height:"] > svg')?.parentElement || null;
            const widthGuess = containerEl ? containerEl.clientWidth : 800;
            const heightGuess = visualizationHeight || 600;
            const spawn = computeSpawnOutsideBBox(
              networkData?.nodes || [],
              { x: anchorX, y: anchorY, key: anchorId },
              280,
              { width: widthGuess, height: heightGuess, pad: 60 }
            );
            register(anchorId, spawn.x, spawn.y, true);
            const { min: simRingMin, max: simRingMax, spacing: simRingSpacing } = getExpansionRingConfig(newNodes.length);
            const initialRadius = computeRingRadius(newNodes.length, simRingMin, simRingMax, simRingSpacing);
            newNodes.forEach((n, idx) => {
              const angle = (idx / newNodes.length) * Math.PI * 2;
              const px = spawn.x + Math.cos(angle) * initialRadius;
              const py = spawn.y + Math.sin(angle) * initialRadius;
              register(n.id, px, py, false);
            });

            const simLinks = newLinks
              .map(link => ({
                source: typeof link.source === 'string' ? link.source : link.source?.id,
                target: typeof link.target === 'string' ? link.target : link.target?.id
              }))
              .filter(l => simNodeMap.has(l.source) && simNodeMap.has(l.target))
              .map(l => ({
                source: simNodeMap.get(l.source),
                target: simNodeMap.get(l.target)
              }));

            if (simLinks.length > 0) {
              const sim = d3.forceSimulation(simNodes)
                .force('link', d3.forceLink(simLinks).distance(140).strength(0.9))
                .force('charge', d3.forceManyBody().strength(-240))
                .force('collision', d3.forceCollide().radius(80))
                .force('center', d3.forceCenter(spawn.x, spawn.y))
                .stop();

              for (let i = 0; i < 200; i += 1) sim.tick();
              sim.stop();
            }

            newNodes.forEach(n => {
              const simNode = simNodeMap.get(n.id);
              if (simNode) {
                n.x = simNode.x;
                n.y = simNode.y;
              } else {
                n.x = spawn.x;
                n.y = spawn.y;
              }
            });

            try {
              nodeUpdates.set(anchorId, {
                id: anchorId,
                x: spawn.x,
                y: spawn.y,
                __reposition: true,
                recentlyExpandedAt: expansionBatchId,
                expansionBatchId: expansionBatchId
              });
            } catch (_) {}
            extendDateRangesForNodes(newNodes);
          }

          console.log('[expandSpecific] newNodes:', newNodes.length, 'newLinks:', newLinks.length, 'anchor:', anchorId, 'relationship:', relationshipType);
          setNetworkData(prev => {
            const merged = mergeNetworkUpdates(prev, newNodes, newLinks, nodeUpdates);
            const fallback = resolveFallbackConfig(relationshipType, node.type);
            const ensuredLinks = Array.isArray(merged.links) ? [...merged.links] : [];
            ensureNodeConnectivity(merged.nodes, ensuredLinks, {
              primaryId: anchorId,
              fallbackType: fallback.type,
              fallbackLabel: fallback.label || ''
            });
            const next = sanitizeGraphData({ nodes: merged.nodes, links: ensuredLinks });
            console.log('[expandSpecific] mergedCounts -> nodes:', next.nodes?.length, 'links:', next.links?.length);
            if (isLayoutDebug()) {
              try {
                const anchor = (next.nodes || []).find(n => n && n.id === anchorId);
                const flagged = (next.links || []).reduce((acc,l)=>acc+(l.expansionInternal?1:0),0);
                debugLog('post-merge-anchor', { anchorId, x: anchor?.x, y: anchor?.y, flaggedLinks: flagged, totalLinks: next.links?.length });
              } catch (_) {}
            }
            return next;
          });

      const pendingHelperMessage = newNodes.length === 0
        ? { text: 'No additional related nodes.', duration: 3200 }
        : { text: '', duration: 0 };
      pendingHelperMessageRef.current = pendingHelperMessage;

      if (newNodes.length > 0) {
        setIsExpansionSimulation(true);
        setShouldRunSimulation(true);
      }
        } else {
          setError(data.error);
        }
      }
    } catch (err) {
      console.error('[expandSpecific] Failed to expand', { nodeId: node?.id, relationshipType, err });
      if (err && err.name === 'AbortError') {
        return;
      }
      const message = typeof err?.message === 'string' && err.message.trim()
        ? err.message.trim()
        : 'Failed to expand specific relationship';
      setError(message.includes('Too many requests') ? message : 'Failed to expand specific relationship');
    } finally {
      setLoading(false);
    }
  };

  const showHelperMessage = (message, duration = 3200) => {
    if (helperMessageTimeoutRef.current) {
      clearTimeout(helperMessageTimeoutRef.current);
      helperMessageTimeoutRef.current = null;
    }
    setHelperMessage(message);
    if (message && duration > 0) {
      helperMessageTimeoutRef.current = setTimeout(() => {
        setHelperMessage('');
        helperMessageTimeoutRef.current = null;
      }, duration);
    }
  };

  function flushPendingHelperMessage() {
    const pending = pendingHelperMessageRef.current;
    pendingHelperMessageRef.current = null;
    if (!pending) return;
    const text = typeof pending.text === 'string' ? pending.text : '';
    const duration = Number.isFinite(pending.duration) ? pending.duration : 0;
    showHelperMessage(text, duration);
  }

  const clearPendingNodeAction = () => {
    if (nodeClickTimeoutRef.current) {
      clearTimeout(nodeClickTimeoutRef.current);
      nodeClickTimeoutRef.current = null;
    }
  };

  const closeNodeMenusAndCards = () => {
    try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
    try { setLinkContextMenu(createLinkContextMenuState()); } catch (_) {}
    try { setProfileCard({ show: false, data: null }); } catch (_) {}
  };

  const selectNodeForFeedback = (node) => {
    if (!node) return;
    setTimeout(() => {
      setSelectedNode(node);
    }, 0);
  };

  const scheduleNodeExpansion = (node) => {
    clearPendingNodeAction();
    nodeClickTimeoutRef.current = setTimeout(() => {
      nodeClickTimeoutRef.current = null;
      lastTappedNodeIdRef.current = null;
      expandAllRelationships(node);
    }, 220);
  };

  const handleNodeSingleActivation = (node) => {
    closeNodeMenusAndCards();
    selectNodeForFeedback(node);
    scheduleNodeExpansion(node);
  };

  const handleNodeDoubleActivation = (node) => {
    closeNodeMenusAndCards();
    selectNodeForFeedback(node);
    clearPendingNodeAction();
    lastTappedNodeIdRef.current = null;
    pushHistory('node-search');
    triggerNodeSearch(node);
  };

  const triggerNodeSearch = (node) => {
    if (!node) return;
    try { window.__cmg_resetZoom && window.__cmg_resetZoom(); } catch (_) {}
    showHelperMessage('', 0);
    if (currentCenterNode !== node.id) {
      setCurrentCenterNode(node.id);
    }
    if (node.type === 'person') {
      const mockSearchItem = {
        name: node.name,
        properties: {
          full_name: node.name,
          voice_type: node.voiceType,
          birth_year: node.birthYear,
          death_year: node.deathYear
        }
      };
      setSearchType('singers');
      getItemDetails(mockSearchItem, 'singers');
    } else if (node.type === 'opera') {
      const mockSearchItem = {
        properties: {
          title: node.opera_name || node.name,
          opera_name: node.opera_name || node.name,
          opera_id: node.opera_id,
          version: node.version
        }
      };
      setSearchType('operas');
      getItemDetails(mockSearchItem, 'operas');
    } else if (node.type === 'book') {
      const mockSearchItem = {
        properties: {
          title: node.title || node.name,
          book_id: node.book_id,
          link: node.link
        }
      };
      setSearchType('books');
      getItemDetails(mockSearchItem, 'books');
    }
    setTimeout(() => {
      setSelectedNode(node);
    }, 0);
  };

  // Function to dismiss other nodes (keep only the selected node, no relationships)
  const dismissOtherNodes = (selectedNode) => {
    const filteredNodes = networkData.nodes.filter(node => node.id === selectedNode.id);
    
    // Remove all relationships - this creates a new visualization starting from this node
    setNetworkData(sanitizeGraphData({
      nodes: filteredNodes,
      links: [] // No relationships - clean slate
    }));
  };

  // Function to dismiss the selected node
  const dismissNode = (nodeToRemove) => {
    const filteredNodes = networkData.nodes.filter(node => node.id !== nodeToRemove.id);
    const filteredLinks = networkData.links.filter(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source?.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target?.id;
      return sourceId !== nodeToRemove.id && targetId !== nodeToRemove.id;
    });
    
    setNetworkData(sanitizeGraphData({
      nodes: filteredNodes,
      links: filteredLinks
    }));
  };

  const getItemDetails = async (item, itemType = null) => {
    try {
      try { window.__cmg_resetZoom && window.__cmg_resetZoom(); } catch (_) {}
      setLoading(true);
      setSelectedItem(item);
      
      // Use passed itemType or fall back to current searchType
      const typeToUse = itemType || searchType;
      
      if (typeToUse === 'singers') {
        const response = await fetchWithRetry(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: item.name, depth: 2 })
        }, { retries: 2, baseDelay: 600 });

        const text = await response.text();
        let data; try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
      if (response.ok) {
        data = await enrichWithFamily(data, item.name);
        const normalized = normalizeDetailsRelationshipSources(data);
          setItemDetails(normalized);
          setCurrentView('network');
          generateNetworkFromDetails(normalized, item.name, 'singers');
        } else {
          setError(data.error || `Failed (${response.status})`);
        }
      } else if (typeToUse === 'operas') {
        const { type: typedType, value: typedValue } = parseTypedId(item.id);
        const payload = {
          operaName: item.properties.opera_name || item.properties.title
        };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        const response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
        }, { retries: 2, baseDelay: 600 });

        const text = await response.text();
        let data; try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
        if (response.ok) {
          data = await enrichWithFamily(data, item.properties.opera_name || item.properties.title);
          const normalized = normalizeDetailsRelationshipSources(data);
          setItemDetails(normalized);
          setCurrentView('network');
          generateNetworkFromDetails(normalized, item.properties.opera_name || item.properties.title, 'operas');
        } else {
          setError(data.error || `Failed (${response.status})`);
        }
      } else if (typeToUse === 'books') {
        const response = await fetchWithRetry(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: item.properties.title })
        }, { retries: 2, baseDelay: 600 });

        const text = await response.text();
        let data; try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
        if (response.ok) {
          data = await enrichWithFamily(data, item.properties.title);
          const normalized = normalizeDetailsRelationshipSources(data);
          setItemDetails(normalized);
          setCurrentView('network');
          generateNetworkFromDetails(normalized, item.properties.title, 'books');
        } else {
          setError(data.error || `Failed (${response.status})`);
        }
      }
    } catch (err) {
      setError('Failed to fetch details');
    } finally {
      setLoading(false);
    }
  };

  const searchForPerson = async (personName) => {
    clearFiltersForNewSearch([]);
    try {
      setLoading(true);
      setError('');
      
      const response = await fetchWithRetry(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ singerName: personName, depth: 2 })
      }, { retries: 2, baseDelay: 600 });

      const data = await response.json();
      if (response.ok) {
        const withFam = await enrichWithFamily(data, personName);
        const normalized = normalizeDetailsRelationshipSources(withFam);
        setItemDetails(normalized);
        setSelectedItem({ name: personName });
        setSearchType('singers');
        setCurrentView('network');
        generateNetworkFromDetails(normalized, personName, 'singers');
        setShouldRunSimulation(true); // Trigger simulation for person search
        setHasExecutedSearch(true);
      } else {
        setError(data.error || 'Person not found');
      }
    } catch (err) {
      setError('Failed to fetch person details');
    } finally {
      setLoading(false);
    }
  };
  const searchForPersonFromOpera = async (personName, currentOpera) => {
    clearFiltersForNewSearch([]);
    try {
      setLoading(true);
      setError('');
      
      const response = await fetchWithRetry(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ singerName: personName, depth: 2 })
      }, { retries: 2, baseDelay: 600 });

      const data = await response.json();
      if (response.ok) {
        // Add the current opera to the works if not already present
        if (currentOpera && !data.works.operas.find(opera => (opera.opera_name || opera.title) === currentOpera.name)) {
          data.works.operas.push({
            opera_name: currentOpera.name,
            composer: currentOpera.composer
          });
        }
        
        const withFam = await enrichWithFamily(data, personName);
        const normalized = normalizeDetailsRelationshipSources(withFam);
        setItemDetails(normalized);
        setSelectedItem({ name: personName });
        setSearchType('singers');
        setCurrentView('network');
        generateNetworkFromDetails(normalized, personName, 'singers');
        setShouldRunSimulation(true); // Trigger simulation for person search
        setHasExecutedSearch(true);
      } else {
        setError(data.error || 'Person not found');
      }
    } catch (err) {
      setError('Failed to fetch person details');
    } finally {
      setLoading(false);
    }
  };

  // Generate colors based on voice type
  const getNodeColor = (node) => {
    if (node.type === 'opera') return '#9CA3AF';
    if (node.type === 'book') return '#9CA3AF';
    
    // Create color map from VOICE_TYPES array for consistency
    const colorMap = {};
    VOICE_TYPES.forEach(voiceType => {
      colorMap[voiceType.name] = voiceType.color;
    });
    
    // Handle null/undefined voice types
    if (!node.voiceType) {
      return '#8cc400'; // Unknown voice type color
    }
    
    return colorMap[node.voiceType] || '#6B7280'; // Fallback gray for unmapped types
  };

  // Comprehensive node styling based on type and selection state
  const getNodeStyle = (node, selectedNode) => {
    const baseColor = getNodeColor(node);
    const isSelected = selectedNode && selectedNode.id === node.id;
    
    let stroke, strokeWidth;
    
    if (isSelected) {
      stroke = d3.color(baseColor).darker(0.5);
      strokeWidth = 3;
    } else if (node.type === 'opera') {
      stroke = "#FFFFFF"; // White border for operas
      strokeWidth = 3;
    } else if (node.type === 'book') {
      stroke = "#6a7304"; // Book nodes get olive border
      strokeWidth = 3;
    } else {
      stroke = "none"; // No border for persons
      strokeWidth = 0;
    }
    
    return {
      fill: baseColor,
      stroke: stroke,
      strokeWidth: strokeWidth
    };
  };

  // Accessible text color selection per WCAG contrast
  const srgbToLinear = (c) => {
    const cs = c / 255;
    return cs <= 0.03928 ? cs / 12.92 : Math.pow((cs + 0.055) / 1.055, 2.4);
  };
  const relativeLuminance = (hex) => {
    const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(String(hex) || '');
    if (!m) return 1; // default to white luminance
    const r = srgbToLinear(parseInt(m[1], 16));
    const g = srgbToLinear(parseInt(m[2], 16));
    const b = srgbToLinear(parseInt(m[3], 16));
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };
  const contrastRatio = (L1, L2) => {
    const maxL = Math.max(L1, L2);
    const minL = Math.min(L1, L2);
    return (maxL + 0.05) / (minL + 0.05);
  };
  const BLACK = '#111827';
  const WHITE = '#FFFFFF';
  const textColorCache = new Map();
  const getAccessibleTextColor = (bgHex, isLarge = false) => {
    const key = `${bgHex}|${isLarge ? 1 : 0}`;
    if (textColorCache.has(key)) return textColorCache.get(key);
    const Lbg = relativeLuminance(bgHex);
    const Lwhite = 1;
    const Lblack = relativeLuminance(BLACK);
    const threshold = isLarge ? 3 : 4.5;
    const cWhite = contrastRatio(Lwhite, Lbg);
    const cBlack = contrastRatio(Lblack, Lbg);
    let fill = cWhite >= cBlack ? WHITE : BLACK;
    let ratio = Math.max(cWhite, cBlack);
    let needsHalo = ratio < threshold;
    const result = { fill, needsHalo };
    textColorCache.set(key, result);
    return result;
  };
  // Function to handle expansion spacing - called when new nodes are added to network
  ;
  // Network visualization using D3
  const NetworkVisualization = ({ viewport: viewportInfo = {} }) => {
    const viewportIsPhone = !!viewportInfo.isPhone;
    const viewportIsTablet = !!viewportInfo.isTablet;
    const containerRef = useRef(null);
    const isSimulationActiveRef = useRef(false);
    const activeSimulationCountRef = useRef(0);
    const [isSimulationLocked, setIsSimulationLocked] = useState(false);
    const latestNodesRef = useRef([]);
    useEffect(() => {
      latestNodesRef.current = Array.isArray(networkData?.nodes) ? networkData.nodes : [];
    }, [networkData?.nodes]);

    const panToNode = useCallback((nodeId, { duration = 650 } = {}) => {
      const normalizedId = normalizeNodeId(nodeId);
      if (!normalizedId) return;
      const nodes = latestNodesRef.current || [];
      const targetNode = nodes.find(n => normalizeNodeId(n?.id ?? n?.name) === normalizedId);
      if (!targetNode || !Number.isFinite(targetNode.x) || !Number.isFinite(targetNode.y)) return;
      const container = containerRef.current;
      const svgEl = svgRef.current;
      const zoom = zoomRef.current;
      if (!container || !svgEl || !zoom) return;
      const width = container.clientWidth || container.offsetWidth || visualizationHeight || 0;
      const height = container.clientHeight || container.offsetHeight || visualizationHeight || 0;
      if (!width || !height) return;
      const current = uiZoomRef.current || d3.zoomIdentity;
      const scale = current.k || 1;
      const target = d3.zoomIdentity
        .translate(width / 2, height / 2)
        .scale(scale)
        .translate(-(Number(targetNode.x) || 0), -(Number(targetNode.y) || 0));
      const finalize = () => {
        zoomTransformRef.current = target;
        uiZoomRef.current = target;
        try { window.__cmg_zoomTransform = target; } catch (_) {}
      };
      try {
        const svgSelection = d3.select(svgEl);
        svgSelection.interrupt('cmg-center-node');
        const transition = svgSelection
          .transition('cmg-center-node')
          .duration(Math.max(0, duration || 0))
          .ease(d3.easeCubicOut)
          .call(zoom.transform, target);
        transition
          .on('end', finalize)
          .on('interrupt', finalize);
      } catch (_) {
        try {
          zoom.transform(d3.select(svgEl), target);
        } catch (_) {
          try {
            d3.select(svgEl).property('__zoom', target);
            d3.select(svgEl).select('g').attr('transform', target);
          } catch (_) {}
        }
        finalize();
      }
    }, [visualizationHeight]);

    useEffect(() => {
      centerOnNodeRef.current = panToNode;
      return () => {
        if (centerOnNodeRef.current === panToNode) {
          centerOnNodeRef.current = null;
        }
      };
    }, [panToNode]);
    const updateSimulationActive = useCallback((active) => {
      if (active) {
        activeSimulationCountRef.current += 1;
      } else {
        activeSimulationCountRef.current = Math.max(0, activeSimulationCountRef.current - 1);
      }
      const isLocked = activeSimulationCountRef.current > 0;
      isSimulationActiveRef.current = isLocked;
      setIsSimulationLocked(isLocked);
    }, []);
    const zoomRef = useRef(null);
    const zoomTransformRef = useRef(d3.zoomIdentity);
    const zoomLockedRef = useRef(false);
    const baseChargeStrengthRef = useRef(-1000);
    const hasAppliedInitialFitRef = useRef(false);
    const LONG_PRESS_DELAY_MS = 700;
    const LONG_PRESS_MOVE_CANCEL_PX = 8;
    const nodeLongPressState = {
      pointerId: null,
      timerId: null,
      startX: 0,
      startY: 0,
      target: null,
      datum: null,
      fired: false
    };
    const clearNodeLongPress = ({ releasePointer = false } = {}) => {
      if (nodeLongPressState.timerId) {
        clearTimeout(nodeLongPressState.timerId);
      }
      if (releasePointer && nodeLongPressState.target && nodeLongPressState.pointerId !== null) {
        try { nodeLongPressState.target.releasePointerCapture(nodeLongPressState.pointerId); } catch (_) {}
      }
      nodeLongPressState.pointerId = null;
      nodeLongPressState.timerId = null;
      nodeLongPressState.target = null;
      nodeLongPressState.datum = null;
      nodeLongPressState.startX = 0;
      nodeLongPressState.startY = 0;
      nodeLongPressState.fired = false;
    };
    const linkLongPressState = {
      pointerId: null,
      timerId: null,
      startX: 0,
      startY: 0,
      target: null,
      datum: null,
      fired: false
    };
    const markLongPressConsumed = () => {
      longPressClickSuppressRef.current = true;
      setTimeout(() => { longPressClickSuppressRef.current = false; }, 500);
    };
    const clearLinkLongPress = ({ releasePointer = false } = {}) => {
      if (linkLongPressState.timerId) {
        clearTimeout(linkLongPressState.timerId);
      }
      if (releasePointer && linkLongPressState.target && linkLongPressState.pointerId !== null) {
        try { linkLongPressState.target.releasePointerCapture(linkLongPressState.pointerId); } catch (_) {}
      }
      linkLongPressState.pointerId = null;
      linkLongPressState.timerId = null;
      linkLongPressState.target = null;
      linkLongPressState.datum = null;
      linkLongPressState.startX = 0;
      linkLongPressState.startY = 0;
      linkLongPressState.fired = false;
    };
    const scheduleNodeLongPress = (event, datum, target) => {
      if (!target || event.pointerType !== 'touch') return;
      clearNodeLongPress({ releasePointer: true });
      clearLinkLongPress({ releasePointer: true });
      nodeLongPressState.pointerId = event.pointerId;
      nodeLongPressState.startX = Number.isFinite(event.clientX) ? event.clientX : (Number.isFinite(event.pageX) ? event.pageX : 0);
      nodeLongPressState.startY = Number.isFinite(event.clientY) ? event.clientY : (Number.isFinite(event.pageY) ? event.pageY : 0);
      nodeLongPressState.target = target;
      nodeLongPressState.datum = datum;
      nodeLongPressState.fired = false;
        try { target.setPointerCapture(event.pointerId); } catch (_) {}
        nodeLongPressState.timerId = window.setTimeout(() => {
          nodeLongPressState.timerId = null;
          nodeLongPressState.fired = true;
          const syntheticEvent = new MouseEvent('contextmenu', {
            bubbles: true,
            cancelable: true,
            view: window,
            clientX: nodeLongPressState.startX,
            clientY: nodeLongPressState.startY
          });
          try { target.dispatchEvent(syntheticEvent); } catch (_) {}
          markLongPressConsumed();
        }, LONG_PRESS_DELAY_MS);
      };
    const scheduleLinkLongPress = (event, datum, target) => {
      if (!target || event.pointerType !== 'touch') return;
      clearLinkLongPress({ releasePointer: true });
      clearNodeLongPress({ releasePointer: true });
      linkLongPressState.pointerId = event.pointerId;
      linkLongPressState.startX = Number.isFinite(event.clientX) ? event.clientX : (Number.isFinite(event.pageX) ? event.pageX : 0);
      linkLongPressState.startY = Number.isFinite(event.clientY) ? event.clientY : (Number.isFinite(event.pageY) ? event.pageY : 0);
      linkLongPressState.target = target;
      linkLongPressState.datum = datum;
      linkLongPressState.fired = false;
        try { target.setPointerCapture(event.pointerId); } catch (_) {}
        linkLongPressState.timerId = window.setTimeout(() => {
          linkLongPressState.timerId = null;
          linkLongPressState.fired = true;
          const syntheticEvent = new MouseEvent('contextmenu', {
            bubbles: true,
            cancelable: true,
            view: window,
            clientX: linkLongPressState.startX,
            clientY: linkLongPressState.startY
          });
          try { target.dispatchEvent(syntheticEvent); } catch (_) {}
          markLongPressConsumed();
        }, LONG_PRESS_DELAY_MS);
      };
    // Date ranges will be reset manually when needed to avoid setState in useEffect

    useEffect(() => {
      if (!networkData.nodes.length || !containerRef.current) return;

      const container = containerRef.current;
      const width = container.clientWidth;
      const height = visualizationHeight;

      const nodesList = Array.isArray(networkData.nodes) ? networkData.nodes : [];
      const nodeIdSet = new Set(nodesList.map(node => normalizeNodeId(node?.id ?? node?.name)).filter(Boolean));
      const hasPlaceholderNodes = nodesList.some(node => {
        const id = normalizeNodeId(node?.id ?? node?.name);
        return isPlaceholderName(id);
      });
      const hasInvalidLinks = (networkData.links || []).some(link => {
        const sourceId = resolveLinkEndpointId(link?.source);
        const targetId = resolveLinkEndpointId(link?.target);
        if (!sourceId || !targetId) return true;
        if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return true;
        return !nodeIdSet.has(sourceId) || !nodeIdSet.has(targetId);
      });
      if (hasPlaceholderNodes || hasInvalidLinks) {
        try {
          console.warn('[graph] Skipping render due to invalid nodes/links', { hasPlaceholderNodes, hasInvalidLinks });
        } catch (_) {}
        return;
      }

      // Restore zoom transform from a global cache across remounts
      try {
        if (window.__cmg_zoomTransform) {
          zoomTransformRef.current = window.__cmg_zoomTransform;
        }
      } catch (_) {}

      // Clear previous visualization
      d3.select(svgRef.current).selectAll("*").remove();

      const svg = d3.select(svgRef.current)
        .attr("width", width)
        .attr("height", height)
        .style("background", "transparent")
        .style("user-select", "none")
        .style("-webkit-user-select", "none")
        .style("touch-action", "none");
      // Prevent default browser context menu on background to avoid accidental pan/zoom
      svg.on('contextmenu', (event) => {
        event.preventDefault();
        if (event.stopImmediatePropagation) event.stopImmediatePropagation();
        event.stopPropagation();
        // Close any open menus on background right-click
        try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
        try { setLinkContextMenu(createLinkContextMenuState()); } catch (_) {}
        // Reassert current transform to ensure no movement occurs on right-click
        try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
      });
      // Block non-primary button presses from initiating any zoom/drag gesture
      svg.on('mousedown', (event) => {
        if (event.button !== 0) {
          event.preventDefault();
          if (event.stopImmediatePropagation) event.stopImmediatePropagation();
          event.stopPropagation();
          try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
        }
      });
      svg.on('pointerdown.cmg', (event) => {
        if (event.buttons && event.buttons !== 1) {
          event.preventDefault();
          if (event.stopImmediatePropagation) event.stopImmediatePropagation();
          event.stopPropagation();
          try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
        }
      });
      svg.on('mouseup', (event) => {
        if (event.button !== 0) {
          event.preventDefault();
          if (event.stopImmediatePropagation) event.stopImmediatePropagation();
          event.stopPropagation();
          try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
        }
      });
      svg.on('pointerup', (event) => {
        if (event.button && event.button !== 0) {
          event.preventDefault();
          if (event.stopImmediatePropagation) event.stopImmediatePropagation();
          event.stopPropagation();
          try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
        }
      });
      svg.on('auxclick', (event) => {
        event.preventDefault();
        if (event.stopImmediatePropagation) event.stopImmediatePropagation();
        event.stopPropagation();
      });

      // Background left-click: anchor all nodes to prevent any drift
      const anchorAllNodes = (event) => {
        if (viewportIsPhone) return;
        if (event.button !== 0) return;
        // Ignore clicks on nodes/links/labels
        const target = event.target;
        if (target.closest && (target.closest('circle') || target.closest('path') || target.closest('text') || target.closest('rect'))) return;
        try {
          networkData.nodes.forEach(n => { n.fx = n.x; n.fy = n.y; n.vx = 0; n.vy = 0; });
          if (simulationRef.current) { try { simulationRef.current.stop(); } catch (_) {} }
          try { setShouldRunSimulation(false); } catch (_) {}
        } catch (_) {}
        try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
        try { setLinkContextMenu(createLinkContextMenuState()); } catch (_) {}
        setExpandSubmenu(null);
      };
      svg.on('click', anchorAllNodes);

      // Create main group for zooming/panning
      const g = svg.append("g");
      let pendingZoomFrame = null;
      let queuedZoomTransform = null;

      const applyGroupTransform = (transform, { immediate = false } = {}) => {
        queuedZoomTransform = transform;
        if (immediate) {
          if (pendingZoomFrame) {
            cancelAnimationFrame(pendingZoomFrame);
            pendingZoomFrame = null;
          }
          g.attr('transform', queuedZoomTransform);
          return;
        }
        if (pendingZoomFrame) return;
        pendingZoomFrame = requestAnimationFrame(() => {
          pendingZoomFrame = null;
          if (queuedZoomTransform) {
            g.attr('transform', queuedZoomTransform);
          }
        });
      };
      // Also guard the group for safety (in case events bind to inner elements)
      g.on('contextmenu', (event) => {
        event.preventDefault();
        if (event.stopImmediatePropagation) event.stopImmediatePropagation();
        event.stopPropagation();
        // Close any open menus on right-click within main group
        try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
        try { setLinkContextMenu(createLinkContextMenuState()); } catch (_) {}
        try { applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}
      });

      // Always restore previous zoom/pan silently (no zoom event)
      try {
        const prev = uiZoomRef.current || d3.zoomIdentity;
        d3.select(svgRef.current).property('__zoom', prev);
        applyGroupTransform(prev, { immediate: true });
      } catch (_) {}

      // Helper to apply a zoom transform silently (no zoom event)
      const applyZoomTransformSilently = (() => {
        let rafId = null;
        let pending = null;
        const nearlyEqual = (a, b) => Math.abs(a - b) < 1e-6;
        const sameTransform = (a, b) => !!a && !!b && nearlyEqual(a.k, b.k) && nearlyEqual(a.x, b.x) && nearlyEqual(a.y, b.y);
        return (t) => {
          try {
            const svgSel = d3.select(svgRef.current);
            const current = svgSel.property('__zoom') || d3.zoomIdentity;
            // Skip redundant writes
            if (sameTransform(current, t)) {
              zoomTransformRef.current = t;
              uiZoomRef.current = t;
              try { window.__cmg_zoomTransform = t; } catch (_) {}
              return;
            }
            pending = t;
            if (rafId != null) return;
            rafId = requestAnimationFrame(() => {
              try {
                svgSel.property('__zoom', pending);
                applyGroupTransform(pending, { immediate: true });
                zoomTransformRef.current = pending;
                uiZoomRef.current = pending;
                try { window.__cmg_zoomTransform = pending; } catch (_) {}
              } finally {
                rafId = null;
                pending = null;
              }
            });
          } catch (_) {}
        };
      })();
      // Expose helpers globally so other flows can manage zoom predictably
      try {
        window.__cmg_reapplyZoom = () => applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity);
        window.__cmg_resetZoom = () => {
          const id = d3.zoomIdentity;
          uiZoomRef.current = id;
          zoomTransformRef.current = id;
          applyZoomTransformSilently(id);
          hasAppliedInitialFitRef.current = false; // allow next render to recenter
        };
      } catch (_) {}

      const centerGraphWithinViewport = ({ padding = 80 } = {}) => {
        const transform = computeCenteredTransform(
          networkData.nodes,
          width,
          height,
          padding
        );
        if (!transform) return;
        applyZoomTransformSilently(transform);
        hasAppliedInitialFitRef.current = true;
      };
      try { window.__cmg_centerGraph = centerGraphWithinViewport; } catch (_) {}

      // Create zoom behavior
      const zoom = d3.zoom()
        .filter((event) => {
          if (isSimulationActiveRef.current) return false;
          // Allow wheel zoom always; block double-click zoom entirely
          if (event.type === 'wheel') return true;
          if (event.type === 'dblclick') return false;
          const isTouchPointer = typeof event.pointerType === 'string' && event.pointerType === 'touch';
          if (isTouchPointer) return false;
          if (typeof event.type === 'string' && event.type.startsWith('touch')) return false;
          // Explicitly block context menu/right-click and middle-click from initiating zoom/pan
          if (event.button === 2 || event.buttons === 2) return false;
          if (event.button === 1 || event.buttons === 4) return false;
          // Only allow primary button drag without Ctrl/Cmd/Meta
          const isPrimary = (event.buttons === 1) || (event.button === 0);
          return isPrimary && !event.ctrlKey && !event.metaKey;
        })
        .clickDistance(0)
        .translateExtent([[-1e6, -1e6], [1e6, 1e6]])
        .scaleExtent([0.1, 4])
        .on("zoom", (event) => {
          if (isSimulationActiveRef.current) {
            const target = uiZoomRef.current || d3.zoomIdentity;
            applyZoomTransformSilently(target);
            return;
          }
          // Hard block any zoom while menus are open or during menu open/close
          if ((zoomLockedRef.current && event.sourceEvent) || contextMenu.show || linkContextMenu.show) {
            const target = uiZoomRef.current || d3.zoomIdentity;
            const current = d3.select(svgRef.current).property('__zoom') || d3.zoomIdentity;
            if (!(Math.abs(target.k - current.k) < 1e-6 && Math.abs(target.x - current.x) < 1e-6 && Math.abs(target.y - current.y) < 1e-6)) {
              applyZoomTransformSilently(target);
            }
            return;
          }
          // Only honor primary-button drag or wheel changes; ignore any other source
          const e = event.sourceEvent;
          if (!e) {
            g.attr("transform", event.transform);
            zoomTransformRef.current = event.transform;
            uiZoomRef.current = event.transform;
            hasAppliedInitialFitRef.current = true;
            return;
          }
          const isWheel = e.type === 'wheel';
          const isPointerMove = e && (e.type === 'pointermove' || e.type === 'mousemove');
          const isTouchEvent = !!(e && (
            (typeof e.pointerType === 'string' && e.pointerType === 'touch') ||
            (typeof e.type === 'string' && e.type.startsWith('touch'))
          ));
          const buttons = typeof e.buttons === 'number' ? e.buttons : 0;
          const button = typeof e.button === 'number' ? e.button : -1;
          const isPrimaryDrag = isPointerMove && (buttons === 1 || button === 0 || isTouchEvent);
          // Ignore if the originating pointer is right or middle button
          const isDisallowedButton = !isTouchEvent && ((buttons === 2) || (button === 2) || (buttons === 4) || (button === 1));
          if (e && isDisallowedButton) {
            const target = uiZoomRef.current || d3.zoomIdentity;
            const current = d3.select(svgRef.current).property('__zoom') || d3.zoomIdentity;
            if (!(Math.abs(target.k - current.k) < 1e-6 && Math.abs(target.x - current.x) < 1e-6 && Math.abs(target.y - current.y) < 1e-6)) {
              applyZoomTransformSilently(target);
            }
            return;
          }
          if (!isWheel && !isPrimaryDrag) {
            // Reassert previous transform to avoid unintended resets (e.g., right-click)
            applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity);
            return;
          }
          applyGroupTransform(event.transform);
          zoomTransformRef.current = event.transform;
          uiZoomRef.current = event.transform;
          hasAppliedInitialFitRef.current = true;
        });

      svg.call(zoom);
      // Reassert current zoom once after zoom is attached (skip if identical)
      try {
        const prev = uiZoomRef.current || d3.zoomIdentity;
        const current = d3.select(svgRef.current).property('__zoom') || d3.zoomIdentity;
        if (!(Math.abs(prev.k - current.k) < 1e-6 && Math.abs(prev.x - current.x) < 1e-6 && Math.abs(prev.y - current.y) < 1e-6)) {
          applyGroupTransform(prev, { immediate: true });
          d3.select(svgRef.current).property('__zoom', prev);
        }
      } catch (_) {}
      zoomRef.current = zoom;

      let touchCleanup = null;
      if (viewportIsPhone || viewportIsTablet) {
        touchCleanup = initTouchInteractions({
          svgElement: svgRef.current,
          svgSelection: svg,
          zoomBehavior: zoom,
          zoomTransformRef,
          uiZoomRef,
          closeOpenMenus: () => {
            try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
            try { setLinkContextMenu(createLinkContextMenuState()); } catch (_) {}
          }
        });
      }


      // Hoisted helper to wrap label text inside circles
      function wrapText(textElement, text, maxWidth, fontSize) {
        if (!text || typeof text !== 'string') {
          return ['Unknown'];
        }
        const words = text.split(/(\s+|-)/);
        const lines = [];
        let currentLine = '';
        const charWidth = fontSize * 0.55;
        const maxCharsPerLine = Math.floor(maxWidth / charWidth);
        for (let word of words) {
          if (word === '') continue;
          const testLine = currentLine + word;
          if (testLine.length <= maxCharsPerLine || currentLine === '') {
            currentLine = testLine;
          } else {
            if (currentLine) {
              lines.push(currentLine);
              currentLine = word;
            } else {
              lines.push(word.substring(0, maxCharsPerLine - 1) + '-');
              currentLine = word.substring(maxCharsPerLine - 1);
            }
          }
        }
        if (currentLine) {
          lines.push(currentLine);
        }
        const finalLines = lines.slice(0, 3);
        if (lines.length > 3) {
          finalLines[2] = finalLines[2].substring(0, finalLines[2].length - 3) + '...';
        }
        return finalLines;
      }
      if (false) {
        // Helpers to resolve link labels from current network
        const normalizeId = v => (typeof v === 'string' ? v : (v?.id || v?.name || v));
        const getLinkLabel = (srcId, trgId) => {
          const match = networkData.links.find(l => normalizeId(l.source) === srcId && normalizeId(l.target) === trgId);
          if (!match) return '';
          if (match.label) return match.label;
          switch (match.type) {
            case 'taught': return 'taught';
            case 'premiered': return 'premiered role in';
            case 'composed': return 'wrote';
            case 'wrote': return 'wrote';
            case 'authored': return 'authored';
            case 'family': return match.label || 'family';
            default: return '';
          }
        };
        // Define arrowhead marker
        const defs = svg.append('defs');
        defs.append('marker')
          .attr('id', 'arrowGrey')
          .attr('markerWidth', 10)
          .attr('markerHeight', 10)
          .attr('refX', 8)
          .attr('refY', 3)
          .attr('orient', 'auto')
          .append('path')
          .attr('d', 'M0,0 L0,6 L9,3 z')
          .attr('fill', '#FFFFFF');
        // Directional hierarchy: teachers above (incoming TAUGHT), students and works below (outgoing)
        const nodeById = Object.fromEntries(networkData.nodes.map(n => [n.id, n]));
        const personNodes = networkData.nodes.filter(n => n.type === 'person');
        const rootId = currentCenterNode && networkData.nodes.find(n => n.id === currentCenterNode)
          ? currentCenterNode
          : (personNodes[0]?.id || null);
        if (!rootId) return;

        const rootNode = nodeById[rootId];
        // Opera-centered hierarchical view: people above, opera below
        if (rootNode && rootNode.type === 'opera') {
          const normalizeId = v => (typeof v === 'string' ? v : (v?.id || v?.name || v));
          const relatedPersons = Array.from(new Set(
            networkData.links
              .filter(l => (l.type === 'premiered' || l.type === 'composed' || l.type === 'wrote') && normalizeId(l.target) === rootId)
              .map(l => normalizeId(l.source))
              .filter(id => !!id && nodeById[id] && nodeById[id].type === 'person')
          ));

          const upData = { id: rootId, children: relatedPersons.map(pid => ({ id: pid, children: [] })) };
          const upRoot = d3.hierarchy(upData, d => d.children && d.children.length ? d.children : null);
          const upLayout = d3.tree().nodeSize([160, 100]);
          upLayout(upRoot);

          // Links: arrowheads toward opera
          g.selectAll('path.h-up')
            .data(upRoot.links())
            .enter()
            .append('path')
            .attr('class', 'h-up')
            .attr('fill', 'none')
            .attr('stroke', '#FFFFFF')
            .attr('stroke-width', 1.5)
            .attr('marker-end', 'url(#arrowGrey)')
            .attr('d', d => {
              const sx = d.target.x, sy = -d.target.y; // person
              const tx = d.source.x, ty = -d.source.y; // opera (root)
              const dx = tx - sx, dy = ty - sy; const dist = Math.hypot(dx, dy) || 1; const nodeRadius = 40;
              const ex = tx - (dx / dist) * (nodeRadius + 8); const ey = ty - (dy / dist) * (nodeRadius + 8);
              const my = (sy + ey) / 2; return `M${sx},${sy}C${sx},${my} ${ex},${my} ${ex},${ey}`;
            });

          const upNode = g.selectAll('g.h-up-node')
            .data(upRoot.descendants())
            .enter()
            .append('g')
            .attr('class', 'h-up-node')
            .attr('transform', d => `translate(${d.x},${-d.y})`);

          upNode.append('circle')
            .attr('r', 40)
            .attr('fill', d => {
              const node = nodeById[d.data.id] || { type: 'opera' };
              return getNodeStyle(node, selectedNode).fill;
            })
            .attr('stroke', d => {
              const node = nodeById[d.data.id] || { type: 'opera' };
              return getNodeStyle(node, selectedNode).stroke;
            })
            .attr('stroke-width', 2)
            .style('cursor', 'pointer')
            .attr('opacity', d => {
              const node = nodeById[d.data.id];
              return node ? (isNodeVisible(node) ? 1 : 0.2) : 1;
            })
            .on('contextmenu', (event, d) => {
              event.preventDefault(); event.stopPropagation();
              try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
              try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
              const nodeData = nodeById[d.data.id]; if (!nodeData) return;
              const menuOffset = 20; const nodeRadius = 40; const containerRect = container.getBoundingClientRect();
              const ctm = g.node().getScreenCTM(); const pt = svgRef.current.createSVGPoint(); pt.x = d.x; pt.y = -d.y;
              const sp = pt.matrixTransform(ctm); const nodeCX = sp.x - containerRect.left; const nodeCY = sp.y - containerRect.top;
              let finalX = nodeCX + nodeRadius + menuOffset; let finalY = nodeCY - nodeRadius;
              const menuWidth = 250, menuHeight = 300, containerWidth = containerRect.width, containerHeight = containerRect.height;
              if (finalX + menuWidth > containerWidth) finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
              if (finalY + menuHeight > containerHeight) finalY = containerHeight - menuHeight;
              if (finalY < 0) finalY = 0; if (finalX < 0) finalX = 0;
              setTimeout(() => { setContextMenu({ show: true, x: finalX, y: finalY, node: nodeData }); setExpandSubmenu(null); }, 0);
            })
            .on('click', (event, d) => {
              event.stopPropagation();
              // Close any open menus when clicking a node
              try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
              try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
              // Also clear any open Full information card
              try { setProfileCard({ show: false, data: null }); } catch(_) {}
              const node = nodeById[d.data.id]; if (!node) return;
              if (node.type === 'person') { setSearchType('singers'); getItemDetails({ name: node.id }, 'singers'); }
              if (node.type === 'opera') { setSearchType('operas'); getItemDetails({ properties: { opera_name: node.name, title: node.name } }, 'operas'); }
            });

          upNode.each(function(d){
            const group = d3.select(this); const fontSize = 11; const radius = 40; const maxWidth = radius * 1.6;
            const label = nodeById[d.data.id]?.name || d.data.id;
            const lines = wrapText(this, label, maxWidth, fontSize); const lineHeight = fontSize * 1.2;
            if (lines.length === 1) {
              const node = nodeById[d.data.id] || { type: 'opera' };
              const bg = getNodeStyle(node, selectedNode).fill;
              const tc = getAccessibleTextColor(bg, (fontSize >= 18));
              group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight', '600').attr('fill', tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('dy','0.35em').style('pointer-events','none').text(lines[0]);
            } else {
              const totalHeight = (lines.length - 1) * lineHeight; const startOffset = -(totalHeight / 2);
              lines.forEach((line,i)=>{
                const node = nodeById[d.data.id] || { type: 'opera' };
                const bg = getNodeStyle(node, selectedNode).fill;
                const tc = getAccessibleTextColor(bg, (fontSize >= 18));
                group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight','600').attr('fill',tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('y', startOffset + (i * lineHeight)).attr('dy','0.35em').style('pointer-events','none').text(line);
              });
            }
          });

          // Render per-person downward subtrees (students/works) so expansions are visible without re-rooting
          const workTypes = new Set(['premiered', 'wrote', 'composed', 'authored']);
          const outgoingStudents = new Map();
          const worksByPerson = new Map();
          networkData.nodes.filter(n => n.type === 'person').forEach(n => { outgoingStudents.set(n.id, []); worksByPerson.set(n.id, []); });
          networkData.links.forEach(l => {
            const src = normalizeId(l.source); const trg = normalizeId(l.target);
            if (l.type === 'taught' && outgoingStudents.has(src)) outgoingStudents.get(src).push(trg);
            if (workTypes.has(l.type) && worksByPerson.has(src)) worksByPerson.get(src).push(trg);
          });

          const buildDownLocal = (id, visited = new Set(), depth = 3) => {
            if (visited.has(id) || depth <= 0) return [];
            visited.add(id);
            const studentIds = Array.from(new Set((outgoingStudents.get(id) || []).filter(s => s !== id)));
            const workIds = Array.from(new Set(worksByPerson.get(id) || []));
            const children = [];
            studentIds.forEach(s => children.push({ id: s, children: buildDownLocal(s, visited, depth - 1) }));
            workIds.forEach(w => children.push({ id: w, children: [] }));
            return children;
          };

          upRoot.descendants().forEach(personNode => {
            if (personNode.data.id === rootId) return; // skip opera root
            const pid = personNode.data.id;
            const downData = { id: pid, children: buildDownLocal(pid) };
            const downLocalRoot = d3.hierarchy(downData, d => d.children && d.children.length ? d.children : null);
            const downLocalLayout = d3.tree().nodeSize([120, 80]);
            downLocalLayout(downLocalRoot);

            // Draw links for this person's subtree (positive y from person)
            g.selectAll(`path.h-down-local-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .data(downLocalRoot.links().filter(l => l.source.data.id !== l.target.data.id))
              .enter()
              .append('path')
              .attr('class', `h-down-local-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .attr('fill', 'none')
              .attr('stroke', '#FFFFFF')
              .attr('stroke-width', 1.5)
              .attr('marker-end', 'url(#arrowGrey)')
              .attr('d', d => {
                // anchor at person's coordinates (px, py)
                const px = personNode.x, py = -personNode.y; // person is above (negative y)
                const sx = px + (d.source.x - downLocalRoot.x);
                const sy = py + (d.source.y - downLocalRoot.y) * 0.8; // compress depth
                const tx = px + (d.target.x - downLocalRoot.x);
                const ty = py + (d.target.y - downLocalRoot.y) * 0.8;
                const dx = tx - sx, dy = ty - sy; const dist = Math.hypot(dx, dy) || 1; const nodeRadius = 40;
                const ex = tx - (dx / dist) * (nodeRadius + 8); const ey = ty - (dy / dist) * (nodeRadius + 8);
                const my = (sy + ey) / 2; return `M${sx},${sy}C${sx},${my} ${ex},${my} ${ex},${ey}`;
              });

            // Link labels for subtree
            g.selectAll(`text.h-down-local-label-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .data(downLocalRoot.links())
              .enter()
              .append('text')
              .attr('class', `h-down-local-label-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif")
              .attr('font-size', 10)
              .attr('fill', '#FFFFFF')
              .attr('text-anchor', 'middle')
              .style('pointer-events','none')
              .attr('x', d => {
                const px = personNode.x; return px + (d.target.x + d.source.x - 2 * downLocalRoot.x) / 2;
              })
              .attr('y', d => {
                const py = -personNode.y; return py + ((d.target.y + d.source.y - 2 * downLocalRoot.y) / 2) * 0.8 - 6;
              })
              .text(d => getLinkLabel(d.source.data.id, d.target.data.id));

            // Draw nodes for this subtree
            const localNodes = g.selectAll(`g.h-down-local-node-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .data(downLocalRoot.descendants().filter(n => n.data.id !== pid))
              .enter()
              .append('g')
              .attr('class', `h-down-local-node-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .attr('transform', d => {
                const px = personNode.x, py = -personNode.y;
                const nx = px + (d.x - downLocalRoot.x);
                const ny = py + (d.y - downLocalRoot.y) * 0.8;
                return `translate(${nx},${ny})`;
              });

            localNodes.append('circle')
              .attr('r', 40)
              .attr('fill', d => {
                const node = nodeById[d.data.id] || { type: 'opera' };
                return getNodeStyle(node, selectedNode).fill;
              })
              .attr('stroke', d => {
                const node = nodeById[d.data.id] || { type: 'opera' };
                return getNodeStyle(node, selectedNode).stroke;
              })
              .attr('stroke-width', 2)
              .attr('opacity', d => {
                const node = nodeById[d.data.id]; return node ? (isNodeVisible(node) ? 1 : 0.2) : 1;
              })
              .style('cursor', 'pointer')
              .on('contextmenu', (event, d) => {
                event.preventDefault(); event.stopPropagation();
                try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
                try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
                const nd = nodeById[d.data.id]; if (!nd) return;
                const menuOffset = 20; const nodeRadius = 40; const containerRect = container.getBoundingClientRect();
                const ctm = g.node().getScreenCTM(); const pt = svgRef.current.createSVGPoint(); pt.x = d.x; pt.y = d.y;
                const sp = pt.matrixTransform(ctm); const nodeCX = sp.x - containerRect.left; const nodeCY = sp.y - containerRect.top;
                let finalX = nodeCX + nodeRadius + menuOffset; let finalY = nodeCY - nodeRadius;
                const menuWidth = 250, menuHeight = 300, containerWidth = containerRect.width, containerHeight = containerRect.height;
                if (finalX + menuWidth > containerWidth) finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
                if (finalY + menuHeight > containerHeight) finalY = containerHeight - menuHeight;
                if (finalY < 0) finalY = 0; if (finalX < 0) finalX = 0;
                setTimeout(() => { setContextMenu({ show: true, x: finalX, y: finalY, node: nd }); setExpandSubmenu(null); }, 0);
              })
              .on('click', (event, d) => {
                event.stopPropagation();
                // Close any open menus when clicking a node
                try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
                try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
                // Also clear any open Full information card
                try { setProfileCard({ show: false, data: null }); } catch(_) {}
                const nd = nodeById[d.data.id]; if (!nd) return;
                if (nd.type === 'person') { setSearchType('singers'); getItemDetails({ name: nd.id }, 'singers'); }
                else if (nd.type === 'opera') { setSearchType('operas'); getItemDetails({ properties: { opera_name: nd.name, title: nd.name } }, 'operas'); }
                else if (nd.type === 'book') { setSearchType('books'); getItemDetails({ properties: { title: nd.name } }, 'books'); }
              });

            localNodes.each(function(d){
              const group = d3.select(this); const fontSize = 11; const radius = 40; const maxWidth = radius * 1.6;
              const label = nodeById[d.data.id]?.name || d.data.id;
              const lines = wrapText(this, label, maxWidth, fontSize); const lineHeight = fontSize * 1.2;
              if (lines.length === 1) {
                const node = nodeById[d.data.id] || { type: 'opera' };
                const bg = getNodeStyle(node, selectedNode).fill;
                const tc = getAccessibleTextColor(bg, (fontSize >= 18));
              group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight', '600').attr('fill', tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('dy','0.35em').style('pointer-events','none').text(lines[0]);
              } else {
                const totalHeight = (lines.length - 1) * lineHeight; const startOffset = -(totalHeight / 2);
                lines.forEach((line,i)=>{
                  const node = nodeById[d.data.id] || { type: 'opera' };
                  const bg = getNodeStyle(node, selectedNode).fill;
                  const tc = getAccessibleTextColor(bg, (fontSize >= 18));
                group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight','600').attr('fill',tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('y', startOffset + (i * lineHeight)).attr('dy','0.35em').style('pointer-events','none').text(line);
                });
              }
            });
          });

          // Intentionally skip any automatic fit to avoid unintended recentering

          return;
        }
        // reuse normalizeId above
        const taughtLinks = networkData.links.filter(l => l.type === 'taught').map(l => ({
          source: normalizeId(l.source),
          target: normalizeId(l.target)
        }));
        // incoming teachers map (teacher -> student is a taught link, so teacher is source, student is target)
        const incomingTeachers = new Map();
        const outgoingStudents = new Map();
        personNodes.forEach(n => { incomingTeachers.set(n.id, []); outgoingStudents.set(n.id, []); });
        taughtLinks.forEach(l => {
          if (incomingTeachers.has(l.target)) incomingTeachers.get(l.target).push(l.source);
          if (outgoingStudents.has(l.source)) outgoingStudents.get(l.source).push(l.target);
        });

        // Works down (premiered/composed/authored)
        const workTypes = new Set(['premiered', 'wrote', 'composed', 'authored']);
        const worksByPerson = new Map();
        personNodes.forEach(n => worksByPerson.set(n.id, []));
        networkData.links.forEach(l => {
          const t = l.type;
          if (!workTypes.has(t)) return;
          const src = normalizeId(l.source);
          const trg = normalizeId(l.target);
          if (worksByPerson.has(src) && nodeById[trg]) worksByPerson.get(src).push(trg);
        });
        const buildUp = (id, visited = new Set()) => {
          if (visited.has(id)) return [];
          visited.add(id);
          const parents = Array.from(new Set((incomingTeachers.get(id) || []).filter(p => p !== id)));
          return parents.map(p => ({ id: p, children: buildUp(p, visited) }));
        };

        const buildDown = (id, visitedPeople = new Set()) => {
          if (visitedPeople.has(id)) return [];
          visitedPeople.add(id);
          const studentIds = Array.from(new Set((outgoingStudents.get(id) || []).filter(s => s !== id)));
          const workIds = Array.from(new Set(worksByPerson.get(id) || []));
          const children = [];
          studentIds.forEach(s => children.push({ id: s, children: buildDown(s, visitedPeople) }));
          workIds.forEach(w => children.push({ id: w, children: [] }));
          return children;
        };

        const upData = { id: rootId, children: buildUp(rootId) };
        const downData = { id: rootId, children: buildDown(rootId) };

        const upRoot = d3.hierarchy(upData, d => d.children && d.children.length ? d.children : null);
        const downRoot = d3.hierarchy(downData, d => d.children && d.children.length ? d.children : null);
        const upLayout = d3.tree().nodeSize([160, 100]); // wider x, shallow y up
        const downLayout = d3.tree().nodeSize([160, 100]); // match spacing
        upLayout(upRoot);
        downLayout(downRoot);

        // Draw upward teachers (negative y)
        const upTeachLinks = g.selectAll('path.h-up')
          .data(upRoot.links())
          .enter()
          .append('path')
          .attr('class', 'h-up')
          .attr('fill', 'none')
          .attr('stroke', '#FFFFFF')
          .attr('stroke-width', 1.5)
          .attr('marker-end', 'url(#arrowGrey)')
          .attr('d', d => {
            // Arrow from teacher (parent = d.target) to student (child = d.source)
            const sx = d.target.x, sy = -d.target.y; // teacher
            const tx = d.source.x, ty = -d.source.y; // student
            const dx = tx - sx, dy = ty - sy; const dist = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40; const ex = tx - (dx / dist) * (nodeRadius + 8); const ey = ty - (dy / dist) * (nodeRadius + 8);
            const my = (sy + ey) / 2;
            return `M${sx},${sy}C${sx},${my} ${ex},${my} ${ex},${ey}`;
          });

        // Labels for teacher->student
        g.selectAll('text.h-up-label')
          .data(upRoot.links())
          .enter()
          .append('text')
          .attr('class','h-up-label')
          .attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif")
          .attr('font-size', 10)
          .attr('fill', '#FFFFFF')
          .attr('text-anchor', 'middle')
          .style('pointer-events','none')
          .attr('x', d => (d.target.x + d.source.x) / 2)
          .attr('y', d => (-(d.target.y) + (-(d.source.y))) / 2 - 6)
          .text('taught');

        const upNode = g.selectAll('g.h-up-node')
          .data(upRoot.descendants())
          .enter()
          .append('g')
          .attr('class', 'h-up-node')
          .attr('transform', d => `translate(${d.x},${-d.y})`);

        upNode.append('circle')
          .attr('r', 40)
          .attr('fill', d => {
            const node = nodeById[d.data.id];
            return getNodeStyle(node || { type: 'person' }, selectedNode).fill;
          })
          .attr('stroke', d => {
            const node = nodeById[d.data.id];
            return getNodeStyle(node || { type: 'person' }, selectedNode).stroke;
          })
          .attr('stroke-width', 2)
          .attr('opacity', d => {
            const node = nodeById[d.data.id];
            return node ? (isNodeVisible(node) ? 1 : 0.2) : 1;
          })
          .style('cursor', 'pointer')
          .on('contextmenu', (event, d) => {
            event.preventDefault(); event.stopPropagation();
            try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
            try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
            const nodeData = nodeById[d.data.id]; if (!nodeData) return;
            const menuOffset = 20; const nodeRadius = 40;
            const containerRect = container.getBoundingClientRect();
            // Convert local (d.x, -d.y) to screen coordinates accounting for g transform
            const ctm = g.node().getScreenCTM();
            const pt = svgRef.current.createSVGPoint();
            pt.x = d.x; pt.y = -d.y;
            const sp = pt.matrixTransform(ctm);
            const nodeCX = sp.x - containerRect.left;
            const nodeCY = sp.y - containerRect.top;
            let finalX = nodeCX + nodeRadius + menuOffset;
            let finalY = nodeCY - nodeRadius;
            const menuWidth = 250, menuHeight = 300, containerWidth = containerRect.width, containerHeight = containerRect.height;
            if (finalX + menuWidth > containerWidth) finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
            if (finalY + menuHeight > containerHeight) finalY = containerHeight - menuHeight;
            if (finalY < 0) finalY = 0; if (finalX < 0) finalX = 0;
            setTimeout(() => { setContextMenu({ show: true, x: finalX, y: finalY, node: nodeData }); setExpandSubmenu(null); }, 0);
          })
          .on('click', (event, d) => {
            event.stopPropagation();
            const node = nodeById[d.data.id];
            if (node && node.type === 'person') {
              setSearchType('singers');
              getItemDetails({ name: node.id }, 'singers');
            }
          });

        // Wrapped label inside circle
        upNode.each(function(d){
          const group = d3.select(this); const fontSize = 11; const radius = 40; const maxWidth = radius * 1.6;
          const lines = wrapText(this, d.data.id, maxWidth, fontSize); const lineHeight = fontSize * 1.2;
          if (lines.length === 1) {
            const node = nodeById[d.data.id] || { type: 'opera' };
            const bg = getNodeStyle(node, selectedNode).fill;
            const tc = getAccessibleTextColor(bg, (fontSize >= 18));
            group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight', '600').attr('fill', tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('dy','0.35em').style('pointer-events','none').text(lines[0]);
          } else {
            const totalHeight = (lines.length - 1) * lineHeight; const startOffset = -(totalHeight / 2);
            lines.forEach((line,i)=>{
              const node = nodeById[d.data.id] || { type: 'opera' };
              const bg = getNodeStyle(node, selectedNode).fill;
              const tc = getAccessibleTextColor(bg, (fontSize >= 18));
              group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight','600').attr('fill',tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('y', startOffset + (i * lineHeight)).attr('dy','0.35em').style('pointer-events','none').text(line);
            });
          }
        });

        // Draw downward students and works (positive y)
        const downLinks = g.selectAll('path.h-down')
          .data(downRoot.links())
          .enter()
          .append('path')
          .attr('class', 'h-down')
          .attr('fill', 'none')
          .attr('stroke', '#9CA3AF')
          .attr('stroke-width', 1.5)
          .attr('marker-end', 'url(#arrowGrey)')
          .attr('d', d => {
            const sx = d.source.x, sy = d.source.y; const tx = d.target.x, ty = d.target.y;
            const dx = tx - sx, dy = ty - sy; const dist = Math.hypot(dx, dy) || 1; const nodeRadius = 40;
            const ex = tx - (dx / dist) * (nodeRadius + 8); const ey = ty - (dy / dist) * (nodeRadius + 8);
            const my = (sy + ey) / 2; return `M${sx},${sy}C${sx},${my} ${ex},${my} ${ex},${ey}`;
          });

        // Labels for down links
        g.selectAll('text.h-down-label')
          .data(downRoot.links())
          .enter()
          .append('text')
          .attr('class','h-down-label')
          .attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif")
          .attr('font-size', 10)
          .attr('fill', '#666')
          .attr('text-anchor', 'middle')
          .style('pointer-events','none')
          .attr('x', d => (d.target.x + d.source.x) / 2)
          .attr('y', d => ((d.target.y) + (d.source.y)) / 2 - 6)
          .text(d => getLinkLabel(d.source.data.id, d.target.data.id));

        const downNode = g.selectAll('g.h-down-node')
          .data(downRoot.descendants())
          .enter()
          .append('g')
          .attr('class', 'h-down-node')
          .attr('transform', d => `translate(${d.x},${d.y})`);

        downNode.append('circle')
          .attr('r', 40)
          .attr('fill', d => {
            const node = nodeById[d.data.id];
            const base = node || { type: 'opera' };
            return getNodeStyle(base, selectedNode).fill;
          })
          .attr('stroke', d => {
            const node = nodeById[d.data.id];
            const base = node || { type: 'opera' };
            return getNodeStyle(base, selectedNode).stroke;
          })
          .attr('stroke-width', 2)
          .attr('opacity', d => {
            const node = nodeById[d.data.id];
            return node ? (isNodeVisible(node) ? 1 : 0.2) : 1;
          })
          .style('cursor', 'pointer')
          .on('contextmenu', (event, d) => {
            event.preventDefault(); event.stopPropagation();
            try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
            try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
            const node = nodeById[d.data.id]; if (!node) return;
            const menuOffset = 20; const nodeRadius = 40; const containerRect = container.getBoundingClientRect();
            // Convert local (d.x, d.y) to screen coordinates
            const ctm = g.node().getScreenCTM();
            const pt = svgRef.current.createSVGPoint();
            pt.x = d.x; pt.y = d.y;
            const sp = pt.matrixTransform(ctm);
            const nodeCX = sp.x - containerRect.left; const nodeCY = sp.y - containerRect.top;
            let finalX = nodeCX + nodeRadius + menuOffset; let finalY = nodeCY - nodeRadius;
            const menuWidth = 250, menuHeight = 300, containerWidth = containerRect.width, containerHeight = containerRect.height;
            if (finalX + menuWidth > containerWidth) finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
            if (finalY + menuHeight > containerHeight) finalY = containerHeight - menuHeight;
            if (finalY < 0) finalY = 0; if (finalX < 0) finalX = 0;
            setTimeout(() => { setContextMenu({ show: true, x: finalX, y: finalY, node }); setExpandSubmenu(null); }, 0);
          })
          .on('click', (event, d) => {
            event.stopPropagation();
            const node = nodeById[d.data.id];
            if (!node) return;
            if (node.type === 'person') {
              setSearchType('singers');
              getItemDetails({ name: node.id }, 'singers');
            } else if (node.type === 'opera') {
              setSearchType('operas');
              getItemDetails({ properties: { opera_name: node.name, title: node.name } }, 'operas');
            } else if (node.type === 'book') {
              setSearchType('books');
              getItemDetails({ properties: { title: node.name } }, 'books');
            }
          });

        // Wrapped labels inside circle
        downNode.each(function(d){
          const group = d3.select(this); const fontSize = 11; const radius = 40; const maxWidth = radius * 1.6;
          const lines = wrapText(this, d.data.id, maxWidth, fontSize); const lineHeight = fontSize * 1.2;
          if (lines.length === 1) {
            const node = nodeById[d.data.id] || { type: 'opera' };
            const bg = getNodeStyle(node, selectedNode).fill;
            const tc = getAccessibleTextColor(bg, (fontSize >= 18));
            group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight', '600').attr('fill', tc.fill).attr('stroke', tc.needsHalo ? (tc.fill === '#FFFFFF' ? '#111827' : '#FFFFFF') : 'none').attr('stroke-width', tc.needsHalo ? 0.8 : 0).attr('text-anchor','middle').attr('dy','0.35em').style('pointer-events','none').text(lines[0]);
          } else {
            const totalHeight = (lines.length - 1) * lineHeight; const startOffset = -(totalHeight / 2);
            lines.forEach((line,i)=>{
              const node = nodeById[d.data.id] || { type: 'opera' };
              const bg = getNodeStyle(node, selectedNode).fill;
              const tc = getAccessibleTextColor(bg, (fontSize >= 18));
              group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight','600').attr('fill',tc.fill).attr('stroke', tc.needsHalo ? (tc.fill === '#FFFFFF' ? '#111827' : '#FFFFFF') : 'none').attr('stroke-width', tc.needsHalo ? 0.8 : 0).attr('text-anchor','middle').attr('y', startOffset + (i * lineHeight)).attr('dy','0.35em').style('pointer-events','none').text(line);
            });
          }
        });

        // Intentionally skip any automatic fit to avoid unintended recentering

        return;
      }

      // Prepare link data to always reference node objects (not just id strings)
      const nodeById = new Map();
      const invalidNodeIds = [];
      (networkData.nodes || []).forEach(node => {
        if (!node) return;
        const normalizedId = normalizeNodeId(node?.id ?? node?.name);
        if (!normalizedId || isPlaceholderName(normalizedId)) {
          invalidNodeIds.push(node?.id ?? node?.name ?? '');
          return;
        }
        node.id = normalizedId;
        nodeById.set(normalizedId, node);
      });
      if (invalidNodeIds.length > 0) {
        try {
          console.warn('[cmg-debug] Dropping placeholder or invalid nodes before simulation', invalidNodeIds);
        } catch (_) {}
      }
      const rawLinks = Array.isArray(networkData.links) ? networkData.links : [];
      const linkData = [];
      let droppedLinksCount = 0;
      rawLinks.forEach(link => {
        const sourceId = resolveLinkEndpointId(link?.source);
        const targetId = resolveLinkEndpointId(link?.target);
        if (
          !sourceId ||
          !targetId ||
          isPlaceholderName(sourceId) ||
          isPlaceholderName(targetId) ||
          !nodeById.has(sourceId) ||
          !nodeById.has(targetId)
        ) {
          droppedLinksCount += 1;
          return;
        }
        linkData.push({
          ...link,
          source: nodeById.get(sourceId),
          target: nodeById.get(targetId)
        });
      });
      if (isLayoutDebug()) {
        try {
          const expansionInternalCount = linkData.reduce((acc, l) => acc + (l.expansionInternal ? 1 : 0), 0);
          debugLog('render-link-stats', { total: linkData.length, expansionInternal: expansionInternalCount });
        } catch (_) {}
      }
      if (droppedLinksCount > 0) {
        try {
          console.warn('[graph] Dropped links referencing missing or placeholder nodes before rendering', { droppedLinksCount });
        } catch (_) {}
      }
      const getEndpointNode = (endpoint) => (
        typeof endpoint === 'string'
          ? nodeById.get(resolveLinkEndpointId(endpoint))
          : endpoint
      );

      // Create links (using paths)
      const link = g.append("g")
        .selectAll("path")
        .data(linkData)
        .enter()
        .append("path")
        .attr("stroke", "#6B7280")
        .attr("stroke-opacity", .6)
        .attr("stroke-width", 1.5)
        .attr("fill", "none")
        .attr("opacity", d => isLinkVisible(d) ? 0.6 : 0.12) // Apply filter-based opacity
        .on("pointerdown.longpress", function(event, d) {
          if (event.pointerType !== 'touch') {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          const linkType = (d && d.type ? String(d.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          const srcNode = getEndpointNode(d.source);
          const tgtNode = getEndpointNode(d.target);
          const isPersonPerson = srcNode?.type === 'person' && tgtNode?.type === 'person';
          const isPersonOpera =
            (srcNode?.type === 'person' && tgtNode?.type === 'opera') ||
            (srcNode?.type === 'opera' && tgtNode?.type === 'person');
          if (!isPersonPerson && !isPersonOpera) {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          scheduleLinkLongPress(event, d, this);
        })
        .on("pointermove.longpress", function(event) {
          if (event.pointerType !== 'touch') return;
          if (linkLongPressState.pointerId !== event.pointerId || linkLongPressState.timerId === null) return;
          const currentX = Number.isFinite(event.clientX) ? event.clientX : (Number.isFinite(event.pageX) ? event.pageX : linkLongPressState.startX);
          const currentY = Number.isFinite(event.clientY) ? event.clientY : (Number.isFinite(event.pageY) ? event.pageY : linkLongPressState.startY);
          const dx = currentX - linkLongPressState.startX;
          const dy = currentY - linkLongPressState.startY;
          if (Math.hypot(dx, dy) > LONG_PRESS_MOVE_CANCEL_PX * 0.8) {
            clearLinkLongPress({ releasePointer: true });
          }
        })
        .on("pointerup.longpress pointercancel.longpress pointerleave.longpress", function(event) {
          if (event.pointerType !== 'touch') return;
          if (longPressClickSuppressRef.current) {
            if (event.cancelable) event.preventDefault();
            event.stopPropagation();
            setTimeout(() => { longPressClickSuppressRef.current = false; }, 250);
            return;
          }
          const isSamePointer = linkLongPressState.pointerId === event.pointerId;
          const longPressFired = linkLongPressState.fired && isSamePointer;
          clearLinkLongPress({ releasePointer: isSamePointer });
          if (longPressFired && event.cancelable) {
            event.preventDefault();
            event.stopPropagation();
          }
        })
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Close any open menus before handling link right-click
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
          const linkType = (d && d.type ? String(d.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            return;
          }
          const containerRect = container.getBoundingClientRect();
          const mouseX = isMobileViewport ? 0 : Math.max(0, event.clientX - containerRect.left);
          const mouseY = isMobileViewport ? 0 : Math.max(0, event.clientY - containerRect.top);
          setTimeout(() => {
            const srcNode = getEndpointNode(d.source);
            const tgtNode = getEndpointNode(d.target);
            const isPersonToOpera = srcNode?.type === 'person' && tgtNode?.type === 'opera';
            const { sourceValues, sourceText, sourceUrl, baseValues } = buildLinkContextSource(d);
            if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
              window.__CMG_LINK_CONTEXT_LOGS = window.__CMG_LINK_CONTEXT_LOGS || [];
              window.__CMG_LINK_CONTEXT_LOGS.push({
                type: d?.type,
                link: d,
                derivedSourceText: sourceText,
                derivedSourceUrl: sourceUrl,
                sourceValues,
                baseValues
              });
              try {
                // eslint-disable-next-line no-console
                console.debug('[cmg] link context menu', {
                  type: d?.type,
                  derivedSourceText: sourceText,
                  derivedSourceUrl: sourceUrl,
                  teacher_rel_source_text: d?.teacher_rel_source_text,
                  teacher_rel_source_url: d?.teacher_rel_source_url,
                  sourceUrl: d?.sourceUrl
                });
              } catch (_) {}
            }
            setLinkContextMenu({
              show: true,
              x: Math.max(0, mouseX),
              y: Math.max(0, mouseY),
              role: isPersonToOpera && d.type === 'premiered' ? (d.role || d.target?.role || '') : '',
              sourceValues,
              sourceText,
              sourceUrl
            });
            try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
          }, 0);
        });

      // Invisible, wider hit area for easier right-click on links
      const linkHit = g.append("g")
        .selectAll("path.link-hit")
        .data(linkData)
        .enter()
        .append("path")
        .attr("class", "link-hit")
        .attr("stroke", "transparent")
        .attr("stroke-width", 12)
        .attr("fill", "none")
        .style("pointer-events", "stroke")
        .on("pointerdown.longpress", function(event, d) {
          if (event.pointerType !== 'touch') {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          const linkType = (d && d.type ? String(d.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          const srcNode = getEndpointNode(d.source);
          const tgtNode = getEndpointNode(d.target);
          const isPersonPerson = srcNode?.type === 'person' && tgtNode?.type === 'person';
          const isPersonOpera =
            (srcNode?.type === 'person' && tgtNode?.type === 'opera') ||
            (srcNode?.type === 'opera' && tgtNode?.type === 'person');
          if (!isPersonPerson && !isPersonOpera) {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          scheduleLinkLongPress(event, d, this);
        })
        .on("pointermove.longpress", function(event) {
          if (event.pointerType !== 'touch') return;
          if (linkLongPressState.pointerId !== event.pointerId || linkLongPressState.timerId === null) return;
          const currentX = Number.isFinite(event.clientX) ? event.clientX : (Number.isFinite(event.pageX) ? event.pageX : linkLongPressState.startX);
          const currentY = Number.isFinite(event.clientY) ? event.clientY : (Number.isFinite(event.pageY) ? event.pageY : linkLongPressState.startY);
          const dx = currentX - linkLongPressState.startX;
          const dy = currentY - linkLongPressState.startY;
          if (Math.hypot(dx, dy) > LONG_PRESS_MOVE_CANCEL_PX * 0.8) {
            clearLinkLongPress({ releasePointer: true });
          }
        })
        .on("pointerup.longpress pointercancel.longpress pointerleave.longpress", function(event) {
          if (event.pointerType !== 'touch') return;
          if (longPressClickSuppressRef.current) {
            if (event.cancelable) event.preventDefault();
            event.stopPropagation();
            return;
          }
          const isSamePointer = linkLongPressState.pointerId === event.pointerId;
          const longPressFired = linkLongPressState.fired && isSamePointer;
          clearLinkLongPress({ releasePointer: isSamePointer });
          if (longPressFired && event.cancelable) {
            event.preventDefault();
            event.stopPropagation();
          }
        })
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Close any open menus before handling link right-click
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
          const linkType = (d && d.type ? String(d.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            return;
          }
          const containerRect = container.getBoundingClientRect();
          const mouseX = isMobileViewport ? 0 : Math.max(0, event.clientX - containerRect.left);
          const mouseY = isMobileViewport ? 0 : Math.max(0, event.clientY - containerRect.top);
          setTimeout(() => {
            const srcNode = getEndpointNode(d.source);
            const tgtNode = getEndpointNode(d.target);
            const isPersonToOpera = srcNode?.type === 'person' && tgtNode?.type === 'opera';
            const { sourceValues, sourceText, sourceUrl } = buildLinkContextSource(d);
            setLinkContextMenu({
              show: true,
              x: Math.max(0, mouseX),
              y: Math.max(0, mouseY),
              role: isPersonToOpera && d.type === 'premiered' ? (d.role || d.target?.role || '') : '',
              sourceValues,
              sourceText,
              sourceUrl
            });
            try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
          }, 0);
        });

      // Build directed-pair groups so multiple relationships share one label (joined with ", ")
      const getNodeId = n => (typeof n === 'string' ? n : (n?.id || n?.name || ''));
      const normalizeRelLabel = (lbl) => {
        if (!lbl && lbl !== 0) return '';
        const v = String(lbl).toLowerCase();
        if (v === 'parentof' || v === 'parent') return 'parent';
        if (v === 'grandparentof' || v === 'grandparent') return 'grandparent';
        return v;
      };
      const toNodeObj = (n) => getEndpointNode(n);
      const isSymmetricRelation = (ld) => {
        const t = String(ld?.label || ld?.role || ld?.type || '').toLowerCase();
        return t.includes('spouse') || t.includes('sibling') || t.includes('family');
      };
      const directedPairKey = (s, t, ld) => {
        const a = getNodeId(s);
        const b = getNodeId(t);
        if (isSymmetricRelation(ld)) {
          return `${[a, b].sort().join('~')}~sym`;
        }
        return `${a}->${b}`;
      };
      const directedGroups = new Map();
      linkData.forEach(ld => {
        const sObj = toNodeObj(ld.source);
        const tObj = toNodeObj(ld.target);
        if (!sObj || !tObj) return;
        const key = directedPairKey(sObj, tObj, ld);
        if (!directedGroups.has(key)) {
          directedGroups.set(key, { source: sObj, target: tObj, labels: [], isPath: false });
        }
        const g = directedGroups.get(key);
        if (ld.label) g.labels.push(normalizeRelLabel(ld.label));
        if (ld.role && !ld.label) g.labels.push(normalizeRelLabel(ld.role));
        g.isPath = g.isPath || !!ld.isPath;
      });
      const linkLabelData = Array.from(directedGroups.values()).map(g => ({
        source: g.source,
        target: g.target,
        label: (g.labels.length ? Array.from(new Set(g.labels)).join(', ') : ''),
        count: g.labels.length,
        isPath: g.isPath
      }));

      // Create link labels
      const linkLabels = g.append("g")
        .selectAll("text")
        .data(linkLabelData)
        .enter()
        .append("text")
        .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
        .attr("font-size", "10px")
        .attr("font-weight", "500")
        .attr("fill", "#FFFFFF")
        .attr("text-anchor", "middle")
        .attr("dy", "-5px")
        .style("pointer-events", "auto")
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Close any open menus before handling link-label right-click
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
          const containerRect = container.getBoundingClientRect();
          const mouseX = isMobileViewport ? 0 : Math.max(0, event.clientX - containerRect.left);
          const mouseY = isMobileViewport ? 0 : Math.max(0, event.clientY - containerRect.top);
          // Find a representative link for this directed pair
          const srcId = resolveLinkEndpointId(d.source);
          const tgtId = resolveLinkEndpointId(d.target);
          const matching = linkData.find(l => resolveLinkEndpointId(l.source) === srcId && resolveLinkEndpointId(l.target) === tgtId);
          const linkType = (matching && matching.type ? String(matching.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            return;
          }
          const srcNode = getEndpointNode(d.source);
          const tgtNode = getEndpointNode(d.target);
          const isPersonToOpera = srcNode?.type === 'person' && tgtNode?.type === 'opera';
          setTimeout(() => {
            const { sourceValues, sourceText, sourceUrl } = buildLinkContextSource(matching);
            setLinkContextMenu({
              show: true,
              x: Math.max(0, mouseX),
              y: Math.max(0, mouseY),
              role: isPersonToOpera && matching?.type === 'premiered' ? (matching.role || '') : '',
              sourceValues,
              sourceText,
              sourceUrl
            });
            try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
          }, 0);
        })
        .attr("opacity", d => isLinkVisible(d) ? 1 : 0.2) // Apply filter-based opacity
        .text(d => d.label);

      // Invisible hit rectangles for labels to enlarge click area
      const approximateTextWidth = (text) => {
        const str = typeof text === 'string' ? text : String(text || '');
        return Math.max(60, str.length * 7 + 24);
      };
      const LABEL_HIT_HEIGHT = 32;
      const linkLabelHits = g.append("g")
        .selectAll("rect.link-label-hit")
        .data(linkLabelData)
        .enter()
        .append("rect")
        .attr("class", "link-label-hit")
        .attr("fill", "transparent")
        .attr("stroke", "none")
        .style("opacity", 0)
        .style("pointer-events", "all")
        .on("pointerdown.longpress", function(event, d) {
          if (event.pointerType !== 'touch') {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          const srcId = resolveLinkEndpointId(d.source);
          const tgtId = resolveLinkEndpointId(d.target);
          const matching = linkData.find(l => resolveLinkEndpointId(l.source) === srcId && resolveLinkEndpointId(l.target) === tgtId);
          const linkType = (matching && matching.type ? String(matching.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          const srcNode = getEndpointNode(d.source);
          const tgtNode = getEndpointNode(d.target);
          const isPersonPerson = srcNode?.type === 'person' && tgtNode?.type === 'person';
          const isPersonOpera =
            (srcNode?.type === 'person' && tgtNode?.type === 'opera') ||
            (srcNode?.type === 'opera' && tgtNode?.type === 'person');
          if (!isPersonPerson && !isPersonOpera) {
            clearLinkLongPress({ releasePointer: true });
            return;
          }
          scheduleLinkLongPress(event, d, this);
        })
        .on("pointermove.longpress", function(event) {
          if (event.pointerType !== 'touch') return;
          if (linkLongPressState.pointerId !== event.pointerId || linkLongPressState.timerId === null) return;
          const currentX = Number.isFinite(event.clientX) ? event.clientX : (Number.isFinite(event.pageX) ? event.pageX : linkLongPressState.startX);
          const currentY = Number.isFinite(event.clientY) ? event.clientY : (Number.isFinite(event.pageY) ? event.pageY : linkLongPressState.startY);
          const dx = currentX - linkLongPressState.startX;
          const dy = currentY - linkLongPressState.startY;
          if (Math.hypot(dx, dy) > LONG_PRESS_MOVE_CANCEL_PX) {
            clearLinkLongPress({ releasePointer: true });
          }
        })
        .on("pointerup.longpress pointercancel.longpress pointerleave.longpress", function(event) {
          if (event.pointerType !== 'touch') return;
          const isSamePointer = linkLongPressState.pointerId === event.pointerId;
          const longPressFired = linkLongPressState.fired && isSamePointer;
          clearLinkLongPress({ releasePointer: isSamePointer });
          if (longPressFired && event.cancelable) {
            event.preventDefault();
            event.stopPropagation();
          }
        })
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Close any open menus before handling link-label-hit right-click
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu(createLinkContextMenuState()); } catch(_) {}
          const containerRect = container.getBoundingClientRect();
          const mouseX = isMobileViewport ? 0 : Math.max(0, event.clientX - containerRect.left);
          const mouseY = isMobileViewport ? 0 : Math.max(0, event.clientY - containerRect.top);
          const srcId = resolveLinkEndpointId(d.source);
          const tgtId = resolveLinkEndpointId(d.target);
          const matching = linkData.find(l => resolveLinkEndpointId(l.source) === srcId && resolveLinkEndpointId(l.target) === tgtId);
          const linkType = (matching && matching.type ? String(matching.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            return;
          }
          const srcNode = getEndpointNode(d.source);
          const tgtNode = getEndpointNode(d.target);
          const isPersonToOpera = srcNode?.type === 'person' && tgtNode?.type === 'opera';
          setTimeout(() => {
            const { sourceValues, sourceText, sourceUrl } = buildLinkContextSource(matching);
            setLinkContextMenu({
              show: true,
              x: Math.max(0, mouseX),
              y: Math.max(0, mouseY),
              role: isPersonToOpera && matching?.type === 'premiered' ? (matching.role || '') : '',
              sourceValues,
              sourceText,
              sourceUrl
            });
            try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
          }, 0);
        });
      // Create nodes
      const getNodeHitRadius = () => {
        const base = 40 * 0.75; // 75% of visual node radius
        const k = zoomTransformRef.current && zoomTransformRef.current.k;
        return (k && k > 0) ? (base / k) : base;
      };

      const node = g.append("g")
        .selectAll("circle")
        .data(networkData.nodes)
        .enter()
        .append("circle")
        .attr("r", 40)
        .attr("fill", d => getNodeStyle(d, selectedNode).fill)
        .attr("stroke", d => getNodeStyle(d, selectedNode).stroke)
        .attr("stroke-width", d => getNodeStyle(d, selectedNode).strokeWidth)
        .attr("opacity", d => isNodeVisible(d) ? 1 : 0.2) // Apply filter-based opacity
        .style("cursor", "pointer")
        .on("pointerdown.longpress", function(event, d) {
          try {
            const [px, py] = d3.pointer(event, g.node());
            if (Math.hypot(px - d.x, py - d.y) > getNodeHitRadius()) {
              clearNodeLongPress({ releasePointer: true });
              return;
            }
          } catch (_) {}
          if (event.pointerType !== 'touch') {
            clearNodeLongPress({ releasePointer: true });
            return;
          }
          scheduleNodeLongPress(event, d, this);
        })
        .on("pointermove.longpress", function(event) {
          if (event.pointerType !== 'touch') return;
          if (nodeLongPressState.pointerId !== event.pointerId || nodeLongPressState.timerId === null) return;
          try {
            const [px, py] = d3.pointer(event, g.node());
            const datum = nodeLongPressState.datum;
            if (datum && Math.hypot(px - datum.x, py - datum.y) > getNodeHitRadius()) {
              clearNodeLongPress({ releasePointer: true });
              return;
            }
          } catch (_) {}
          const dx = (event.clientX ?? nodeLongPressState.startX) - nodeLongPressState.startX;
          const dy = (event.clientY ?? nodeLongPressState.startY) - nodeLongPressState.startY;
          if (Math.hypot(dx, dy) > LONG_PRESS_MOVE_CANCEL_PX) {
            clearNodeLongPress({ releasePointer: true });
          }
        })
        .on("pointerup.longpress pointercancel.longpress pointerleave.longpress", function(event, d) {
          if (event.pointerType !== 'touch') return;
          try {
            const [px, py] = d3.pointer(event, g.node());
            if (Math.hypot(px - d.x, py - d.y) > getNodeHitRadius()) {
              clearNodeLongPress({ releasePointer: true });
              return;
            }
          } catch (_) {}
          if (longPressClickSuppressRef.current) {
            if (event.cancelable) event.preventDefault();
            event.stopPropagation();
            setTimeout(() => { longPressClickSuppressRef.current = false; }, 250);
            return;
          }
          if (dragActiveRef.current || dragSuppressClickRef.current) {
            if (event.cancelable) event.preventDefault();
            event.stopPropagation();
            dragSuppressClickRef.current = false;
            return;
          }
          const isSamePointer = nodeLongPressState.pointerId === event.pointerId;
          const longPressFired = nodeLongPressState.fired && isSamePointer;
          clearNodeLongPress({ releasePointer: isSamePointer });
          if (longPressFired) {
            if (event.cancelable) {
              event.preventDefault();
            }
            event.stopPropagation();
            return;
          }
          if (event.type !== 'pointerup' || !d) return;
          suppressNextClickRef.current = true;
          setTimeout(() => {
            suppressNextClickRef.current = false;
          }, 320);
          const nodeId = d.id || null;
          const isDoubleTap = nodeClickTimeoutRef.current && lastTappedNodeIdRef.current && lastTappedNodeIdRef.current === nodeId;
          if (isDoubleTap) {
            clearPendingNodeAction();
            lastTappedNodeIdRef.current = null;
            handleNodeDoubleActivation(d);
          } else {
            lastTappedNodeIdRef.current = nodeId;
            handleNodeSingleActivation(d);
          }
          if (event.cancelable) {
            event.preventDefault();
          }
          event.stopPropagation();
        })
        .on("click", (event, d) => {
          const pointerType = event?.sourceEvent?.pointerType || event.pointerType || '';
          try {
            const [px, py] = d3.pointer(event, g.node());
            if (Math.hypot(px - d.x, py - d.y) > getNodeHitRadius()) {
              event.stopPropagation();
              return;
            }
          } catch (_) {}
          if (dragActiveRef.current || dragSuppressClickRef.current) {
            event.stopPropagation();
            dragSuppressClickRef.current = false;
            return;
          }
          if (longPressClickSuppressRef.current) {
            event.stopPropagation();
            longPressClickSuppressRef.current = false;
            return;
          }
          if (pointerType === 'touch' && suppressNextClickRef.current) {
            event.stopPropagation();
            return;
          }
          event.stopPropagation();
          lastTappedNodeIdRef.current = null;
          handleNodeSingleActivation(d);
        })
        .on("dblclick", (event, d) => {
          try {
            const [px, py] = d3.pointer(event, g.node());
            if (Math.hypot(px - d.x, py - d.y) > getNodeHitRadius()) {
              event.preventDefault();
              event.stopPropagation();
              return;
            }
          } catch (_) {}
          if (longPressClickSuppressRef.current) {
            event.preventDefault();
            event.stopPropagation();
            longPressClickSuppressRef.current = false;
            return;
          }
          if (dragActiveRef.current || dragSuppressClickRef.current) {
            event.preventDefault();
            event.stopPropagation();
            dragSuppressClickRef.current = false;
            return;
          }
          event.preventDefault();
          event.stopPropagation();
          clearPendingNodeAction();
          lastTappedNodeIdRef.current = null;
          handleNodeDoubleActivation(d);
        })
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Ensure any menus are closed when right-clicking a node
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
          try { setLinkContextMenu(createLinkContextMenuState()); } catch (_) {}
          
          // Calculate screen position using current zoom/pan transform
          const nodeRadius = 40;
          const menuOffset = 20;
          const containerRect = container.getBoundingClientRect();
          const ctm = g.node().getScreenCTM();
          const pt = svgRef.current.createSVGPoint();
          pt.x = d.x; pt.y = d.y;
          const sp = pt.matrixTransform(ctm);
          const nodeCX = sp.x - containerRect.left;
          const nodeCY = sp.y - containerRect.top;
          
          let finalX;
          let finalY;
          if (isMobileViewport) {
            finalX = 0;
            finalY = 0;
          } else {
            // Position relative to the container (not absolute)
            finalX = nodeCX + nodeRadius + menuOffset;
            finalY = nodeCY - nodeRadius;

            // Keep menu within container bounds
            const menuWidth = 250; // estimated menu width
            const menuHeight = 300; // estimated menu height
            const containerWidth = containerRect.width;
            const containerHeight = containerRect.height;

            if (finalX + menuWidth > containerWidth) {
              finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
            }
            if (finalY + menuHeight > containerHeight) {
              finalY = containerHeight - menuHeight;
            }
            if (finalY < 0) finalY = 0;
            if (finalX < 0) finalX = 0;
          }
          
          // Temporarily lock zoom updates while opening/closing the menu
          setTimeout(() => {
            // Snapshot prior to opening context menu / expansions
            pushHistory('context-open');
            zoomLockedRef.current = true;
            try {
              setContextMenu({ show: true, x: finalX, y: finalY, node: d });
              setExpandSubmenu(null);
              try {
                const t = zoomTransformRef.current || d3.zoomIdentity;
                const svgSel = d3.select(svgRef.current);
                svgSel.property('__zoom', t);
                g.attr('transform', t);
              } catch (_) {}
            } finally {
              // Unlock after a short delay to allow React to render menu without D3 zoom interference
              setTimeout(() => { zoomLockedRef.current = false; }, 50);
            }
          }, 0);
          // Clear any pending submenu timeout
          if (submenuTimeoutRef.current) {
            clearTimeout(submenuTimeoutRef.current);
          }
        })
        .call(
          d3.drag()
            .clickDistance(0)
            .filter(event => {
              const src = event?.sourceEvent || event;
              if (!src) return false;
              const buttons = typeof src.buttons === 'number' ? src.buttons : null;
              const button = typeof src.button === 'number' ? src.button : null;
              const isPrimary = (buttons === 1) || (button === 0) || (buttons === null && button === null);
              if (!isPrimary) return false;
              return !src.ctrlKey && !src.metaKey;
            })
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended)
        );

      // Directed-pair labels already grouped; no additional precompute needed

      // Create node labels with multi-line text support
      const nodeLabels = g.append("g")
        .selectAll("g")
        .data(networkData.nodes)
        .enter()
        .append("g")
        .attr("class", "node-label");
      // Add text elements for each node
      nodeLabels.each(function(d) {
        const group = d3.select(this);
        const fontSize = d.isCenter ? 11 : 10;
        const radius = 40;
        const maxWidth = radius * 1.6;
        
        const lines = wrapText(this, d.name, maxWidth, fontSize);
        const lineHeight = fontSize * 1.2;
        
        if (lines.length === 1) {
          const bg = getNodeStyle(d, selectedNode).fill;
          const tc = getAccessibleTextColor(bg, (fontSize >= 18));
          group.append("text")
            .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
            .attr("font-size", `${fontSize}px`)
            .attr("font-weight", d.isCenter ? "600" : "500")
            .attr("fill", tc.fill)
            .attr("stroke", 'none')
            .attr("stroke-width", 0)
            .attr("text-anchor", "middle")
            .attr("x", 0)
            .attr("y", 0)
            .attr("dy", "0.35em")
            .style("pointer-events", "none")
            .text(lines[0]);
        } else {
          const totalHeight = (lines.length - 1) * lineHeight;
          const startOffset = -(totalHeight / 2);
          
          lines.forEach((line, i) => {
            const bg = getNodeStyle(d, selectedNode).fill;
            const tc = getAccessibleTextColor(bg, (fontSize >= 18));
            group.append("text")
              .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
              .attr("font-size", `${fontSize}px`)
              .attr("font-weight", d.isCenter ? "600" : "500")
              .attr("fill", tc.fill)
              .attr("stroke", 'none')
              .attr("stroke-width", 0)
              .attr("text-anchor", "middle")
              .attr("x", 0)
              .attr("y", startOffset + (i * lineHeight))
              .attr("dy", "0.35em")
              .style("pointer-events", "none")
              .text(line);
          });
        }
      });

      
      const renderNetwork = () => {
        // Compute parallel link metadata so multiple relationships between the same pair don't overlap
        const groupMap = new Map();
        const pairKey = (l) => {
          const s = typeof l.source === 'string' ? l.source : l.source?.id;
          const t = typeof l.target === 'string' ? l.target : l.target?.id;
          return `${[s, t].sort().join('~')}~${l.type}`;
        };
        linkData.forEach(l => {
          const key = pairKey(l);
          if (!groupMap.has(key)) groupMap.set(key, []);
          groupMap.get(key).push(l);
        });
        groupMap.forEach(arr => {
          arr.forEach((l, i) => { l._parallelIndex = i; l._parallelCount = arr.length; });
        });
        // Convert link source/target to objects if they're strings
        const processedLinks = linkData;
        const resolveCoords = (endpoint) => {
          const node =
            typeof endpoint === 'string'
              ? nodeById.get(resolveLinkEndpointId(endpoint))
              : endpoint;
          if (!node) return null;
          const x = Number(node.x);
          const y = Number(node.y);
          if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
          return { x, y, node };
        };

        // Position links
        const longEdgeThreshold = (typeof window !== 'undefined' && Number.isFinite(window.__CMG_LONG_EDGE_THRESHOLD)) ? window.__CMG_LONG_EDGE_THRESHOLD : 260;
        link
          .attr("stroke", _d => "#FFFFFF")
          .attr("stroke-width", d => d.isPath ? 2.5 : 1.5)
          .attr("stroke-opacity", _d => 1)
          .attr("d", d => {
          const source = resolveCoords(d.source);
          const target = resolveCoords(d.target);

          if (!source || !target) return '';

          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          if (!Number.isFinite(distance) || distance === 0) return '';
          const nodeRadius = 40;
          const baseMargin = d.isPath ? 14 : 16;
          const margin = d.type === 'premiered' ? 8 : baseMargin;
          const adjusted = Math.max(0, margin - 5);
          const adjustedStart = Math.max(0, adjusted - 5);
          const endX = target.x - (dx / distance) * (nodeRadius + adjusted);
          const endY = target.y - (dy / distance) * (nodeRadius + adjusted);
          const startX = source.x + (dx / distance) * (nodeRadius + adjustedStart);
          const startY = source.y + (dy / distance) * (nodeRadius + adjustedStart);

          // Curve offset perpendicular to the line for parallel links
          const count = d._parallelCount || 1;
          const index = d._parallelIndex || 0;
          const spread = 14; // revert to previous spacing
          const offset = count > 1 ? (index - (count - 1) / 2) * spread : 0;
          const nx = distance ? -dy / distance : 0;
          const ny = distance ? dx / distance : 0;
          const mx = (startX + endX) / 2 + nx * offset;
          const my = (startY + endY) / 2 + ny * offset;
          if (!Number.isFinite(mx) || !Number.isFinite(my)) return '';

          const path = `M${startX},${startY} Q ${mx},${my} ${endX},${endY}`;
          if (isLayoutDebug()) {
            const len = Math.hypot(dx, dy);
            d.__renderDistance = len;
            if (len > longEdgeThreshold) debugLog('link-long-render', { s: source.node.id, t: target.node.id, type: d.type, len });
          }
          return path;
        })
        .attr("opacity", d => isLinkVisible(d) ? 1 : 0.12); // Apply filter-based opacity

        // Keep hit area in sync with link positions
        linkHit.attr("d", d => {
          const source = resolveCoords(d.source);
          const target = resolveCoords(d.target);
          if (!source || !target) return '';
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          if (!Number.isFinite(distance) || distance === 0) return '';
          const nodeRadius = 40;
          const baseMargin = d.isPath ? 14 : 16;
          const margin = d.type === 'premiered' ? 8 : baseMargin;
          const adjusted = Math.max(0, margin - 5);
          const adjustedStart = Math.max(0, adjusted - 5);
          const endX = target.x - (dx / distance) * (nodeRadius + adjusted);
          const endY = target.y - (dy / distance) * (nodeRadius + adjusted);
          const startX = source.x + (dx / distance) * (nodeRadius + adjustedStart);
          const startY = source.y + (dy / distance) * (nodeRadius + adjustedStart);
          const count = d._parallelCount || 1;
          const index = d._parallelIndex || 0;
          const spread = 14;
          const offset = count > 1 ? (index - (count - 1) / 2) * spread : 0;
          const nx = distance ? -dy / distance : 0;
          const ny = distance ? dx / distance : 0;
          const mx = (startX + endX) / 2 + nx * offset;
          const my = (startY + endY) / 2 + ny * offset;
          if (!Number.isFinite(mx) || !Number.isFinite(my)) return '';
          return `M${startX},${startY} Q ${mx},${my} ${endX},${endY}`;
        });

        // Remove arrows and link paths, then redraw from merged link data to avoid stale directions
        g.selectAll(".arrow-group").remove();
        link.attr("d", d => {
          const source = resolveCoords(d.source);
          const target = resolveCoords(d.target);
          if (!source || !target) return '';
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          const nodeRadius = 40;
          const margin = d.isPath ? 15 : 17;
          const adjusted = Math.max(0, margin - 5);
          const adjustedStart = Math.max(0, adjusted - 5);
          if (!Number.isFinite(distance) || distance < 1e-6) {
            // Self-loop: U-shaped loop below the node, from bottom-right to bottom-left
            const x = source.x;
            const y = source.y;
            const sideOffset = 6; // narrower
            const loopHeight = 80; // longer
            const sx = x + sideOffset;
            const sy = y + (nodeRadius + adjustedStart);
            const ex = x - sideOffset;
            const ey = y + (nodeRadius + adjustedStart);
            const cp1x = x + 25;
            const cp1y = y + nodeRadius + loopHeight;
            const cp2x = x - 25;
            const cp2y = y + nodeRadius + loopHeight;
            return `M${sx},${sy} C ${cp1x},${cp1y} ${cp2x},${cp2y} ${ex},${ey}`;
          }
          const endX = target.x - (dx / distance) * (nodeRadius + adjusted);
          const endY = target.y - (dy / distance) * (nodeRadius + adjusted);
          const startX = source.x + (dx / distance) * (nodeRadius + adjustedStart);
          const startY = source.y + (dy / distance) * (nodeRadius + adjustedStart);
          const path = `M${startX},${startY}L${endX},${endY}`;
          if (isLayoutDebug()) {
            d.__renderDistance = distance;
          }
          return path;
        });

        // When layout debug is enabled, visually flag long edges
        if (isLayoutDebug()) {
          link.attr('stroke', d => {
            const src = resolveCoords(d.source); const tgt = resolveCoords(d.target);
            if (!src || !tgt) return '#FFFFFF';
            const len = Math.hypot(tgt.x - src.x, tgt.y - src.y);
            if (len > longEdgeThreshold) return '#ef4444'; // red for long edges
            if (d.expansionInternal) return '#93c5fd'; // light blue for internal
            return '#FFFFFF';
          });
        }
        
        // Create arrows directly for each link
        processedLinks.forEach(linkData => {
          const source = resolveCoords(linkData.source);
          const target = resolveCoords(linkData.target);
          if (!source || !target) return;
          
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          
          if (Number.isFinite(distance) && distance > 0) {
            const nodeRadius = 40;
            const extraBase = linkData.isPath ? 12 : 10;
            const extra = Math.max(0, extraBase - 5);
            const arrowX = target.x - (dx / distance) * (nodeRadius + extra);
            const arrowY = target.y - (dy / distance) * (nodeRadius + extra);
            
            const angle = Math.atan2(dy, dx);
            const arrowLength = linkData.isPath ? 10 : 8;
            
            const x1 = arrowX - Math.cos(angle - Math.PI / 6) * arrowLength;
            const y1 = arrowY - Math.sin(angle - Math.PI / 6) * arrowLength;
            
            const x2 = arrowX - Math.cos(angle + Math.PI / 6) * arrowLength;
            const y2 = arrowY - Math.sin(angle + Math.PI / 6) * arrowLength;
            
            g.append("polygon")
              .attr("class", "arrow-group")
              .attr("points", `${arrowX},${arrowY} ${x1},${y1} ${x2},${y2}`)
              .attr("fill", "#FFFFFF")
              .attr("opacity", isLinkVisible(linkData) ? 1 : 0.12)
              .attr("stroke", "none");
          } else {
            // Self-loop arrow: at the returning end on bottom-left, pointing up toward node
            const nodeRadius = 40;
            const extraBase = linkData.isPath ? 12 : 10;
            const extra = Math.max(0, extraBase - 5);
            const arrowLength = linkData.isPath ? 10 : 8;
            const sideOffset = 6;
            const loopHeight = 80;
            const x = source ? source.x : 0;
            const y = source ? source.y : 0;
            const cp2x = x - 25;
            const cp2y = y + nodeRadius + loopHeight;
            const arrowX = x - sideOffset;
            const arrowY = y + (nodeRadius + extra);
            const angle = Math.atan2(arrowY - cp2y, arrowX - cp2x); // tangent at end, about upward
            const x1 = arrowX - Math.cos(angle - Math.PI / 6) * arrowLength;
            const y1 = arrowY - Math.sin(angle - Math.PI / 6) * arrowLength;
            const x2 = arrowX - Math.cos(angle + Math.PI / 6) * arrowLength;
            const y2 = arrowY - Math.sin(angle + Math.PI / 6) * arrowLength;
            g.append("polygon")
              .attr("class", "arrow-group")
              .attr("points", `${arrowX},${arrowY} ${x1},${y1} ${x2},${y2}`)
              .attr("fill", "#FFFFFF")
              .attr("opacity", isLinkVisible(linkData) ? 1 : 0.12)
              .attr("stroke", "none");
          }
        });

        // Position link labels (directed): center along each edge
        linkLabels
          .attr("x", d => {
            const source = getEndpointNode(d.source);
            const target = getEndpointNode(d.target);
            if (!source || !target) return 0;
            if (source === target) {
              const nodeRadius = 40;
              const margin = d.isPath ? 15 : 17;
              const adjusted = Math.max(0, margin - 5);
              const adjustedStart = Math.max(0, adjusted - 5);
              const sideOffset = 6;
              const loopHeight = 80;
              // left vertical part midpoint
              return source.x - sideOffset - 6;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startX = source.x + (dx / len) * (nodeRadius + adjustedStart);
            const endX = target.x - (dx / len) * (nodeRadius + adjusted);
            // midpoint between start and end of the visible segment
            return (startX + endX) / 2;
          })
          .attr("y", d => {
            const source = getEndpointNode(d.source);
            const target = getEndpointNode(d.target);
            if (!source || !target) return 0;
            if (source === target) {
              const nodeRadius = 40;
              const margin = d.isPath ? 15 : 17;
              const adjusted = Math.max(0, margin - 5);
              const adjustedStart = Math.max(0, adjusted - 5);
              const sideOffset = 6;
              const loopHeight = 80;
              return source.y + (nodeRadius + adjustedStart) + loopHeight / 2;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startY = source.y + (dy / len) * (nodeRadius + adjustedStart);
            const endY = target.y - (dy / len) * (nodeRadius + adjusted);
            // midpoint between start and end of the visible segment
            return (startY + endY) / 2;
          })
          // keep label-hit rectangles aligned, and rotate labels parallel to edges
          .attr("text-anchor", _d => 'middle')
          // Rotate labels parallel to the link; keep upright and adjust anchor directionality
          .attr("transform", function(d) {
            const source = getEndpointNode(d.source);
            const target = getEndpointNode(d.target);
            if (!source || !target) return '';
            if (source === target) {
              const attrX = this && this.getAttribute ? parseFloat(this.getAttribute('x')) : NaN;
              const attrY = this && this.getAttribute ? parseFloat(this.getAttribute('y')) : NaN;
              const x = Number.isFinite(attrX) ? attrX : (source.x - 16);
              const y = Number.isFinite(attrY) ? attrY : (source.y + 50);
              // Align vertically along the left leg
              return `rotate(-90, ${x}, ${y})`;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const angle = Math.atan2(dy, dx) * 180 / Math.PI;
            // Use the current midpoint attrs as the rotation center
            const attrX = this && this.getAttribute ? parseFloat(this.getAttribute('x')) : NaN;
            const attrY = this && this.getAttribute ? parseFloat(this.getAttribute('y')) : NaN;
            const x = Number.isFinite(attrX) ? attrX : (source.x + target.x) / 2;
            const y = Number.isFinite(attrY) ? attrY : (source.y + target.y) / 2;
            const adjustedAngle = Math.abs(angle) > 90 ? angle + 180 : angle;
            return `rotate(${adjustedAngle}, ${x}, ${y})`;
          })
          .attr("opacity", d => isLinkVisible(d) ? 1 : 0.2) // Apply filter-based opacity
          .attr("fill", _d => "#FFFFFF");

        // Position and size label hit rects (centered on the edge)
        linkLabelHits
          .attr("x", function(d) {
            const source = getEndpointNode(d.source);
            const target = getEndpointNode(d.target);
            if (!source || !target) return -9999;
            if (source === target) {
              const nodeRadius = 40;
              const margin = d.isPath ? 15 : 17;
              const adjusted = Math.max(0, margin - 5);
              const adjustedStart = Math.max(0, adjusted - 5);
              const sideOffset = 6;
              const loopHeight = 80;
              const centerX = source.x - sideOffset - 6;
              const w = approximateTextWidth(d.label);
              return centerX - w / 2;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startX = source.x + (dx / len) * (nodeRadius + adjustedStart);
            const endX = target.x - (dx / len) * (nodeRadius + adjusted);
            const x = (startX + endX) / 2;
            const w = approximateTextWidth(d.label);
            return x - w / 2;
          })
          .attr("y", function(d) {
            const source = getEndpointNode(d.source);
            const target = getEndpointNode(d.target);
            if (!source || !target) return -9999;
            if (source === target) {
              const nodeRadius = 40;
              const margin = d.isPath ? 15 : 17;
              const adjusted = Math.max(0, margin - 5);
              const adjustedStart = Math.max(0, adjusted - 5);
              const loopHeight = 80;
              const centerY = source.y + (nodeRadius + adjustedStart) + loopHeight / 2;
              return centerY - LABEL_HIT_HEIGHT / 2;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startY = source.y + (dy / len) * (nodeRadius + adjustedStart);
            const endY = target.y - (dy / len) * (nodeRadius + adjusted);
            const y = (startY + endY) / 2;
            return y - LABEL_HIT_HEIGHT / 2;
          })
          .attr("width", d => approximateTextWidth(d.label))
          .attr("height", LABEL_HIT_HEIGHT)
          .attr("transform", function(d) {
            const source = getEndpointNode(d.source);
            const target = getEndpointNode(d.target);
            if (!source || !target) return '';
            const width = approximateTextWidth(d.label);
            const height = LABEL_HIT_HEIGHT;
            let centerX;
            let centerY;
            if (source === target) {
              const nodeRadius = 40;
              const margin = d.isPath ? 15 : 17;
              const adjusted = Math.max(0, margin - 5);
              const adjustedStart = Math.max(0, adjusted - 5);
              const sideOffset = 6;
              const loopHeight = 80;
              centerX = source.x - sideOffset - 6;
              centerY = source.y + (nodeRadius + adjustedStart) + loopHeight / 2;
              return `rotate(-90, ${centerX}, ${centerY})`;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startX = source.x + (dx / len) * (nodeRadius + adjustedStart);
            const endX = target.x - (dx / len) * (nodeRadius + adjusted);
            const startY = source.y + (dy / len) * (nodeRadius + adjustedStart);
            const endY = target.y - (dy / len) * (nodeRadius + adjusted);
            centerX = (startX + endX) / 2;
            centerY = (startY + endY) / 2;
            const angle = Math.atan2(dy, dx) * 180 / Math.PI;
            const adjustedAngle = Math.abs(angle) > 90 ? angle + 180 : angle;
            return `rotate(${adjustedAngle}, ${centerX}, ${centerY})`;
          });
        // Position nodes
        node
          .attr("cx", d => d.x)
          .attr("cy", d => d.y)
          .attr("stroke", d => getNodeStyle(d, selectedNode).stroke)
          .attr("stroke-width", d => getNodeStyle(d, selectedNode).strokeWidth)
          .attr("opacity", d => isNodeVisible(d) ? 1 : 0.2) // Apply filter-based opacity
          .style("cursor", "pointer");

        // Position node labels
        nodeLabels
          .attr("transform", d => `translate(${d.x}, ${d.y})`)
          .attr("opacity", d => isNodeVisible(d) ? 1 : 0.2); // Apply filter-based opacity to labels too

      };
      // Create simulation for initial positioning only - controlled by shouldRunSimulation flag
      const nodesForSimulation = (networkData.nodes || []).filter(node => node && nodeById.has(node.id));
      if (!nodesForSimulation.length) {
        try { console.warn('[cmg-debug] Aborting simulation; no eligible nodes after filtering.'); } catch (_) {}
        return;
      }

      let settleTimeout = null;
      let coolTimeout = null;

      if (shouldRunSimulation) {
        // Apply anti-overlap positioning if nodes don't have valid positions
        const needsInitialPlacement = nodesForSimulation.some(node => {
          if (!node) return true;
          const x = Number(node.x);
          const y = Number(node.y);
          if (!Number.isFinite(x) || !Number.isFinite(y)) return true;
          // Initial search results come in at (0, 0); treat that as unpositioned.
          if (Math.abs(x) < 1 && Math.abs(y) < 1) return true;
          return false;
        });

        const isExpansion = isExpansionSimulation;

        if (!isExpansion && needsInitialPlacement) {
          // Reset positions for anti-overlap system only for networks that truly need an initial layout.
          nodesForSimulation.forEach(node => {
            node.x = 0;
            node.y = 0;
          });
          positionNodesWithoutOverlap(nodesForSimulation, width, height);
        } else if (needsInitialPlacement) {
          // Expansions should preserve their spawn geometry; only fix nodes lacking coordinates.
          nodesForSimulation.forEach(node => {
            if (!node) return;
            const x = Number(node.x);
            const y = Number(node.y);
            if (!Number.isFinite(x) || !Number.isFinite(y)) {
              node.x = 0;
              node.y = 0;
            }
          });
        }
        
        // Reset simulation properties
        nodesForSimulation.forEach(node => {
          node.fx = null;
          node.fy = null;
          node.vx = 0; // Reset velocity
          node.vy = 0;
        });

        try {
          const hasPinnedNodes = nodesForSimulation.some(n => n && n.userPlaced);
          // Analyze network structure to adjust force parameters
          const nodeTypes = nodesForSimulation.reduce((acc, node) => {
            acc[node.type] = (acc[node.type] || 0) + 1;
            return acc;
          }, {});
          
          const hasOperas = nodeTypes.opera > 0;
          const hasBooks = nodeTypes.book > 0;
          const nodeCount = nodesForSimulation.length;
          
          // Adjust forces based on network composition
          let linkDistance = 140;
          let linkStrength = 0.3;
          let chargeStrength = -1000;
          baseChargeStrengthRef.current = chargeStrength;
          let collisionRadius = 60;
          
          if (hasOperas || hasBooks) {
            // Opera/book networks tend to be star-shaped (one center, many connections)
            linkDistance = 180; // Longer links for better spread
            linkStrength = 0.4; // Stronger links to maintain structure
            chargeStrength = -1200; // Stronger repulsion to prevent overlap
            collisionRadius = 65; // Larger collision radius
          }
          
          if (nodeCount > 10) {
            // Larger networks need different parameters
            chargeStrength = Math.max(-1500, chargeStrength - (nodeCount * 30));
            linkDistance = Math.max(120, linkDistance - (nodeCount * 3));
          }
          
          // Additional spacing for better visualization
          if (nodeCount > 5) {
            linkDistance += 20;
            collisionRadius += 10;
          }
          


          // Use different parameters for expansion vs initial simulations
          const simulationAlphaDecay = isExpansion ? 0.015 : 0.035; // Even slower decay to allow more settling
          const simulationAlphaMin = isExpansion ? 0.003 : 0.008;   // Lower minimum for more iterations
          const simulationVelocityDecay = isExpansion ? 0.35 : 0.55; // Lower velocity decay for smoother settle

          const now = Date.now();
          const expansionRelaxWindowMs = isExpansion ? 8000 : 5000;
          const relaxedChargeStrength = Math.round(chargeStrength * 0.35);
          const simulation = d3.forceSimulation(nodesForSimulation)
            .force("link", d3.forceLink(linkData)
              .id(d => d.id)
              .distance(l => {
                const sameBatch = !!(l && l.source && l.target &&
                  (l.source.expansionBatchId != null) &&
                  (l.source.expansionBatchId === l.target.expansionBatchId));
                if (l && (l.expansionInternal || sameBatch)) {
                  // Keep expansion clusters compact while giving them breathing room
                  const override = (typeof window !== 'undefined' && Number.isFinite(window.__CMG_INTERNAL_LINK_DISTANCE))
                    ? Math.max(40, Number(window.__CMG_INTERNAL_LINK_DISTANCE))
                    : null;
                  const defaultDistance = Math.max(230, linkDistance * 0.9);
                  const d = override != null ? override : defaultDistance;
                  l.__distanceUsed = d; if (isLayoutDebug()) debugLog('sim-link-distance', { type: l.type, expansionInternal: true, sameBatch, d });
                  return d;
                }
                const sPlaced = !!(l.source && l.source.userPlaced);
                const tPlaced = !!(l.target && l.target.userPlaced);
                if (sPlaced || tPlaced) {
                  const stretched = linkDistance * 1.35;
                  const d = Math.min(stretched, linkDistance + 140);
                  l.__distanceUsed = d; if (isLayoutDebug()) debugLog('sim-link-distance', { type: l.type, pinned: true, d });
                  return d;
                }
                l.__distanceUsed = linkDistance; if (isLayoutDebug()) debugLog('sim-link-distance', { type: l.type, d: linkDistance });
                return linkDistance;
              })
              .strength(l => {
                const sPlaced = !!(l.source && l.source.userPlaced);
                const tPlaced = !!(l.target && l.target.userPlaced);
                const base = linkStrength;
                // Stronger springs for expansion-internal edges (including same-batch links)
                const sameBatch = !!(l && l.source && l.target &&
                  (l.source.expansionBatchId != null) &&
                  (l.source.expansionBatchId === l.target.expansionBatchId));
                if (l && (l.expansionInternal || sameBatch)) {
                  const override = (typeof window !== 'undefined' && Number.isFinite(window.__CMG_INTERNAL_LINK_STRENGTH))
                    ? Math.max(0.01, Math.min(1, Number(window.__CMG_INTERNAL_LINK_STRENGTH)))
                    : null;
                  const defaultStrength = 0.55;
                  const stiff = override != null ? override : defaultStrength;
                  return Math.min(1, stiff);
                }
                if (sPlaced || tPlaced) {
                  return Math.max(0.02, base * 0.35);
                }
                return base;
              }))
            .force("charge", d3.forceManyBody().strength(n => {
              if (!n) return chargeStrength;
              if (n.userPlaced) return 0;
              const expandedAt = Number(n.recentlyExpandedAt || n.expansionBatchId);
              if (Number.isFinite(expandedAt) && now - expandedAt <= expansionRelaxWindowMs) {
                return relaxedChargeStrength;
              }
              return chargeStrength;
            }))
            .force("center", hasPinnedNodes ? null : d3.forceCenter(width / 2, height / 2))
            .force("collision", d3.forceCollide().radius(n => {
              if (!n) return collisionRadius;
              const expandedAt = Number(n.recentlyExpandedAt || n.expansionBatchId);
              if (Number.isFinite(expandedAt) && now - expandedAt <= expansionRelaxWindowMs) {
                return Math.max(30, Math.round(collisionRadius * 0.6));
              }
              return collisionRadius;
            }))
            .force("x", d3.forceX(width / 2).strength(n => {
              if (!n) return 0.1;
              if (n.userPlaced) return 0;
              const expandedAt = Number(n.recentlyExpandedAt || n.expansionBatchId);
              if (Number.isFinite(expandedAt) && now - expandedAt <= expansionRelaxWindowMs) {
                return 0.02; // keep expansion cluster from being dragged to viewport center
              }
              return 0.1;
            }))
            .force("y", d3.forceY(height / 2).strength(n => {
              if (!n) return 0.1;
              if (n.userPlaced) return 0;
              const expandedAt = Number(n.recentlyExpandedAt || n.expansionBatchId);
              if (Number.isFinite(expandedAt) && now - expandedAt <= expansionRelaxWindowMs) {
                return 0.02;
              }
              return 0.1;
            }))
            .alpha(1)
            .alphaDecay(simulationAlphaDecay)
            .alphaMin(simulationAlphaMin)
            .velocityDecay(simulationVelocityDecay);

          // Smooth warm-up / cool-down for better animation feel
          simulation.alphaTarget(isExpansion ? 0.28 : 0.18); // quick initial lift
          try {
            settleTimeout = setTimeout(() => {
              try { simulation.alphaTarget(isExpansion ? 0.16 : 0.1); } catch (_) {}
            }, 420);
            coolTimeout = setTimeout(() => {
              try { simulation.alphaTarget(0); } catch (_) {}
            }, isExpansion ? 1600 : 1100);
          } catch (_) {}

          simulationRef.current = simulation;
          updateSimulationActive(true);
          try { zoomLockedRef.current = true; } catch (_) {}

          // Let simulation run for optimal balance of speed and quality
          const simulationDuration = isExpansion ? 5500 : 2200; // Longer durations for clearer settling
          const simulationTimeout = setTimeout(() => {
            if (simulation) {
              if (settleTimeout) clearTimeout(settleTimeout);
              if (coolTimeout) clearTimeout(coolTimeout);
              try { simulation.alphaTarget(0); } catch (_) {}
              simulation.stop();
              updateSimulationActive(false);
              setShouldRunSimulation(false);
              try { zoomLockedRef.current = false; } catch (_) {}
              if (isExpansion) {
                setIsExpansionSimulation(false); // Reset expansion flag
              }
              // Reassert current zoom once to prevent any implicit resets after layout settles
              try { applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}
              try {
                nodesForSimulation.forEach(node => {
                  if (!node) return;
                  if (!node.userPlaced) {
                    node.fx = node.x;
                    node.fy = node.y;
                  }
                  if (!Number.isFinite(node.homeX)) node.homeX = node.x;
                  if (!Number.isFinite(node.homeY)) node.homeY = node.y;
                });
              } catch (_) {}
              flushPendingHelperMessage();
            }
          }, simulationDuration);

          // Set up event handlers
          simulation.on("tick", () => {
            try {
              if (isLayoutDebug() && window.__CMG_DEBUG_TRACK_NODE) {
                const id = String(window.__CMG_DEBUG_TRACK_NODE);
                const found = nodesForSimulation.find(n => n && n.id === id);
                if (found) debugLog('tick-node', { id, x: Math.round(found.x), y: Math.round(found.y) });
              }
            } catch (_) {}
            renderNetwork();
          });
          
          simulation.on("end", () => {
            clearTimeout(simulationTimeout);
            if (settleTimeout) clearTimeout(settleTimeout);
            if (coolTimeout) clearTimeout(coolTimeout);
            try { simulation.alphaTarget(0); } catch (_) {}
            updateSimulationActive(false);
            setShouldRunSimulation(false); // Clear flag when simulation ends
            try { zoomLockedRef.current = false; } catch (_) {}
            if (isExpansion) {
              setIsExpansionSimulation(false); // Reset expansion flag
            }
            // Ensure whatever zoom user had is preserved post-simulation (single reapply)
            try { applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}
            try {
              nodesForSimulation.forEach(node => {
                if (!node) return;
                if (!node.userPlaced) {
                  node.fx = node.x;
                  node.fy = node.y;
                }
                if (!Number.isFinite(node.homeX)) node.homeX = node.x;
                if (!Number.isFinite(node.homeY)) node.homeY = node.y;
              });
            } catch (_) {}
            flushPendingHelperMessage();
          });
          
          simulation.restart();
          
        } catch (error) {
          console.error("❌ Error creating simulation:", error);
          if (settleTimeout) clearTimeout(settleTimeout);
          if (coolTimeout) clearTimeout(coolTimeout);
          try { zoomLockedRef.current = false; } catch (_) {}
          try {
            if (error && typeof error.message === 'string' && error.message.includes('node not found')) {
              const missingMatch = error.message.match(/node not found:\s*(.+)$/i);
              const missingIdRaw = missingMatch && missingMatch[1] ? missingMatch[1].trim() : null;
              const normalizedMissing = missingIdRaw ? normalizeNodeId(missingIdRaw) : '';
              const nodeIdsSnapshot = (networkData.nodes || []).map(n => n?.id).filter(Boolean);
              const linkSnapshot = (networkData.links || []).map(l => ({
                source: typeof l?.source === 'string' ? l.source : l?.source?.id ?? l?.source?.name ?? '',
                target: typeof l?.target === 'string' ? l.target : l?.target?.id ?? l?.target?.name ?? '',
                type: l?.type || '',
                label: l?.label || ''
              }));
              const offendingLinks = linkSnapshot.filter(l => {
                const s = normalizeNodeId(l.source);
                const t = normalizeNodeId(l.target);
                return (
                  (missingIdRaw && (l.source || '').includes(missingIdRaw)) ||
                  (missingIdRaw && (l.target || '').includes(missingIdRaw)) ||
                  (normalizedMissing && (s === normalizedMissing || t === normalizedMissing))
                );
              });
        const diagPayload = {
                missingIdRaw,
                normalizedMissing,
                nodeCount: nodeIdsSnapshot.length,
                hasMissingNode: normalizedMissing ? nodeIdsSnapshot.includes(normalizedMissing) : false,
                offendingLinks,
                rawLinkSnapshot: linkSnapshot.slice(0, 20),
                sampleNodes: nodeIdsSnapshot.slice(0, 20)
              };
              console.warn('[cmg-debug] Simulation missing node diagnostics', diagPayload);
              try {
                if (window && typeof window === 'object') {
                  window.__cmg_debugPayloads = window.__cmg_debugPayloads || [];
                  window.__cmg_debugPayloads.push({
                    timestamp: Date.now(),
                    kind: 'simulation-missing-node',
                    data: diagPayload
                  });
                  window.__cmg_lastNetworkOnError = {
                    nodes: Array.isArray(networkData.nodes) ? [...networkData.nodes] : [],
                    links: Array.isArray(networkData.links) ? [...networkData.links] : []
                  };
                }
              } catch (_) {}
              if (normalizedMissing) {
                try {
                  setNetworkData(prev => {
                    if (!prev) return prev;
                    const filteredNodes = (prev.nodes || []).filter(node => normalizeNodeId(node?.id ?? node?.name) !== normalizedMissing);
                    const filteredLinks = (prev.links || []).filter(link => {
                      const srcId = normalizeNodeId(resolveLinkEndpointId(link?.source));
                      const tgtId = normalizeNodeId(resolveLinkEndpointId(link?.target));
                      if (!srcId || !tgtId) return false;
                      if (srcId === normalizedMissing || tgtId === normalizedMissing) return false;
                      return true;
                    });
                    const cleaned = sanitizeGraphData({ nodes: filteredNodes, links: filteredLinks });
                    console.warn('[cmg-debug] Removed links/nodes referencing missing id after simulation error', {
                      normalizedMissing,
                      removedLinkDelta: (prev.links || []).length - (cleaned.links || []).length,
                      removedNodeDelta: (prev.nodes || []).length - (cleaned.nodes || []).length
                    });
                    return cleaned;
                  });
                } catch (cleanupErr) {
                  console.warn('[cmg-debug] Failed to clean up after simulation missing node error', cleanupErr);
                }
              }
            }
          } catch (diagErr) {
            console.warn('[cmg-debug] Failed to collect simulation diagnostics', diagErr);
          }
          updateSimulationActive(false);
          setShouldRunSimulation(false);
          flushPendingHelperMessage();
        }
      } else {
        // If we are here, the outer decision chose not to run the full sim (positions exist)
        // Build a dormant simulation so user interactions can reheat it while preserving layout
        try {
          const defaultLinkDistance = 160;
          const defaultLinkStrength = 0.35;
          const defaultChargeStrength = -1100;
          baseChargeStrengthRef.current = defaultChargeStrength;
          const defaultCollisionRadius = 60;
          const hasPinnedNodes = nodesForSimulation.some(n => n && n.userPlaced);
          const now = Date.now();
          const expansionRelaxWindowMs = 5000;
          const relaxedChargeStrength = Math.round(defaultChargeStrength * 0.35);
          const simulation = d3.forceSimulation(nodesForSimulation)
            .force("link", d3.forceLink(linkData)
              .id(d => d.id)
              .distance(l => {
                const sameBatch = !!(l && l.source && l.target &&
                  (l.source.expansionBatchId != null) &&
                  (l.source.expansionBatchId === l.target.expansionBatchId));
                if (l && (l.expansionInternal || sameBatch)) {
                  const override = (typeof window !== 'undefined' && Number.isFinite(window.__CMG_INTERNAL_LINK_DISTANCE))
                    ? Math.max(40, Number(window.__CMG_INTERNAL_LINK_DISTANCE))
                    : null;
                  const defaultDistance = Math.max(230, defaultLinkDistance * 0.9);
                  const d = override != null ? override : defaultDistance;
                  l.__distanceUsed = d; if (isLayoutDebug()) debugLog('sim-link-distance', { type: l.type, expansionInternal: true, sameBatch, d });
                  return d;
                }
                const sPlaced = !!(l.source && l.source.userPlaced);
                const tPlaced = !!(l.target && l.target.userPlaced);
                if (sPlaced || tPlaced) {
                  const stretched = defaultLinkDistance * 1.35;
                  const d = Math.min(stretched, defaultLinkDistance + 140);
                  l.__distanceUsed = d; if (isLayoutDebug()) debugLog('sim-link-distance', { type: l.type, pinned: true, d });
                  return d;
                }
                l.__distanceUsed = defaultLinkDistance; if (isLayoutDebug()) debugLog('sim-link-distance', { type: l.type, d: defaultLinkDistance });
                return defaultLinkDistance;
              })
              .strength(l => {
                const sPlaced = !!(l.source && l.source.userPlaced);
                const tPlaced = !!(l.target && l.target.userPlaced);
                const base = defaultLinkStrength;
                const sameBatch = !!(l && l.source && l.target &&
                  (l.source.expansionBatchId != null) &&
                  (l.source.expansionBatchId === l.target.expansionBatchId));
                if (l && (l.expansionInternal || sameBatch)) {
                  const override = (typeof window !== 'undefined' && Number.isFinite(window.__CMG_INTERNAL_LINK_STRENGTH))
                    ? Math.max(0.01, Math.min(1, Number(window.__CMG_INTERNAL_LINK_STRENGTH)))
                    : null;
                  const defaultStrength = 0.55;
                  const stiff = override != null ? override : defaultStrength;
                  return Math.min(1, stiff);
                }
                if (sPlaced || tPlaced) {
                  return Math.max(0.02, base * 0.35);
                }
                return base;
              }))
            .force("charge", d3.forceManyBody().strength(n => {
              if (!n) return Math.round(defaultChargeStrength);
              if (n.userPlaced) return 0;
              const expandedAt = Number(n.recentlyExpandedAt || n.expansionBatchId);
              if (Number.isFinite(expandedAt) && now - expandedAt <= expansionRelaxWindowMs) {
                return relaxedChargeStrength;
              }
              return Math.round(defaultChargeStrength);
            }))
            .force("center", hasPinnedNodes ? null : d3.forceCenter(width / 2, height / 2))
            .force("collision", d3.forceCollide().radius(n => {
              if (!n) return defaultCollisionRadius;
              const expandedAt = Number(n.recentlyExpandedAt || n.expansionBatchId);
              if (Number.isFinite(expandedAt) && now - expandedAt <= expansionRelaxWindowMs) {
                return Math.max(30, Math.round(defaultCollisionRadius * 0.6));
              }
              return defaultCollisionRadius;
            }))
            .force("x", d3.forceX(width / 2).strength(n => {
              if (!n) return 0.1;
              if (n.userPlaced) return 0;
              const expandedAt = Number(n.recentlyExpandedAt || n.expansionBatchId);
              if (Number.isFinite(expandedAt) && now - expandedAt <= expansionRelaxWindowMs) {
                return 0.02;
              }
              return 0.1;
            }))
            .force("y", d3.forceY(height / 2).strength(n => {
              if (!n) return 0.1;
              if (n.userPlaced) return 0;
              const expandedAt = Number(n.recentlyExpandedAt || n.expansionBatchId);
              if (Number.isFinite(expandedAt) && now - expandedAt <= expansionRelaxWindowMs) {
                return 0.02;
              }
              return 0.1;
            }))
            .alpha(0)
            .alphaDecay(0.035)
            .alphaMin(0.001)
            .velocityDecay(0.55);
          simulationRef.current = simulation;
          simulation.on("tick", () => { renderNetwork(); });
          simulation.stop();
          try {
            nodesForSimulation.forEach(node => {
              if (!node) return;
              if (!node.userPlaced) {
                node.fx = node.x;
                node.fy = node.y;
              }
              if (!Number.isFinite(node.homeX)) node.homeX = node.x;
              if (!Number.isFinite(node.homeY)) node.homeY = node.y;
            });
          } catch (_) {}
        } catch (_) {}
        updateSimulationActive(false);
        renderNetwork();
        flushPendingHelperMessage();
      }

      

      const DRAG_ACTIVATION_PX = 8;
      const startDragForNode = (dragEvent, node) => {
        if (!node) return;
        dragActiveRef.current = false;
        dragSuppressClickRef.current = false;
        dragStartPosRef.current = { x: node.x, y: node.y };

        if (simulationRef.current) {
          try {
            const sim = simulationRef.current;
            const base = baseChargeStrengthRef.current || -1000;
            const reduced = Math.round(base * 0.45);
            const neighborIds = new Set();
            (networkData.links || []).forEach(l => {
              const s = typeof l.source === 'string' ? l.source : l.source?.id;
              const t = typeof l.target === 'string' ? l.target : l.target?.id;
              if (s === node.id && t) neighborIds.add(t);
              if (t === node.id && s) neighborIds.add(s);
            });
            sim.force("charge", d3.forceManyBody().strength(n => {
              if (!n) return base;
              if (n.userPlaced) return 0;
              return neighborIds.has(n.id) ? Math.round(base * 0.25) : reduced;
            }));
            sim.alphaTarget(0.35).restart();
          } catch (_) {}
        }

        node.fx = node.x;
        node.fy = node.y;
        node.userPlaced = true;
        node.homeX = node.x;
        node.homeY = node.y;
        const stateNode = networkData.nodes.find(n => n.id === node.id);
        if (stateNode && stateNode !== node) {
          stateNode.fx = node.fx;
          stateNode.fy = node.fy;
          stateNode.x = node.x;
          stateNode.y = node.y;
        }
        if (stateNode) {
          stateNode.homeX = stateNode.x;
          stateNode.homeY = stateNode.y;
        }

        try {
          const neighborIds = new Set();
          (networkData.links || []).forEach(l => {
            const s = typeof l.source === 'string' ? l.source : l.source?.id;
            const t = typeof l.target === 'string' ? l.target : l.target?.id;
            if (s === node.id && t) neighborIds.add(t);
            if (t === node.id && s) neighborIds.add(s);
          });
          networkData.nodes.forEach(n => {
            if (!n || n.id === node.id) return;
            if (!n.userPlaced && !neighborIds.has(n.id)) {
              n.fx = n.x;
              n.fy = n.y;
              n.vx = 0; n.vy = 0;
              n._frozenDuringDrag = true;
              if (!Number.isFinite(n.homeX)) n.homeX = n.x;
              if (!Number.isFinite(n.homeY)) n.homeY = n.y;
            }
          });
        } catch (_) {}

        const origEvt = dragEvent && dragEvent.sourceEvent ? dragEvent.sourceEvent : dragEvent;
        const modifierHeld = !!(origEvt && (origEvt.shiftKey || origEvt.altKey));
        const isPathGroup = !!(node.wasAddedByPath || node.isPath) && modifierHeld;
        dragGroupIdsRef.current = new Set();
        dragGroupInitialPosRef.current = new Map();
        dragLeaderInitialPosRef.current = { x: node.x, y: node.y };
        if (isPathGroup) {
          const pathIds = new Set(
            networkData.nodes.filter(n => n.wasAddedByPath || n.isPath).map(n => n.id)
          );
          const neighbors = new Map();
          networkData.links.forEach(l => {
            const s = typeof l.source === 'string' ? l.source : l.source?.id;
            const t = typeof l.target === 'string' ? l.target : l.target?.id;
            if (!s || !t) return;
            if (!pathIds.has(s) || !pathIds.has(t)) return;
            if (!neighbors.has(s)) neighbors.set(s, new Set());
            if (!neighbors.has(t)) neighbors.set(t, new Set());
            neighbors.get(s).add(t);
            neighbors.get(t).add(s);
          });
          const stack = [node.id];
          const visited = new Set();
          while (stack.length) {
            const id = stack.pop();
            if (visited.has(id)) continue;
            visited.add(id);
            dragGroupIdsRef.current.add(id);
            const nodeObj = networkData.nodes.find(n => n.id === id);
            if (nodeObj) {
              dragGroupInitialPosRef.current.set(id, { x: nodeObj.x, y: nodeObj.y });
            }
            const nbrs = neighbors.get(id);
            if (nbrs) nbrs.forEach(nid => { if (!visited.has(nid)) stack.push(nid); });
          }
        } else {
          dragGroupIdsRef.current.add(node.id);
          dragGroupInitialPosRef.current.set(node.id, { x: node.x, y: node.y });
        }
      };

      function dragstarted(event, d) {
        dragActiveRef.current = false;
        startDragForNode(event, d);
      }

      function dragged(event, d) {
        if (!dragActiveRef.current) {
          // Activate drag only after movement threshold
          const dx0 = event.x - dragStartPosRef.current.x;
          const dy0 = event.y - dragStartPosRef.current.y;
          const dist = Math.hypot(dx0, dy0);
          if (dist >= DRAG_ACTIVATION_PX) {
            dragActiveRef.current = true;
            dragSuppressClickRef.current = true;
            clearNodeLongPress({ releasePointer: true });
          } else {
            return;
          }
        }

        const dx = event.x - dragLeaderInitialPosRef.current.x;
        const dy = event.y - dragLeaderInitialPosRef.current.y;

        dragGroupIdsRef.current.forEach(id => {
          const initial = dragGroupInitialPosRef.current.get(id);
          if (!initial) return;
          const nodeObj = networkData.nodes.find(n => n.id === id);
          if (!nodeObj) return;
          const nx = initial.x + dx;
          const ny = initial.y + dy;
          nodeObj.fx = nx; nodeObj.fy = ny; nodeObj.x = nx; nodeObj.y = ny;
        });

        if (simulationRef.current) {
          try { simulationRef.current.alpha(0.18); } catch (_) {}
        }
        renderNetwork();
      }

      function dragended(event, d) {
        if (!dragActiveRef.current) {
          return;
        }
        dragActiveRef.current = false;
        // Suppress click/doubleclick immediately after a drag
        dragSuppressClickRef.current = true;
        setTimeout(() => { dragSuppressClickRef.current = false; }, 300);
        dragGroupIdsRef.current.forEach(id => {
          const nodeObj = networkData.nodes.find(n => n.id === id);
          if (!nodeObj) return;
          nodeObj.fx = nodeObj.x;
          nodeObj.fy = nodeObj.y;
        });
        try {
          networkData.nodes.forEach(n => {
            if (!n || !n._frozenDuringDrag) return;
            if (!n.userPlaced) { n.fx = n.x; n.fy = n.y; }
            n._frozenDuringDrag = false;
            n.vx = 0; n.vy = 0;
            if (!Number.isFinite(n.homeX)) n.homeX = n.x;
            if (!Number.isFinite(n.homeY)) n.homeY = n.y;
          });
        } catch (_) {}
        if (simulationRef.current && !event.active) {
          try {
            const sim = simulationRef.current;
            const base = baseChargeStrengthRef.current || -1000;
            sim.force("charge", d3.forceManyBody().strength(n => (
              n && n.userPlaced ? 0 : Math.round(base)
            )));
            sim.alphaTarget(0);
          } catch (_) {}
        }
      }

      // Initial render (simulation will take over immediately)
      renderNetwork();

      // Cleanup
      return () => {
        clearNodeLongPress({ releasePointer: true });
        clearLinkLongPress({ releasePointer: true });
        if (typeof touchCleanup === 'function') {
          touchCleanup();
        }
        if (pendingZoomFrame) {
          cancelAnimationFrame(pendingZoomFrame);
          pendingZoomFrame = null;
        }
        try {
          if (typeof window !== 'undefined' && window.__cmg_centerGraph === centerGraphWithinViewport) {
            window.__cmg_centerGraph = null;
          }
        } catch (_) {}
        if (simulationRef.current) {
          simulationRef.current.stop();
        }
      };
    }, [
      networkData.nodes.length,
      networkData.links.length,
      visualizationHeight,
      birthYearRange[0],
      birthYearRange[1],
      deathYearRange[0],
      deathYearRange[1],
      selectedVoiceTypes,
      filtersVersion,
      currentCenterNode,
      viewportIsPhone,
      viewportIsTablet,
      shouldRunSimulation,
      isExpansionSimulation
    ]); // Re-run on data, height, filter, or simulation flags
    // Guard against outside clicks forcing any transform reset by reapplying zoom
    useEffect(() => {
      const onDocClickCapture = (e) => {
        // If sheets/overlays are open, do not touch zoom on outside clicks
        if (showFilterPanel || showPathPanel) return;
        const container = containerRef.current;
        if (!container) return;
        if (!container.contains(e.target)) {
          // Temporarily lock zoom updates and reapply current transform after handlers run
          try { zoomLockedRef.current = true; } catch (_) {}
          setTimeout(() => {
            try {
              const t = zoomTransformRef.current || d3.zoomIdentity;
              const svgSel = d3.select(svgRef.current);
              svgSel.property('__zoom', t);
              const g = svgSel.select('g');
              g.attr('transform', t);
              // Unlock shortly after to allow normal zooming again
              setTimeout(() => { try { zoomLockedRef.current = false; } catch (_) {} }, 0);
            } catch (_) {}
          }, 0);
        }
      };
      document.addEventListener('mousedown', onDocClickCapture, true);
      return () => {
        document.removeEventListener('mousedown', onDocClickCapture, true);
      };
    }, [showFilterPanel, showPathPanel]);

    return (
      <div
        className={viewportIsPhone ? 'mobile-network-shell' : undefined}
        style={{
          position: 'relative',
          boxSizing: 'border-box',
          width: '100%'
        }}
      >
        <div
          ref={containerRef}
          style={{
            position: 'relative',
            width: '100%',
            height: `${visualizationHeight}px`,
            border: viewportIsPhone ? '4px solid #FFFFFF' : '6px solid #FFFFFF',
            borderRadius: '8px',
            backgroundColor: '#3e96e2',
            overflow: 'hidden',
            boxSizing: 'border-box',
            marginBottom: viewportIsPhone ? '24px' : 0
          }}
        >
          {isSimulationLocked && (
            <div
              style={{
                position: 'absolute',
                inset: 0,
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 12,
                pointerEvents: 'auto',
                backgroundColor: viewportIsPhone ? 'rgba(15,23,42,0.18)' : 'transparent',
                color: '#f8fafc',
                fontWeight: 600,
                textAlign: 'center',
                fontSize: viewportIsPhone ? 16 : 14,
                cursor: viewportIsPhone ? 'default' : 'progress'
              }}
            >
              {viewportIsPhone && (
                <span style={{ padding: '8px 16px', borderRadius: 999, backgroundColor: 'rgba(15,23,42,0.45)' }}>
                  Layout stabilizing…
                </span>
              )}
            </div>
          )}
          <svg ref={svgRef}></svg>

          <ContextMenu />
            {linkContextMenu.show && (
              <div
                className="context-menu"
                style={{
                  position: 'absolute',
                  top: linkContextMenu.y,
                  left: linkContextMenu.x,
                  backgroundColor: 'white',
                  border: '2px solid #3e96e2',
                  borderRadius: '8px',
                  padding: '12px 16px 12px 12px',
                  boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
                  zIndex: 1000,
                  width: 'fit-content',
                  minWidth: '220px',
                  maxWidth: '520px',
                  paddingRight: '28px', // leave room for the dismissing ×
                  wordBreak: 'break-word',
                  whiteSpace: 'normal',
                  fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
                  fontSize: '16px'
                }}
                onClick={(e) => e.stopPropagation()}
              >
                <button
                  type="button"
                  onClick={() => setLinkContextMenu(createLinkContextMenuState())}
                  aria-label="Close relationship menu"
                  style={{
                    position: 'absolute',
                    top: 6,
                    right: 6,
                    border: 'none',
                    background: 'transparent',
                    color: '#6b7280',
                    fontSize: '18px',
                    cursor: 'pointer',
                    padding: 4,
                    lineHeight: 1
                  }}
                >
                  ×
                </button>
                {linkContextMenu.role && (
                  <>
                <div style={{ fontWeight: 600, marginBottom: '6px', color: '#1f2937' }}>
                  Premiered role in
                </div>
                <div style={{ color: '#374151', marginBottom: '4px' }}>
                      <strong>Role:</strong> {linkContextMenu.role}
                </div>
                  </>
                )}
                {(() => {
                  const content = renderRelationshipSourceLink(...(linkContextMenu.sourceValues || []));
                  const fallback = typeof linkContextMenu.sourceText === 'string'
                    ? linkContextMenu.sourceText.trim()
                    : '';
                  if (!content && !fallback) {
                    return (
                      <div style={{ color: '#374151' }}>
                        <strong>Relationship source:</strong>{' '}
                        <span style={{ color: '#6b7280' }}>Not provided</span>
                      </div>
                    );
                  }
                  return (
                    <div style={{ color: '#374151' }}>
                      <strong>Relationship source:</strong>{' '}
                      {content || fallback}
                    </div>
                  );
                })()}
              </div>
            )}
            <ProfileCard />
          </div>

          {/** Reusable handler so both button click and Enter key submit run the same code */}
          {(() => {
            // define on every render to capture latest state in closure
            window.__cmg_runFindPath = async () => {
                const from = pathFromValRef.current?.trim();
                const to = pathToValRef.current?.trim();
                if (!from || !to) return;
                let previousPathSnapshot = prePathNetworkRef.current;
                try {
                  // Snapshot before mutating the network with path overlay
                  pushHistory('path-find');
                  setLoading(true);
                  setError('');
                  // Snapshot current network before overlay so Clear can restore baseline
                  previousPathSnapshot = prePathNetworkRef.current;
                  prePathNetworkRef.current = {
                    nodes: networkData.nodes.map(n => ({ ...n })),
                    links: networkData.links.map(l => ({
                      ...l,
                      source: (typeof l.source === 'string' ? l.source : l.source?.id),
                      target: (typeof l.target === 'string' ? l.target : l.target?.id),
                      isPath: false,
                      wasAddedByPath: false
                    }))
                  };
                  if (pathApiUnavailableRef.current) {
                    throw new Error('Path finding is currently unavailable.');
                  }
                  const payload = { from, to, maxHops: 25 };
                  const resp = await fetchWithRetry(`${API_BASE}/path/find`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
                    body: JSON.stringify(payload)
                  }, { retries: 2, baseDelay: 600 });
                  if (!resp) {
                    throw new Error('No response from path service');
                  }
                  const textResp = await resp.text();
                  let data;
                  try {
                    data = textResp ? JSON.parse(textResp) : {};
                  } catch (_) {
                    data = { error: textResp || 'Invalid response' };
                  }
                  if (resp.status === 404) {
                    const errorMessage = typeof data?.error === 'string' ? data.error : '';
                    if (errorMessage.toLowerCase().includes('no path')) {
                      const friendlyMessage = `No path found between "${from}" and "${to}".`;
                      setError(friendlyMessage);
                      prePathNetworkRef.current = previousPathSnapshot || null;
                      pathApiUnavailableRef.current = false;
                      return;
                    }
                  }
                  if (!resp.ok) {
                    prePathNetworkRef.current = previousPathSnapshot || null;
                    if (resp.status >= 500) {
                      pathApiUnavailableRef.current = true;
                      throw new Error(data?.error || 'Path finding service is temporarily unavailable. Please try again later.');
                    }
                    throw new Error((data && data.error) || `Path find failed (${resp.status})`);
                  }
                  pathApiUnavailableRef.current = false;
                  const rawPathNodes = Array.isArray(data.nodes) ? data.nodes : [];
                  const sanitizedPathNodes = rawPathNodes
                    .map(node => {
                      const normalizedId = normalizeNodeId(node?.id ?? node?.name);
                      if (!normalizedId || isPlaceholderName(normalizedId)) return null;
                      return {
                        ...node,
                        id: normalizedId,
                        name: node?.name ? String(node.name).trim() || normalizedId : normalizedId
                      };
                    })
                    .filter(Boolean);
                  if (centerOnNodeRef.current && sanitizedPathNodes.length > 0) {
                    centerOnNodeRef.current(sanitizedPathNodes[0].id, { duration: 700 });
                  }
                  const pathNodeIdSet = new Set(sanitizedPathNodes.map(n => n.id));
                  const rawPathLinks = Array.isArray(data.links) ? data.links : [];
                  const sanitizedPathLinks = rawPathLinks
                    .map(link => {
                      const sourceId = resolveLinkEndpointId(link?.source);
                      const targetId = resolveLinkEndpointId(link?.target);
                      if (
                        !sourceId ||
                        !targetId ||
                        isPlaceholderName(sourceId) ||
                        isPlaceholderName(targetId) ||
                        !pathNodeIdSet.has(sourceId) ||
                        !pathNodeIdSet.has(targetId)
                      ) {
                        return null;
                      }
                      return {
                        ...link,
                        source: sourceId,
                        target: targetId
                      };
                    })
                    .filter(Boolean);
                  const normalizedPathLinks = normalizeLinks(
                    sanitizedPathLinks.map(link => ({ ...link }))
                  );
                  const normalizedSteps = Array.isArray(data.steps)
                    ? data.steps.map(step => {
                        const sourceText = deriveRelationshipSourceText(
                          step.relationshipSourceDisplay,
                          step.sourceInfo,
                          step.relationship_source,
                          step.source
                        );
                        const sourceUrl = deriveRelationshipSourceUrl(step.sourceUrl);
                        return {
                          ...step,
                          relationshipSourceDisplay: sourceText,
                          sourceInfo: sourceText,
                          sourceUrl: sourceUrl || null
                        };
                      })
                    : [];
                  setPathInfo({ nodes: sanitizedPathNodes, links: normalizedPathLinks, steps: normalizedSteps });
                  if (viewportIsPhone) {
                    if (pathFromRef.current) pathFromRef.current.value = '';
                    if (pathToRef.current) pathToRef.current.value = '';
                    pathFromValRef.current = '';
                    pathToValRef.current = '';
                    try { pathFromRef.current && pathFromRef.current.blur(); } catch (_) {}
                    try { pathToRef.current && pathToRef.current.blur(); } catch (_) {}
                  }
                  // Show the path on the graph by merging nodes/links and highlighting the path
                  const existingNodeMap = new Map();
                  networkData.nodes.forEach(existingNode => {
                    if (existingNode && existingNode.type === 'person') {
                      const normalized = normalizePersonNode(existingNode);
                      if (normalized !== existingNode) {
                        Object.assign(existingNode, normalized);
                      }
                    }
                    if (existingNode?.id) {
                      existingNodeMap.set(existingNode.id, existingNode);
                    }
                  });
                  const existingLinkKeys = new Set(
                    networkData.links.map(l => `${typeof l.source === 'string' ? l.source : l.source?.id}-${typeof l.target === 'string' ? l.target : l.target?.id}-${l.type}`)
                  );
                  const pathNodeIds = new Set(sanitizedPathNodes.map(n => n.id));

                  // Prepare merged nodes
                  const mergedNodes = [...networkData.nodes];
                  // ensure trackers exist and accumulate across runs
                  if (!pathOverlayRef.current.addedNodeIds) pathOverlayRef.current.addedNodeIds = new Set();
                  if (!pathOverlayRef.current.addedLinkKeys) pathOverlayRef.current.addedLinkKeys = new Set();
                  // Helper: place new node near an existing neighbor if possible
                  const placeNearA = (neighbor) => {
                    const angle = Math.random() * Math.PI * 2;
                    const radius = 90;
                    return { x: (neighbor.x || 300) + Math.cos(angle) * radius, y: (neighbor.y || 300) + Math.sin(angle) * radius };
                  };
                  // Build neighbor map from path links
                  const pathNeighbors = new Map();
                  sanitizedPathLinks.forEach(l => {
                    if (!pathNeighbors.has(l.source)) pathNeighbors.set(l.source, new Set());
                    if (!pathNeighbors.has(l.target)) pathNeighbors.set(l.target, new Set());
                    pathNeighbors.get(l.source).add(l.target);
                    pathNeighbors.get(l.target).add(l.source);
                  });

                  // Partition base constellation (non-path) vs path nodes
                  const baseConstellation = mergedNodes.filter(n => !pathNodeIds.has(n.id));
                  const existing = baseConstellation.length ? baseConstellation : mergedNodes;
                  const minX = Math.min(...existing.map(n => n.x || 0));
                  const maxX = Math.max(...existing.map(n => n.x || 0));
                  const minY = Math.min(...existing.map(n => n.y || 0));
                  const maxY = Math.max(...existing.map(n => n.y || 0));
                  const cx = (minX + maxX) / 2;
                  const cy = (minY + maxY) / 2;
                  const bboxMargin = 40;
                  const minDistance = 120;

                  const isInsideBBox = (x, y) => (x > (minX - bboxMargin) && x < (maxX + bboxMargin) && y > (minY - bboxMargin) && y < (maxY + bboxMargin));
                  const collides = (x, y) => existing.some(o => {
                    const ox = o.x || 0, oy = o.y || 0; const dx = x - ox, dy = y - oy; return Math.hypot(dx, dy) < minDistance;
                  });
                  const normalize = (vx, vy) => {
                    const m = Math.hypot(vx, vy) || 1; return { x: vx / m, y: vy / m };
                  };
                  const rotate = (vx, vy, ang) => {
                    const c = Math.cos(ang), s = Math.sin(ang); return { x: vx * c - vy * s, y: vx * s + vy * c };
                  };

                  // Compute a spawn center outside the current base constellation and place all new path nodes around it
                  const existingPathAnchors = sanitizedPathNodes.filter(n => existingNodeMap.has(n.id));
                  const anchorRef = existingPathAnchors.length > 0 ? existingNodeMap.get(existingPathAnchors[0].id) : { x: cx, y: cy };
                  const mergeKey = `${typeof from === 'string' ? from : ''}|${typeof to === 'string' ? to : ''}`;
                  const containerEl2 = document.querySelector('div[style*="height:"] > svg')?.parentElement || null;
                  const widthGuess2 = containerEl2 ? containerEl2.clientWidth : 800;
                  const heightGuess2 = visualizationHeight || 600;
                  const spawn = computeSpawnOutsideBBox(
                    baseConstellation,
                    { x: anchorRef?.x ?? cx, y: anchorRef?.y ?? cy, key: mergeKey },
                    280,
                    { width: widthGuess2, height: heightGuess2, pad: 60 }
                  );
                  const newOnlyIds = sanitizedPathNodes.filter(n => !existingNodeMap.has(n.id)).map(n => n.id);
                  const { min: pathRingMin, max: pathRingMax, spacing: pathRingSpacing } = getExpansionRingConfig(newOnlyIds.length);
                  const ringR = computeRingRadius(newOnlyIds.length, pathRingMin, pathRingMax, pathRingSpacing);
                  const posMap = new Map();
                  newOnlyIds.forEach((id, idx) => {
                    const ang = (idx / Math.max(1, newOnlyIds.length)) * Math.PI * 2;
                    posMap.set(id, { x: spawn.x + Math.cos(ang) * ringR, y: spawn.y + Math.sin(ang) * ringR });
                  });

                  sanitizedPathNodes.forEach(rawNode => {
                    const canonicalNode = normalizePersonNode(rawNode);
                    if (!canonicalNode?.id) return;
                    if (!existingNodeMap.has(canonicalNode.id)) {
                      const pos = posMap.get(canonicalNode.id) || { x: spawn.x, y: spawn.y };
                      const x = pos.x;
                      const y = pos.y;
                      const newNode = normalizePersonNode({ ...canonicalNode, x, y, isPath: true, wasAddedByPath: true });
                      mergedNodes.push(newNode);
                      existingNodeMap.set(newNode.id, newNode);
                      pathOverlayRef.current.addedNodeIds.add(newNode.id);
                    } else {
                      const ex = existingNodeMap.get(canonicalNode.id);
                      const updated = normalizePersonNode({ ...ex, ...canonicalNode, isPath: true });
                      Object.assign(ex, updated);
                    }
                  });
                  // Fixed height; do not auto-grow the canvas
                  // if (height > (visualizationHeight || 600)) setVisualizationHeight(height);

                  // Anti-overlap nudge for base constellation only (skip path nodes)
                  const containerEl = document.querySelector('div[style*="height:"] > svg')?.parentElement || null;
                  const widthGuess = containerEl ? containerEl.clientWidth : 800;
                  const baseOnly = mergedNodes.filter(n => !n.isPath);
                  positionNodesWithoutOverlap(baseOnly, widthGuess, visualizationHeight || 600);

                  // Prefetch person details for path endpoints so we can attach relationship sources
                  try {
                    const pathPersonNames = Array.from(new Set(
                      sanitizedPathNodes
                        .filter(n => n && n.type === 'person' && (n.name || n.id))
                        .map(n => n.name || n.id)
                        .filter(Boolean)
                    ));
                    await Promise.all(pathPersonNames.map(nm => fetchAndCachePersonDetails(nm)));
                  } catch (_) {}

                  // Helper: resolve relationship source for path links (person→person only)
                  const resolvePathRelSource = (srcName, trgName, relType) => {
                    try {
                      const cache = personCacheRef.current || new Map();
                      const src = cache.get(srcName);
                      const trg = cache.get(trgName);
                      const type = (relType || '').toLowerCase();
                      if (type === 'taught') {
                        const sStudents = (src?.students || []);
                        const tTeachers = (trg?.teachers || []);
                        const a = sStudents.find(x => x?.full_name === trgName);
                        if (a && a.teacher_rel_source) return a.teacher_rel_source;
                        const b = tTeachers.find(x => x?.full_name === srcName);
                        if (b && b.teacher_rel_source) return b.teacher_rel_source;
                      }
                      if (type === 'family' || type === 'parent' || type === 'spouse' || type === 'sibling' || type === 'grandparent') {
                        const sFam = (src?.family || []);
                        const tFam = (trg?.family || []);
                        const a = sFam.find(x => x?.full_name === trgName);
                        if (a && (a.teacher_rel_source || a.source)) return a.teacher_rel_source || a.source;
                        const b = tFam.find(x => x?.full_name === srcName);
                        if (b && (b.teacher_rel_source || b.source)) return b.teacher_rel_source || b.source;
                      }
                    } catch (_) {}
                    return '';
                  };

                  // Prepare merged links with path highlighting
                  const mergedLinks = [...networkData.links];
                  // Build orientation hints from existing graph
                  const teacherPairs = new Set(); // teacher->student
                  const workPairs = new Set(); // person->work
                  const familyPairs = new Set(); // keep as seen
                  mergedLinks.forEach(el => {
                    const s = typeof el.source === 'string' ? el.source : el.source?.id;
                    const t = typeof el.target === 'string' ? el.target : el.target?.id;
                    if (!s || !t) return;
                    if (el.type === 'taught') teacherPairs.add(`${s}->${t}`);
                    if (el.type === 'premiered' || el.type === 'composed' || el.type === 'authored' || el.type === 'edited') workPairs.add(`${s}->${t}`);
                    if (el.type === 'family') familyPairs.add(`${s}->${t}`);
                  });
                  sanitizedPathLinks.forEach(l => {
                    let src = l.source;
                    let trg = l.target;
                    const type = l.type;

                    // Enforce canonical orientation per relationship using existing graph as source of truth
                    const sourceNode = existingNodeMap.get(src) || sanitizedPathNodes.find(n => n.id === src);
                    const targetNode = existingNodeMap.get(trg) || sanitizedPathNodes.find(n => n.id === trg);
                    const isWorkType = type === 'premiered' || type === 'wrote' || type === 'composed' || type === 'authored' || type === 'edited';
                    if (type === 'taught') {
                      if (teacherPairs.has(`${src}->${trg}`)) {
                        // ok
                      } else if (teacherPairs.has(`${trg}->${src}`)) {
                        const tmp = src; src = trg; trg = tmp;
                      }
                      // else fall back to backend orientation
                    } else if (isWorkType && sourceNode && targetNode) {
                      // Person -> Work (Opera/Book)
                      const sourceIsPerson = sourceNode.type === 'person';
                      const targetIsWork = targetNode.type === 'opera' || targetNode.type === 'book';
                      if (!(sourceIsPerson && targetIsWork)) {
                        const tmp = src; src = trg; trg = tmp;
                      }
                    } else if (type === 'family') {
                      if (familyPairs.has(`${src}->${trg}`)) {
                        // keep
                      } else if (familyPairs.has(`${trg}->${src}`)) {
                        const tmp = src; src = trg; trg = tmp;
                      }
                    }

                    const key = `${src}-${trg}-${type}`;
                    const revKey = `${trg}-${src}-${type}`;
                    const existingIdx = mergedLinks.findIndex(ml => {
                      const s = typeof ml.source === 'string' ? ml.source : ml.source?.id;
                      const t = typeof ml.target === 'string' ? ml.target : ml.target?.id;
                      return `${s}-${t}-${ml.type}` === key || `${s}-${t}-${ml.type}` === revKey;
                    });

                    const srcName = src;
                    const trgName = trg;
                    const computedSourceInfo = resolvePathRelSource(srcName, trgName, type);
                    const pathSourceText = deriveRelationshipSourceText(
                      l.relationshipSourceDisplay,
                      l.relationship_source,
                      l.teacher_rel_source_text,
                      l.teacher_rel_source,
                      l.opera_source_text,
                      l.sourceInfo,
                      computedSourceInfo
                    );
                    const pathSourceUrl = deriveRelationshipSourceUrl(
                      l.teacher_rel_source_url,
                      l.opera_source_url,
                      l.sourceUrl
                    );
                    const isTeacherFamily = type === 'taught' || type === 'family';
                    const isOperaRel = type === 'premiered' || type === 'composed';

                    if (existingIdx >= 0) {
                      // Do NOT change existing orientation; just mark as path to match base graph
                      const cur = mergedLinks[existingIdx];
                      const updatedSource = deriveRelationshipSourceText(
                        cur.relationshipSourceDisplay,
                        cur.sourceInfo,
                        pathSourceText
                      );
                      const updatedUrl = deriveRelationshipSourceUrl(
                        cur.sourceUrl,
                        pathSourceUrl
                      );
                      const updatedLink = {
                        ...cur,
                        isPath: true,
                        relationshipSourceDisplay: updatedSource,
                        sourceInfo: updatedSource,
                        sourceUrl: updatedUrl || null
                      };
                      if (isTeacherFamily) {
                        const teacherText = deriveRelationshipSourceText(
                          cur.teacher_rel_source_text,
                          cur.teacher_rel_source,
                          pathSourceText
                        );
                        updatedLink.teacher_rel_source_text = teacherText || null;
                        updatedLink.teacher_rel_source = teacherText || null;
                        updatedLink.teacher_rel_source_url = cur.teacher_rel_source_url || pathSourceUrl || null;
                      }
                      if (isOperaRel) {
                        const operaText = deriveRelationshipSourceText(
                          cur.opera_source_text,
                          pathSourceText
                        );
                        updatedLink.opera_source_text = operaText || null;
                        updatedLink.opera_source_url = cur.opera_source_url || pathSourceUrl || null;
                      }
                      if (updatedSource) {
                        updatedLink.relationship_source = updatedSource;
                      }
                      mergedLinks[existingIdx] = updatedLink;
                    } else {
                      const newLink = {
                        ...l,
                        source: src,
                        target: trg,
                        isPath: true,
                        wasAddedByPath: true,
                        relationshipSourceDisplay: pathSourceText,
                        sourceInfo: pathSourceText,
                        relationship_source: pathSourceText,
                        sourceUrl: pathSourceUrl || null
                      };
                      if (isTeacherFamily) {
                        newLink.teacher_rel_source = pathSourceText || null;
                        newLink.teacher_rel_source_text = pathSourceText || null;
                        newLink.teacher_rel_source_url = pathSourceUrl || null;
                      }
                      if (isOperaRel) {
                        newLink.opera_source_text = pathSourceText || null;
                        newLink.opera_source_url = pathSourceUrl || null;
                      }
                      if (pathSourceText) {
                        newLink.relationship_source = pathSourceText;
                      }
                      mergedLinks.push(newLink);
                      pathOverlayRef.current.addedLinkKeys.add(key);
                    }
                  });

                  // Mark nodes in path
                  mergedNodes.forEach(n => { if (pathNodeIds.has(n.id)) n.isPath = true; });
                  setNetworkData(sanitizeGraphData({ nodes: mergedNodes, links: mergedLinks }));
                  try { setHasExecutedSearch(true); } catch (_) {}
                  const pathPersons = mergedNodes.filter(n => n && n.type === 'person' && pathNodeIds.has(n.id));
                  extendDateRangesForNodes(pathPersons, { resetUserRangeFlags: true });
                  // Enrich newly added path person nodes so CSV has full details
                  const newPersonNames = sanitizedPathNodes
                    .filter(n => n && n.type === 'person' && (n.name || n.id))
                    .map(n => n.name || n.id)
                    .filter(Boolean);
                  enrichPersonNodes(newPersonNames);
                  // After path overlay, briefly run an expansion-style simulation so nodes settle
                  setTimeout(() => {
                    setIsExpansionSimulation(true);
                    setShouldRunSimulation(true);
                  }, 120);
                } catch (e) {
                  prePathNetworkRef.current = previousPathSnapshot || prePathNetworkRef.current;
                  setError(e.message || 'Path find failed');
                } finally {
                  setLoading(false);
                  // After request, re-focus last edited field to avoid losing focus
                  if (document.activeElement === document.body) {
                    if (pathToRef.current) pathToRef.current.focus();
                  }
                }
              };
            // Provide a small helper to disable zoom if needed
            window.__cmg_disableZoomWhileScrolling = (disabled) => {
              // We rely on event.stopPropagation on the list, but keep this for future
              // hooks if we want to temporarily unbind zoom.
              // No-op currently.
              return !!disabled;
            };
            return null;
          })()}
          {viewportIsPhone && showPathPanel && (
            <div
              ref={pathPanelRef}
              style={{
                position: 'fixed',
                left: '16px',
                right: '16px',
                bottom: '112px',
                backgroundColor: 'white',
                border: '2px solid #3e96e2',
                borderRadius: '16px',
                boxShadow: '0 16px 40px rgba(37, 99, 235, 0.25)',
                padding: '16px',
                maxHeight: '60vh',
                overflowY: 'auto',
                zIndex: 1401,
                overscrollBehavior: 'contain'
              }}
              onWheel={(e) => { e.stopPropagation(); }}
            >
              {renderPathPanelContent({
              isMobile: true,
              pathFromRef,
              pathToRef,
              pathFromValRef,
              pathToValRef,
              pathInfo,
              pathListRef,
              handleClearPath,
              onFindPath: runPathFind,
              renderRelationshipSourceLink,
              onClose: closePathPanel
            })}
            </div>
          )}
          {/* Resizer removed */}
        
        {/* Height indicator removed */}
      </div>
    );
  };
  // Saved View Dialog
  const SavedViewDialog = () => {
    const [copied, setCopied] = useState(false);
    const [copyMessage, setCopyMessage] = useState('');
    if (!showSavedViewDialog) return null;
    const copyToClipboard = async (text, label = 'Copied to clipboard') => {
      try {
        await navigator.clipboard.writeText(text);
        setCopyMessage(label);
        setCopied(true);
        setTimeout(() => setCopied(false), 1800);
      } catch (_) {}
    };
    const combined = savedViewLabel ? `${savedViewToken}\nLabel: ${savedViewLabel}` : savedViewToken;

    const content = (
      <>
        <p style={{ margin: 0, color: '#555', fontSize: isMobileViewport ? '15px' : '14px' }}>
          Copy the string below to load this view later.
        </p>
        <div style={{ minHeight: 22 }}>
          <span style={{
            padding: '4px 10px',
            borderRadius: 9999,
            border: '2px solid #3e96e2',
            background: '#ecfdf5',
            color: '#065f46',
            fontSize: 12,
            opacity: copied ? 1 : 0,
            transition: 'opacity 200ms ease'
          }}>
            {copyMessage}
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <label style={{ fontSize: 12, color: '#666' }}>Saved view string</label>
          <div style={{ display: 'flex', flexDirection: isMobileViewport ? 'column' : 'row', gap: 8 }}>
            <textarea
              readOnly
              value={savedViewToken}
              style={{
                flex: 1,
                padding: 10,
                border: '2px solid #3e96e2',
                borderRadius: 10,
                fontFamily: 'monospace',
                fontSize: 13,
                resize: 'vertical',
                minHeight: isMobileViewport ? 100 : 60,
                color: '#111'
              }}
            />
            <button
              onClick={() => copyToClipboard(savedViewToken)}
              style={{
                padding: '12px 16px',
                backgroundColor: '#ffffff',
                color: '#374151',
                border: '2px solid #3e96e2',
                borderRadius: 10,
                cursor: 'pointer',
                fontSize: '16px',
                height: '48px'
              }}
            >
              Copy
            </button>
          </div>
          {savedViewLabel && (
            <>
              <label style={{ fontSize: 12, color: '#666' }}>Label</label>
              <div style={{ display: 'flex', flexDirection: isMobileViewport ? 'column' : 'row', gap: 8 }}>
                <input
                  readOnly
                  value={savedViewLabel}
                  style={{ flex: 1, padding: 10, border: '2px solid #3e96e2', borderRadius: 10, fontSize: 13, color: '#111' }}
                />
                <button
                  onClick={() => copyToClipboard(savedViewLabel)}
                  style={{ padding: '12px 16px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: 10, cursor: 'pointer', fontSize: '16px', height: '48px' }}
                >
                  Copy
                </button>
              </div>
            </>
          )}
        </div>
      </>
    );

    const actionButtons = (
      <>
        <button
          onClick={() => copyToClipboard(combined)}
          style={{ padding: '12px 16px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: 10, cursor: 'pointer', fontSize: '16px', height: '48px' }}
        >
          Copy All
        </button>
        <button
          onClick={() => setShowSavedViewDialog(false)}
          style={{ padding: '12px 16px', backgroundColor: '#111827', color: '#ffffff', border: '2px solid #111827', borderRadius: 10, cursor: 'pointer', fontSize: '16px', height: '48px' }}
        >
          Close
        </button>
      </>
    );

    if (isMobileViewport) {
      return (
        <>
          <div
            className="mobile-overlay-backdrop is-open"
            style={{ zIndex: 2000 }}
            onClick={() => setShowSavedViewDialog(false)}
          />
          <div
            className="mobile-sheet is-open"
            style={{ zIndex: 2001, paddingBottom: '24px' }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="mobile-sheet__header" style={{ paddingBottom: 0 }}>
              <div className="mobile-sheet__handle" />
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
                <h3 style={{ margin: 0, fontSize: '20px', fontWeight: 600, color: '#0f172a' }}>Saved View</h3>
                <button
                  onClick={() => setShowSavedViewDialog(false)}
                  style={{
                    background: 'none',
                    border: 'none',
                    color: '#1f2937',
                    fontSize: '24px',
                    cursor: 'pointer',
                    fontWeight: 600,
                    padding: 4,
                    width: 40,
                    height: 40,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center'
                  }}
                  aria-label="Close saved view dialog"
                >
                  ×
                </button>
              </div>
            </div>
            <div className="mobile-sheet__content" style={{ paddingTop: 12 }}>
              {content}
            </div>
            <div className="mobile-sheet__footer">
              {actionButtons}
            </div>
          </div>
        </>
      );
    }

    return (
      <div
        style={{
          position: 'fixed',
          inset: 0,
          backgroundColor: 'rgba(15,23,42,0.45)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 2000
        }}
        onClick={() => setShowSavedViewDialog(false)}
      >
        <div
          style={{
            background: 'white',
            borderRadius: 8,
            padding: 20,
            width: 520,
            boxShadow: '0 18px 44px rgba(15,23,42,0.3)',
            display: 'flex',
            flexDirection: 'column',
            gap: 12
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <h3 style={{ margin: 0 }}>Saved View</h3>
          {content}
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            {actionButtons}
          </div>
        </div>
      </div>
    );
  };

  // Function to fetch actual relationship counts from database
  const fetchActualCounts = async (node) => {
    if (actualCounts[node.id]) {
      return actualCounts[node.id];
    }

    const coerceCount = (value) => {
      const num = Number(value);
      return Number.isFinite(num) && num >= 0 ? num : 0;
    };

    try {
      let countsFromCountsEndpoint = null;

      if (
        !relationshipCountsUnavailableRef.current &&
        (node.type === 'person' || node.type === 'opera' || node.type === 'book')
      ) {
        try {
          const response = await fetchWithRetry(`${API_BASE}/node/relationship-counts`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify({ nodeType: node.type, nodeName: node.name })
          }, { retries: 1, baseDelay: 400 });

          if (response) {
            if (response.status === 404) {
              relationshipCountsUnavailableRef.current = true;
            } else if (response.ok) {
              const payload = await response.json();
              if (payload && payload.counts) {
                const countsPayload = payload.counts;
                countsFromCountsEndpoint = {
                  taughtBy: coerceCount(countsPayload.taughtBy),
                  taught: coerceCount(countsPayload.taught),
                  authored: coerceCount(countsPayload.authored),
                  premieredRoleIn: coerceCount(countsPayload.premieredRoleIn),
                  wrote: coerceCount(countsPayload.wrote),
                  parent: coerceCount(countsPayload.parent),
                  parentOf: coerceCount(countsPayload.parentOf),
                  spouse: coerceCount(countsPayload.spouse),
                  grandparent: coerceCount(countsPayload.grandparent),
                  grandparentOf: coerceCount(countsPayload.grandparentOf),
                  sibling: coerceCount(countsPayload.sibling),
                  edited: coerceCount(countsPayload.edited),
                  editedBy: coerceCount(countsPayload.editedBy)
                };
              }
            }
          }
        } catch (countErr) {
          if (countErr && countErr.status === 404) {
            relationshipCountsUnavailableRef.current = true;
          }
          // Fall through to legacy detail endpoints on failures
        }
      }

      if (countsFromCountsEndpoint) {
        setActualCounts(prev => ({ ...prev, [node.id]: countsFromCountsEndpoint }));
        return countsFromCountsEndpoint;
      }

      let response, data;
      
      if (node.type === 'person') {
        // All people are persons in Neo4j - use the network endpoint for all persons
        response = await fetchWithRetry(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 1 })
        }, { retries: 2, baseDelay: 600 });
      } else if (node.type === 'opera') {
        const { type: typedType, value: typedValue } = parseTypedId(node.id);
        const payload = { operaName: node.name };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
        }, { retries: 2, baseDelay: 600 });
      } else if (node.type === 'book') {
        response = await fetchWithRetry(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: node.name })
        }, { retries: 2, baseDelay: 600 });
      }

      if (response && response.ok) {
        data = await response.json();
      } else if (response) {
        // Throw error with status for proper handling
        const error = new Error(`HTTP ${response.status}: ${response.statusText}`);
        error.status = response.status;
        throw error;
      }

      // Process the data and count relationships
      const counts = {
        taughtBy: 0,
        taught: 0,
        authored: 0,
        premieredRoleIn: 0,
        wrote: 0,
        parent: 0,
        parentOf: 0,
        spouse: 0,
        grandparent: 0,
        grandparentOf: 0,
        sibling: 0,
        edited: 0,
        editedBy: 0
      };

      if (node.type === 'person') {
        if (data.teachers) counts.taughtBy = data.teachers.length;
        if (data.students) counts.taught = data.students.length;
        if (data.family) {
          data.family.forEach(relative => {
            const relType = relative.relationship_type?.toLowerCase() || '';
            if (relType.includes('parent') && relType.includes('of')) counts.parentOf++;
            else if (relType.includes('parent')) counts.parent++;
            else if (relType.includes('spouse')) counts.spouse++;
            else if (relType.includes('grandparent') && relType.includes('of')) counts.grandparentOf++;
            else if (relType.includes('grandparent')) counts.grandparent++;
            else if (relType.includes('sibling')) counts.sibling++;
          });
        }
        const composedOperas = Array.isArray(data.works?.composedOperas) ? data.works.composedOperas : [];
        const wroteRelationships = Array.isArray(data.wrote) ? data.wrote : [];

        if (data.works) {
          if (Array.isArray(data.works.operas)) counts.premieredRoleIn = data.works.operas.length;
          if (Array.isArray(data.works.books)) counts.authored = data.works.books.length;
          if (Array.isArray(data.works.editedBooks)) counts.edited = data.works.editedBooks.length;
        }

        const combinedWrote = [...composedOperas, ...wroteRelationships];
        if (combinedWrote.length > 0) {
          const uniqueWrote = new Set(
            combinedWrote
              .map(entry => {
                const candidate =
                  entry?.title ||
                  entry?.opera_name ||
                  entry?.name ||
                  entry?.operaTitle ||
                  entry?.label ||
                  entry?.display_name;
                if (candidate) {
                  return normalizeNodeId(candidate);
                }
                const derived = deriveOperaName(entry, '');
                return normalizeNodeId(derived);
              })
              .filter(Boolean)
          );
          counts.wrote = uniqueWrote.size || combinedWrote.length;
        }
        
        // Note: All people are persons in Neo4j regardless of activity (singer, composer, etc.)
        // The API should return comprehensive data for all persons through the network endpoint
      } else if (node.type === 'opera') {
        // Operas have people who premiered roles in them (incoming relationships)
        if (Array.isArray(data.premieredRoles)) counts.premieredRoleIn = data.premieredRoles.length;
        const composerRows = Array.isArray(data.wrote) ? data.wrote : [];
        if (composerRows.length > 0) {
          const uniqueComposers = new Set(
            composerRows
              .map(row => normalizeNodeId(row?.composer || row?.name || row?.full_name))
              .filter(Boolean)
          );
          counts.wrote = uniqueComposers.size || composerRows.length;
        } else if (data.opera && data.opera.composer) {
          counts.wrote = 1;
        }
      } else if (node.type === 'book') {
        if (data.book && data.book.author) counts.authored = 1;
      }

      // Cache the counts
      setActualCounts(prev => ({ ...prev, [node.id]: counts }));
      return counts;
    } catch (err) {
      // Only log unexpected errors, not 404s for non-existent nodes
      if (err.status !== 404) {
        console.error('Failed to fetch actual counts:', err);
      }
      // Re-throw to allow the caller to handle it
      throw err;
    }

    return {
      taughtBy: 0,
      taught: 0,
      authored: 0,
      premieredRoleIn: 0,
      wrote: 0,
      parent: 0,
      parentOf: 0,
      spouse: 0,
      grandparent: 0,
      grandparentOf: 0,
      sibling: 0,
      edited: 0,
      editedBy: 0
    };
  };

  // Helper function to get expandable relationship counts for a node
  const getExpandableRelationshipCounts = (node) => {
    // If we have actual counts from API, use those and subtract already visible relationships
    if (actualCounts[node.id]) {
      const apiCounts = actualCounts[node.id];
      const visibleCounts = getVisibleRelationshipCounts(node);
      
      return {
        taughtBy: Math.max(0, apiCounts.taughtBy - visibleCounts.taughtBy),
        taught: Math.max(0, apiCounts.taught - visibleCounts.taught),
        authored: Math.max(0, apiCounts.authored - visibleCounts.authored),
        premieredRoleIn: Math.max(0, apiCounts.premieredRoleIn - visibleCounts.premieredRoleIn),
        wrote: Math.max(0, apiCounts.wrote - visibleCounts.wrote),
        parent: Math.max(0, apiCounts.parent - visibleCounts.parent),
        parentOf: Math.max(0, apiCounts.parentOf - visibleCounts.parentOf),
        spouse: Math.max(0, apiCounts.spouse - visibleCounts.spouse),
        grandparent: Math.max(0, apiCounts.grandparent - visibleCounts.grandparent),
        grandparentOf: Math.max(0, apiCounts.grandparentOf - visibleCounts.grandparentOf),
        sibling: Math.max(0, apiCounts.sibling - visibleCounts.sibling),
        edited: Math.max(0, apiCounts.edited - visibleCounts.edited),
        editedBy: Math.max(0, apiCounts.editedBy - visibleCounts.editedBy)
      };
    }
    
    // Fallback: if no API counts yet, only show "All" option
    // Don't show misleading specific relationship options without accurate data
    return {
      taughtBy: 0,
      taught: 0,
      authored: 0,
      premieredRoleIn: 0,
      wrote: 0,
      parent: 0,
      parentOf: 0,
      spouse: 0,
      grandparent: 0,
      grandparentOf: 0,
      sibling: 0,
      edited: 0,
      editedBy: 0
    };
  };
  // Helper function to get visible relationship counts for a node (what's already in the network)
  const getVisibleRelationshipCounts = (node) => {
    const counts = {
      taughtBy: 0,
      taught: 0,
      authored: 0,
      premieredRoleIn: 0,
      wrote: 0,
      parent: 0,
      parentOf: 0,
      spouse: 0,
      grandparent: 0,
      grandparentOf: 0,
      sibling: 0,
      edited: 0,
      editedBy: 0
    };

    if (!networkData.links) return counts;

    // Count relationships based on links
    networkData.links.forEach(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source?.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target?.id;
      const isSource = sourceId === node.id;
      const isTarget = targetId === node.id;

      if (isSource) {
        switch (link.type) {
          case 'taught':
            counts.taught++;
            break;
          case 'authored':
            counts.authored++;
            break;
          case 'premiered':
            counts.premieredRoleIn++;
            break;
          case 'wrote':
            counts.wrote++;
            break;
          case 'composed':
            counts.wrote++;
            break;
          case 'family':
            // Parse family relationship types
            const label = link.label?.toLowerCase() || '';
            if (label.includes('parent')) counts.parent++;
            else if (label.includes('spouse')) counts.spouse++;
            else if (label.includes('grandparent')) counts.grandparent++;
            else if (label.includes('sibling')) counts.sibling++;
            break;
          case 'edited':
            counts.edited++;
            break;
        }
      }

      if (isTarget) {
        switch (link.type) {
          case 'taught':
            counts.taughtBy++;
            break;
          case 'authored':
            // Count books that were authored by others
            break;
          case 'premiered':
            // Count performers who premiered in this work (opera as target)
            counts.premieredRoleIn++;
            break;
          case 'wrote':
            counts.wrote++;
            break;
          case 'composed':
            counts.wrote++;
            break;
          case 'family':
            // Parse family relationship types for reverse
            const label = link.label?.toLowerCase() || '';
            if (label.includes('parent')) counts.parentOf++;
            else if (label.includes('spouse')) counts.spouse++;
            else if (label.includes('grandparent')) counts.grandparentOf++;
            else if (label.includes('sibling')) counts.sibling++;
            break;
          case 'edited':
            counts.editedBy++;
            break;
        }
      }
    });

    return counts;
  };
    const ContextMenu = React.memo(() => {
    const node = contextMenu.node;
    const [hoveredMenuIndex, setHoveredMenuIndex] = useState(null);
    useEffect(() => {
      if (!contextMenu.show) {
        setHoveredMenuIndex(null);
      }
    }, [contextMenu.show]);
    
    // Extract stable values
    const nodeId = node?.id;
    const nodeActualCount = actualCounts[nodeId];
    const nodesLength = networkData.nodes.length;
    const linksLength = networkData.links.length;
    
    // Check if node is alone (no other nodes or no relationships)
    const isNodeAlone = React.useMemo(() => {
      return nodesLength === 1 || linksLength === 0;
    }, [nodesLength, linksLength]);
    
    // Get expandable relationship counts (what can actually be expanded)
    const counts = React.useMemo(() => {
      if (!contextMenu.show || !node) return {};
      return getExpandableRelationshipCounts(node);
    }, [contextMenu.show, nodeId, nodeActualCount, linksLength, networkData.nodes.length]);
    
    // Calculate total relationships for "All" option
    const totalRelationships = React.useMemo(() => {
      const specificCounts = Object.values(counts).reduce((sum, count) => sum + (typeof count === 'number' ? count : 0), 0);
      
      // Show "All" if there are specific expandable relationships OR if we don't have API data yet
      const hasApiData = nodeActualCount;
      return specificCounts > 0 || !hasApiData ? 1 : 0;
    }, [counts, nodeActualCount]);
    
    const menuItems = React.useMemo(() => {
      if (!contextMenu.show || !node) return [];
      const hasApiData = !!nodeActualCount;
      const hasAnyExpandable = hasApiData
        ? Object.values(counts).some(v => (typeof v === 'number' ? v : 0) > 0)
        : true; // allow expand even before counts load
      return [
      {
        label: 'Full information',
        action: () => {
          showFullInformation(node);
          setContextMenu({ show: false, x: 0, y: 0, node: null });
        }
      },
      {
        label: 'Expand',
        disabled: !hasAnyExpandable,
        hasSubmenu: hasAnyExpandable,
        submenu: [
          {
            label: 'All',
            action: () => {
              expandAllRelationships(node);
              setContextMenu({ show: false, x: 0, y: 0, node: null });
            }
          },
          ...(node?.type === 'person' ? [
            ...(counts.taughtBy > 0 ? [{
              label: `<- Taught - (${counts.taughtBy} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'taughtBy');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.authored > 0 ? [{
              label: `- Authored -> (${counts.authored} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'authored');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.premieredRoleIn > 0 ? [{
              label: `- Premiered role in -> (${counts.premieredRoleIn} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'premieredRoleIn');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.taught > 0 ? [{
              label: `- Taught -> (${counts.taught} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'taught');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.parent > 0 ? [{
              label: `- Parent -> (${counts.parent} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'parent');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.parentOf > 0 ? [{
              label: `<- Parent - (${counts.parentOf} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'parentOf');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.spouse > 0 ? [{
              label: `- Spouse -> (${counts.spouse} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'spouse');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.spouse > 0 ? [{
              label: `<- Spouse - (${counts.spouse} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'spouseOf');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.grandparent > 0 ? [{
              label: `- Grandparent -> (${counts.grandparent} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'grandparent');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.grandparentOf > 0 ? [{
              label: `<- Grandparent - (${counts.grandparentOf} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'grandparentOf');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.sibling > 0 ? [{
              label: `- Sibling - (${counts.sibling} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'sibling');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : [])
          ] : []),
          ...(node?.type === 'opera' ? [
            ...(counts.premieredRoleIn > 0 ? [{
              label: `<- Premiered role in - (${counts.premieredRoleIn} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'premieredRoleIn');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.wrote > 0 ? [{
              label: `<- Wrote - (${counts.wrote} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'wrote');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : [])
          ] : []),
          ...(node?.type === 'book' ? [
            ...(counts.authored > 0 ? [{
              label: `Authored (${counts.authored} nodes)`,
              action: () => {
                // For books, authors are inbound; use authoredBy to make intent clear
                expandSpecificRelationship(node, 'authoredBy');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.editedBy > 0 ? [{
              label: `Edited (${counts.editedBy} nodes)`,
              action: () => {
                // Editors inbound to a book
                expandSpecificRelationship(node, 'editedBy');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : [])
          ] : [])
        ]
      },
      {
         label: 'Dismiss other nodes',
         action: () => {
           dismissOtherNodes(node);
           setContextMenu({ show: false, x: 0, y: 0, node: null });
         }
       },
       {
         label: 'Dismiss',
         action: () => {
           dismissNode(node);
           setContextMenu({ show: false, x: 0, y: 0, node: null });
         }
       }
     ];
   }, [nodeId, isNodeAlone, counts]);

    if (!contextMenu.show) return null;

    const activeSubmenuItem = (expandSubmenu != null && menuItems[expandSubmenu]?.hasSubmenu)
      ? { index: expandSubmenu, item: menuItems[expandSubmenu] }
      : null;

    const dismissMenu = () => {
      setContextMenu({ show: false, x: 0, y: 0, node: null });
      setExpandSubmenu(null);
      setHoveredMenuIndex(null);
    };

   return (
      <div
        className="context-menu"
        style={{
          position: 'absolute',
          top: contextMenu.y,
          left: contextMenu.x,
          backgroundColor: 'white',
          border: '2px solid #3e96e2',
          borderRadius: '8px',
          padding: '4px 0',
          boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
          zIndex: 1000,
          minWidth: '220px',
          maxWidth: '300px',
          fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
          fontSize: '16px'
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <button
          type="button"
          onClick={dismissMenu}
          aria-label="Close menu"
          style={{
            position: 'absolute',
            top: 6,
            right: 6,
            border: 'none',
            background: 'transparent',
            color: '#6b7280',
            fontSize: '18px',
            cursor: 'pointer',
            padding: 4,
            lineHeight: 1
          }}
        >
          ×
        </button>
        {/* Header */}
        <div style={{ 
          padding: '8px 12px 8px 12px', 
          fontWeight: '600', 
          borderBottom: '1px solid #e5e7eb',
          color: '#1f2937',
          fontSize: '13px',
          paddingRight: '36px'
        }}>
          {node?.name}
        </div>

        {/* Menu Items */}
        {menuItems.map((item, index) => (
          <div key={index} style={{ position: 'relative' }}>
            <div
              style={{
                padding: '8px 12px',
                cursor: item.disabled ? 'not-allowed' : 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                color: item.disabled ? '#9ca3af' : '#374151',
                transition: 'background-color 0.1s',
                backgroundColor:
                  (!item.disabled && hoveredMenuIndex === index) ||
                  (item.hasSubmenu && expandSubmenu === index)
                    ? '#f3f4f6'
                    : 'transparent'
              }}
              onMouseEnter={() => {
                setHoveredMenuIndex(index);
                if (item.hasSubmenu) {
                  if (submenuTimeoutRef.current) {
                    clearTimeout(submenuTimeoutRef.current);
                  }
                  setExpandSubmenu(index);
                }
              }}
              onMouseLeave={() => {
                setHoveredMenuIndex((current) => (current === index ? null : current));
                if (item.hasSubmenu) {
                  if (submenuTimeoutRef.current) {
                    clearTimeout(submenuTimeoutRef.current);
                  }
                  submenuTimeoutRef.current = setTimeout(() => {
                    setExpandSubmenu(null);
                  }, 300);
                }
              }}
              onTouchStart={() => {
                setHoveredMenuIndex(index);
                if (item.hasSubmenu) {
                  if (submenuTimeoutRef.current) {
                    clearTimeout(submenuTimeoutRef.current);
                  }
                  setExpandSubmenu(index);
                }
              }}
              onClick={() => {
                if (item.disabled) return;
                if (item.hasSubmenu) {
                  // Toggle submenu on click for touch/keyboard users
                  const willOpen = expandSubmenu !== index;
                  setExpandSubmenu(willOpen ? index : null);
                  setHoveredMenuIndex(willOpen ? index : null);
                  return;
                }
                if (typeof item.action === 'function') {
                  item.action();
                }
              }}
            >
              <span>{item.label}</span>
              {item.hasSubmenu && <span style={{ color: '#9ca3af' }}>▶</span>}
            </div>
          </div>
        ))}
        {activeSubmenuItem?.item?.submenu && (
          <div
            style={{
              position: 'absolute',
              top: '10px',
              left: '10px',
              backgroundColor: 'white',
              border: '2px solid #3e96e2',
              borderRadius: '8px',
              padding: '8px 0',
              boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
              minWidth: '280px',
              maxWidth: '400px',
              zIndex: 1001,
              fontSize: '16px'
            }}
            onMouseEnter={() => {
              if (submenuTimeoutRef.current) {
                clearTimeout(submenuTimeoutRef.current);
              }
              if (activeSubmenuItem) {
                setExpandSubmenu(activeSubmenuItem.index);
                setHoveredMenuIndex(activeSubmenuItem.index);
              }
            }}
            onMouseLeave={() => {
              if (submenuTimeoutRef.current) {
                clearTimeout(submenuTimeoutRef.current);
              }
              submenuTimeoutRef.current = setTimeout(() => {
                setExpandSubmenu(null);
                setHoveredMenuIndex(null);
              }, 300);
            }}
            onTouchStart={() => {
              if (submenuTimeoutRef.current) {
                clearTimeout(submenuTimeoutRef.current);
              }
              if (activeSubmenuItem) {
                setExpandSubmenu(activeSubmenuItem.index);
                setHoveredMenuIndex(activeSubmenuItem.index);
              }
            }}
            onClick={(e) => e.stopPropagation()}
          >
            {activeSubmenuItem.item.submenu.map((subItem, subIndex) => (
              <div
                key={subIndex}
                style={{
                  padding: '8px 12px',
                  cursor: 'pointer',
                  color: '#374151',
                  fontSize: '16px',
                  fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
                  whiteSpace: 'nowrap',
                  minHeight: '24px',
                  display: 'flex',
                  alignItems: 'center'
                }}
                onMouseEnter={(e) => {
                  if (e.currentTarget) e.currentTarget.style.backgroundColor = '#f3f4f6';
                }}
                onMouseLeave={(e) => {
                  if (e.currentTarget) e.currentTarget.style.backgroundColor = 'transparent';
                }}
                onClick={() => {
                  if (typeof subItem.action === 'function') subItem.action();
                }}
              >
                {subItem.label}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  });
  // Filter Panel Component
  const FilterPanel = () => {
    const { totalNodes, visibleNodes } = getFilterCounts();
    const { birthRange, deathRange } = getDateRanges();
    const hasVoiceFilters = selectedVoiceTypes.size > 0;
    const hasBirthFilter = birthRangeIsUserSet;
    const hasDeathFilter = deathRangeIsUserSet;
    const hasBirthplaceFilters = selectedBirthplaces.size > 0;
    const hasAnyFilters = hasVoiceFilters || hasBirthFilter || hasDeathFilter || hasBirthplaceFilters;

    // Local input state to prevent re-renders during typing
    const [birthMinInput, setBirthMinInput] = useState('');
    const [birthMaxInput, setBirthMaxInput] = useState('');
    const [deathMinInput, setDeathMinInput] = useState('');
    const [deathMaxInput, setDeathMaxInput] = useState('');
    const contentRef = useRef(null);
    const isVoiceOpen = filterSectionsOpen.voice;
    const isBirthOpen = filterSectionsOpen.birth;
    const isDeathOpen = filterSectionsOpen.death;
    const isBirthplacesOpen = filterSectionsOpen.birthplaces;

    useLayoutEffect(() => {
      // Sync inputs when ranges or panel visibility changes
      if (birthRangeIsUserSet) {
        setBirthMinInput(String(birthYearRange[0]));
        setBirthMaxInput(String(birthYearRange[1]));
      } else {
        setBirthMinInput('');
        setBirthMaxInput('');
      }
      if (deathRangeIsUserSet) {
        setDeathMinInput(String(deathYearRange[0]));
        setDeathMaxInput(String(deathYearRange[1]));
      } else {
        setDeathMinInput('');
        setDeathMaxInput('');
      }
    }, [birthYearRange, deathYearRange, birthRangeIsUserSet, deathRangeIsUserSet, showFilterPanel]);

    const clamp = (value, min, max) => Math.max(min, Math.min(value, max));

    const applyBirthRange = () => {
      const scrollEl = contentRef.current;
      const prevTop = scrollEl ? scrollEl.scrollTop : null;
      const winY = typeof window !== 'undefined' ? window.scrollY : null;
      const ranges = getDateRanges();
      const dataBirthRange = Array.isArray(ranges.birthRange) ? ranges.birthRange : DEFAULT_BIRTH_RANGE;
      const minBound = Math.min(DEFAULT_BIRTH_RANGE[0], dataBirthRange[0] ?? DEFAULT_BIRTH_RANGE[0]);
      const maxBound = Math.max(DEFAULT_BIRTH_RANGE[1], dataBirthRange[1] ?? DEFAULT_BIRTH_RANGE[1]);
      const hasMin = typeof birthMinInput === 'string' && birthMinInput.trim() !== '';
      const hasMax = typeof birthMaxInput === 'string' && birthMaxInput.trim() !== '';

      if (!hasMin && !hasMax) {
        birthRangeIsUserSetRef.current = false;
        setBirthRangeIsUserSet(false);
        updateBirthYearRange([...DEFAULT_BIRTH_RANGE], { userInitiated: false });
        setBirthMinInput('');
        setBirthMaxInput('');
        if (prevTop !== null && scrollEl) {
          requestAnimationFrame(() => {
            scrollEl.scrollTop = prevTop;
            if (winY !== null) window.scrollTo(0, winY);
            requestAnimationFrame(() => {
              scrollEl.scrollTop = prevTop;
              if (winY !== null) window.scrollTo(0, winY);
            });
          });
        }
        return;
      }

      const parsedMin = parseInt(birthMinInput, 10);
      const parsedMax = parseInt(birthMaxInput, 10);
      let nextMin = hasMin
        ? (Number.isNaN(parsedMin) ? birthYearRange[0] : clamp(parsedMin, minBound, maxBound))
        : birthYearRange[0];
      let nextMax = hasMax
        ? (Number.isNaN(parsedMax) ? birthYearRange[1] : clamp(parsedMax, minBound, maxBound))
        : birthYearRange[1];
      if (nextMax < nextMin) nextMax = nextMin;
      updateBirthYearRange([nextMin, nextMax], { userInitiated: true });
      setBirthMinInput(hasMin ? String(nextMin) : '');
      setBirthMaxInput(hasMax ? String(nextMax) : '');
      if (prevTop !== null && scrollEl) {
        requestAnimationFrame(() => {
          scrollEl.scrollTop = prevTop;
          if (winY !== null) window.scrollTo(0, winY);
          requestAnimationFrame(() => {
            scrollEl.scrollTop = prevTop;
            if (winY !== null) window.scrollTo(0, winY);
          });
        });
      }
    };

    const applyDeathRange = () => {
      const scrollEl = contentRef.current;
      const prevTop = scrollEl ? scrollEl.scrollTop : null;
      const winY = typeof window !== 'undefined' ? window.scrollY : null;
      const ranges = getDateRanges();
      const dataDeathRange = Array.isArray(ranges.deathRange) ? ranges.deathRange : DEFAULT_DEATH_RANGE;
      const minBound = Math.min(DEFAULT_DEATH_RANGE[0], dataDeathRange[0] ?? DEFAULT_DEATH_RANGE[0]);
      const maxBound = Math.max(DEFAULT_DEATH_RANGE[1], dataDeathRange[1] ?? DEFAULT_DEATH_RANGE[1]);
      const hasMin = typeof deathMinInput === 'string' && deathMinInput.trim() !== '';
      const hasMax = typeof deathMaxInput === 'string' && deathMaxInput.trim() !== '';

      if (!hasMin && !hasMax) {
        deathRangeIsUserSetRef.current = false;
        setDeathRangeIsUserSet(false);
        updateDeathYearRange([...DEFAULT_DEATH_RANGE], { userInitiated: false });
        setDeathMinInput('');
        setDeathMaxInput('');
        if (prevTop !== null && scrollEl) {
          requestAnimationFrame(() => {
            scrollEl.scrollTop = prevTop;
            if (winY !== null) window.scrollTo(0, winY);
            requestAnimationFrame(() => {
              scrollEl.scrollTop = prevTop;
              if (winY !== null) window.scrollTo(0, winY);
            });
          });
        }
        return;
      }

      const parsedMin = parseInt(deathMinInput, 10);
      const parsedMax = parseInt(deathMaxInput, 10);
      let nextMin = hasMin
        ? (Number.isNaN(parsedMin) ? deathYearRange[0] : clamp(parsedMin, minBound, maxBound))
        : deathYearRange[0];
      let nextMax = hasMax
        ? (Number.isNaN(parsedMax) ? deathYearRange[1] : clamp(parsedMax, minBound, maxBound))
        : deathYearRange[1];
      if (nextMax < nextMin) nextMax = nextMin;
      updateDeathYearRange([nextMin, nextMax], { userInitiated: true });
      setDeathMinInput(hasMin ? String(nextMin) : '');
      setDeathMaxInput(hasMax ? String(nextMax) : '');
      if (prevTop !== null && scrollEl) {
        requestAnimationFrame(() => {
          scrollEl.scrollTop = prevTop;
          if (winY !== null) window.scrollTo(0, winY);
          requestAnimationFrame(() => {
            scrollEl.scrollTop = prevTop;
            if (winY !== null) window.scrollTo(0, winY);
          });
        });
      }
    };

    const applyAllFilters = () => {
      applyBirthRange();
      applyDeathRange();
      setFiltersVersion(v => v + 1);
    };

    return (
      <>
        {/* Overlay */}
        <div
          className={`mobile-overlay-backdrop${showFilterPanel ? ' is-open' : ''}`}
          onClick={() => setShowFilterPanel(false)}
        />

        {/* Filter Panel */}
        <div
          className={isMobileViewport ? `mobile-sheet${showFilterPanel ? ' is-open' : ''}` : undefined}
          style={isMobileViewport ? undefined : {
            position: 'fixed',
            top: 0,
            left: showFilterPanel ? 0 : -350,
            width: '350px',
            height: '100vh',
            backgroundColor: 'white',
            boxShadow: '2px 0 10px rgba(0, 0, 0, 0.1)',
            zIndex: 1000,
            transition: 'left 0.3s ease',
            display: 'flex',
            flexDirection: 'column',
            pointerEvents: showFilterPanel ? 'auto' : 'none'
          }}
          role="dialog"
          aria-modal="true"
        >
          {/* Header */}
          <div
            className={isMobileViewport ? 'mobile-sheet__header' : undefined}
            style={isMobileViewport ? undefined : {
              padding: '20px',
              borderBottom: '1px solid #e5e7eb',
              backgroundColor: '#f8f9fa'
            }}
          >
            {isMobileViewport && <div className="mobile-sheet__handle" />}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3 style={{ margin: 0, fontSize: isMobileViewport ? '16px' : '18px', fontWeight: '600', color: '#1f2937' }}>
                Filters
              </h3>
              <button
                onClick={() => setShowFilterPanel(false)}
                style={{
                  background: 'none',
                  border: 'none',
                  fontSize: isMobileViewport ? '24px' : '24px',
                  cursor: 'pointer',
                  color: '#6b7280',
                  padding: isMobileViewport ? '2px' : '0',
                  width: isMobileViewport ? '32px' : '32px',
                  height: isMobileViewport ? '32px' : '32px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center'
                }}
              >
                ×
              </button>
            </div>
            
            {/* Clear/Apply Buttons */}
            <div
              className={isMobileViewport ? 'mobile-sheet__footer' : undefined}
              style={isMobileViewport ? undefined : {
                display: 'flex',
                gap: '8px',
                marginTop: '12px'
              }}
            >
              {hasAnyFilters && (
                <button
                  onClick={clearAllFilters}
                  style={{
                    background: 'none',
                    border: '1px solid #dc2626',
                    color: '#dc2626',
                    padding: isMobileViewport ? '8px 12px' : '6px 12px',
                    borderRadius: '8px',
                    fontSize: isMobileViewport ? '14px' : '16px',
                    cursor: 'pointer'
                  }}
                >
                  Clear All Filters
                </button>
              )}
              <button
                onClick={applyAllFilters}
                style={{
                  backgroundColor: '#2563eb',
                  color: 'white',
                  border: '2px solid #3e96e2',
                  padding: isMobileViewport ? '8px 12px' : '6px 12px',
                  borderRadius: '8px',
                  fontSize: isMobileViewport ? '14px' : '16px',
                  cursor: 'pointer'
                }}
              >
                Apply Filters
              </button>
            </div>
            
            {/* Filter Count Display */}
            {totalNodes > 0 && (
              <div style={{
                marginTop: hasAnyFilters ? (isMobileViewport ? '6px' : '8px') : (isMobileViewport ? '8px' : '12px'),
                padding: isMobileViewport ? '8px 10px' : '8px 12px',
                backgroundColor: hasAnyFilters ? '#f0f9ff' : '#f9fafb',
                border: hasAnyFilters ? '1px solid #0ea5e9' : '1px solid #e5e7eb',
                borderRadius: '6px',
                fontSize: isMobileViewport ? '14px' : '16px',
                color: hasAnyFilters ? '#0c4a6e' : '#374151'
              }}>
                {hasAnyFilters ? (
                  <>
                    <strong>{visibleNodes}</strong> of <strong>{totalNodes}</strong> nodes match current filters
                    {visibleNodes !== totalNodes && (
                      <div style={{ fontSize: '12px', opacity: 0.8, marginTop: '2px' }}>
                        {totalNodes - visibleNodes} nodes filtered out
                      </div>
                    )}
                  </>
                ) : (
                  <>Showing all <strong>{totalNodes}</strong> nodes</>
                )}
              </div>
            )}
          </div>

          {/* Filter Content */}
          <div 
            className={isMobileViewport ? 'mobile-sheet__content' : undefined}
            style={isMobileViewport ? undefined : {
              flex: 1,
              overflowY: 'auto',
              padding: '20px',
              overflowAnchor: 'none'
            }}
            ref={contentRef}
          >
            {/* Voice Type Section */}
            <div style={{ marginBottom: isMobileViewport ? '16px' : '24px' }}>
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  setFilterSectionsOpen(prev => ({ ...prev, voice: !prev.voice }));
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                    setFilterSectionsOpen(prev => ({ ...prev, voice: !prev.voice }));
                  }
                }}
                style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '8px 0', gap: '10px', background: 'none', border: 'none', width: '100%', textAlign: 'left' }}
              >
                <span style={{ color: '#374151', fontSize: '22px', lineHeight: 1 }}>{isVoiceOpen ? '▾' : '▸'}</span>
                <h4 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#374151' }}>Voice Type</h4>
              </button>
              {isVoiceOpen && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  {getVisibleVoiceTypes().map(voiceType => (
                    <label
                      key={voiceType.name}
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        cursor: 'pointer',
                        padding: '6px 8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    >
                      <input
                        type="checkbox"
                        checked={selectedVoiceTypes.has(voiceType.name)}
                        onChange={() => toggleVoiceTypeFilter(voiceType.name)}
                        style={{
                          marginRight: '10px',
                          width: '14px',
                          height: '14px',
                          accentColor: voiceType.color
                        }}
                      />
                      <div
                        style={{
                          width: '14px',
                          height: '14px',
                          backgroundColor: voiceType.color,
                          borderRadius: '50%',
                          marginRight: '10px',
                          border: '2px solid #3e96e2',
                          boxShadow: '0 0 0 1px rgba(0,0,0,0.1)'
                        }}
                      />
                      <span style={{
                        fontSize: '13px',
                        color: '#374151',
                        fontWeight: selectedVoiceTypes.has(voiceType.name) ? '600' : '400'
                      }}>
                        {voiceType.name} ({voiceType.count})
                      </span>
                    </label>
                  ))}
                </div>
              )}
            </div>

            {/* Birthplace Section */}
            <div style={{ marginBottom: '24px' }}>
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  setFilterSectionsOpen(prev => ({ ...prev, birthplaces: !prev.birthplaces }));
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                    setFilterSectionsOpen(prev => ({ ...prev, birthplaces: !prev.birthplaces }));
                  }
                }}
                style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '8px 0', gap: '10px', background: 'none', border: 'none', width: '100%', textAlign: 'left' }}
              >
                <span style={{ color: '#374151', fontSize: '22px', lineHeight: 1 }}>{isBirthplacesOpen ? '▾' : '▸'}</span>
                <h4 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#374151' }}>Birthplace</h4>
              </button>
              {isBirthplacesOpen && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  {getVisibleBirthplaces().map(bp => (
                    <label key={bp.name} style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer' }}>
                      <input
                        type="checkbox"
                        checked={selectedBirthplaces.has(normalizePlaceName(bp.name))}
                        onChange={() => toggleBirthplaceFilter(bp.name)}
                      />
                      <span style={{ fontSize: '13px', color: '#374151' }}>{bp.name} ({bp.count})</span>
                    </label>
                  ))}
                  {getVisibleBirthplaces().length === 0 && (
                    <div style={{ fontSize: '12px', color: '#6b7280' }}>No birthplaces in current view</div>
                  )}
                </div>
              )}
            </div>

            {/* Birth Year Range Section */}
            <div style={{ marginBottom: '24px' }}>
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  setFilterSectionsOpen(prev => ({ ...prev, birth: !prev.birth }));
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                    setFilterSectionsOpen(prev => ({ ...prev, birth: !prev.birth }));
                  }
                }}
                style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '8px 0', gap: '10px', background: 'none', border: 'none', width: '100%', textAlign: 'left' }}
              >
                <span style={{ color: '#374151', fontSize: '22px', lineHeight: 1 }}>{isBirthOpen ? '▾' : '▸'}</span>
                <h4 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#374151' }}>Birth Year Range</h4>
              </button>
              {isBirthOpen && (
              <div style={{ display: 'flex', gap: '8px' }}>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>From</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    placeholder="yyyy"
                    value={birthMinInput}
                    onChange={(e) => setBirthMinInput(e.target.value)}
                    onBlur={applyBirthRange}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                        applyBirthRange();
                      }
                    }}
                    onMouseDown={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onTouchStart={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onFocus={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    style={{ width: '100%', padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: '8px' }}
                  />
                </div>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>To</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    placeholder="yyyy"
                    value={birthMaxInput}
                    onChange={(e) => setBirthMaxInput(e.target.value)}
                    onBlur={applyBirthRange}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                        applyBirthRange();
                      }
                    }}
                    onMouseDown={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onTouchStart={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onFocus={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    style={{ width: '100%', padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: '8px' }}
                  />
                </div>
              </div>
              )}
            </div>
            {/* Death Year Range Section */}
            <div style={{ marginBottom: '24px' }}>
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  setFilterSectionsOpen(prev => ({ ...prev, death: !prev.death }));
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                    setFilterSectionsOpen(prev => ({ ...prev, death: !prev.death }));
                  }
                }}
                style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '8px 0', gap: '10px', background: 'none', border: 'none', width: '100%', textAlign: 'left' }}
              >
                <span style={{ color: '#374151', fontSize: '22px', lineHeight: 1 }}>{isDeathOpen ? '▾' : '▸'}</span>
                <h4 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#374151' }}>Death Year Range</h4>
              </button>
              {isDeathOpen && (
              <div style={{ display: 'flex', gap: '8px' }}>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>From</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    placeholder="yyyy"
                    value={deathMinInput}
                    onChange={(e) => setDeathMinInput(e.target.value)}
                    onBlur={applyDeathRange}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                        applyDeathRange();
                      }
                    }}
                    onMouseDown={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onTouchStart={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onFocus={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    style={{ width: '100%', padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: '8px' }}
                  />
                </div>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>To</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    placeholder="yyyy"
                    value={deathMaxInput}
                    onChange={(e) => setDeathMaxInput(e.target.value)}
                    onBlur={applyDeathRange}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                        applyDeathRange();
                      }
                    }}
                    onMouseDown={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onTouchStart={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onFocus={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    style={{ width: '100%', padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: '8px' }}
                  />
                </div>
              </div>
              )}
            </div>
          </div>
        </div>
      </>
    );
  };
  const ProfileCard = () => {
    if (!profileCard.show || !profileCard.data) return null;

    const data = profileCard.data;
    const isSinger = !!data?.full_name;
    const isOpera = !!data?.opera_name;
    const isBook = !!data?.title && !isSinger && !isOpera;

    const cardStyle = isMobileViewport ? {
      position: 'fixed',
      left: '50%',
      bottom: `calc(${showPathPanel ? '190px' : '120px'} + var(--cmg-mobile-block-padding-end))`,
      transform: 'translateX(-50%)',
      zIndex: 1000,
      fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif"
    } : {
      position: 'absolute',
      bottom: '20px',
      left: '20px',
      width: '300px',
      maxHeight: '400px',
      backgroundColor: 'white',
      borderRadius: '8px',
      boxShadow: '0 8px 32px rgba(0,0,0,0.15)',
      border: '2px solid #3e96e2',
      zIndex: 1000,
      fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
      overflow: 'hidden'
    };

    const cardClassName = isMobileViewport ? 'mobile-profile-card' : undefined;

    const contentStyle = isMobileViewport ? {
      padding: '20px 24px',
      maxHeight: 'calc(70vh - 72px)',
      overflowY: 'auto'
    } : {
      padding: '16px',
      maxHeight: '340px',
      overflowY: 'auto'
    };

    return (
      <div className={cardClassName} style={cardStyle}>
        {/* Header */}
        <div style={{
          padding: isMobileViewport ? '18px 24px 14px' : '16px',
          backgroundColor: '#f9fafb',
          borderBottom: '1px solid #e5e7eb',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          position: isMobileViewport ? 'relative' : undefined
        }}>
          {isMobileViewport && (
            <div style={{
              position: 'absolute',
              left: '50%',
              top: 10,
              transform: 'translateX(-50%)',
              width: 48,
              height: 4,
              borderRadius: 9999,
              backgroundColor: '#d1d5db'
            }} />
          )}
          <h3 style={{
            margin: 0,
            fontSize: isMobileViewport ? '18px' : '16px',
            fontWeight: '600',
            color: '#1f2937'
          }}>
            {isSinger ? '👤 Singer Profile' : isOpera ? '🎵 Opera Profile' : isBook ? '📚 Book Profile' : 'Profile'}
          </h3>
          <button
            onClick={() => setProfileCard({ show: false, data: null })}
            style={{
              background: 'none',
              border: 'none',
              fontSize: isMobileViewport ? '24px' : '18px',
              cursor: 'pointer',
              color: '#6b7280',
              padding: isMobileViewport ? '4px' : '0',
              width: isMobileViewport ? '40px' : '24px',
              height: isMobileViewport ? '40px' : '24px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}
          >
            ×
          </button>
        </div>

        {/* Content */}
        <div style={contentStyle}>
          {isSinger && (
            <>
          <div style={{ marginBottom: '12px' }}>
            <strong style={{ color: '#1f2937' }}>Name:</strong>
            <div style={{ color: '#374151', marginTop: '2px' }}>
              {data.full_name}
            </div>
          </div>

          {data.voice_type && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Voice type:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.voice_type}
              </div>
            </div>
          )}

          {(data.birth_year || data.death_year || data.birth || data.death) && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Dates:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.birth_year && data.death_year
                  ? `${data.birth_year}-${data.death_year}`
                  : data.birth_year
                  ? `${data.birth_year}-`
                  : data.death_year
                  ? `-${data.death_year}`
                  : (data.birth && data.death)
                  ? `${data.birth.low}-${data.death.low}`
                  : data.birth
                  ? `${data.birth.low}-`
                  : data.death
                  ? `-${data.death.low}`
                  : ''}
              </div>
            </div>
          )}

          {(data.birthplace || data.citizen) && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Birthplace:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                    {data.birthplace || data.citizen}
              </div>
            </div>
          )}

          {(data.spotify_link || data.youtube_search) && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Spotify:</strong>
              <div style={{ marginTop: '2px' }}>
                <a
                  href={data.spotify_link || data.youtube_search}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ color: '#2563eb', textDecoration: 'underline', fontSize: '16px', overflowWrap: 'anywhere', wordBreak: 'break-word', display: 'inline-block' }}
                  onMouseOver={(e) => (e.target.style.color = '#1d4ed8')}
                  onMouseOut={(e) => (e.target.style.color = '#2563eb')}
                >
                  {data.spotify_link || data.youtube_search}
                </a>
              </div>
            </div>
          )}

          {data.underrepresented_group && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Underrepresented group:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.underrepresented_group}
              </div>
            </div>
          )}

          {data.roles && Array.isArray(data.roles) && data.roles.length > 0 && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Roles premiered:</strong>
              <ul style={{ marginTop: '6px', paddingLeft: '18px', color: '#374151' }}>
                {data.roles.map((r, i) => (
                  <li key={i}>{r}</li>
                ))}
              </ul>
            </div>
          )}

              <div style={{ marginTop: '16px', paddingTop: '16px', borderTop: '1px solid #e5e7eb' }}>
                <strong style={{ color: '#1f2937', fontSize: '16px' }}>Sources:</strong>
            <div style={{ marginTop: '8px' }}>
              {(data.spelling_source_text || data.spelling_source_url || data.spelling_source) && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                  <strong>Spelling:</strong>{' '}
                  {renderRelationshipSourceLink(
                    data.spelling_source_text,
                    data.spelling_source_url,
                    data.spelling_source
                  )}
                </div>
              )}
              {(data.voice_type_source_text || data.voice_type_source_url || data.voice_type_source) && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                  <strong>Voice type:</strong>{' '}
                  {renderRelationshipSourceLink(
                    data.voice_type_source_text,
                    data.voice_type_source_url,
                    data.voice_type_source
                  )}
                </div>
              )}
              {(data.dates_source_text || data.dates_source_url || data.dates_source) && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                  <strong>Dates:</strong>{' '}
                  {renderRelationshipSourceLink(
                    data.dates_source_text,
                    data.dates_source_url,
                    data.dates_source
                  )}
                </div>
              )}
                  {(data.birthplace_source_text || data.birthplace_source_url || data.birthplace_source) && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                      <strong>Birthplace:</strong>{' '}
                      {renderRelationshipSourceLink(
                        data.birthplace_source_text,
                        data.birthplace_source_url,
                        data.birthplace_source
                      )}
                </div>
              )}
                  {(data.underrepresented_source_text || data.underrepresented_source_url || data.underrepresented_source) && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                      <strong>Underrepresented group:</strong>{' '}
                      {renderRelationshipSourceLink(
                        data.underrepresented_source_text,
                        data.underrepresented_source_url,
                        data.underrepresented_source
                      )}
                </div>
              )}
            </div>
          </div>
            </>
          )}

          {isOpera && (
            <>
              <div style={{ marginBottom: '12px' }}>
                <strong style={{ color: '#1f2937' }}>Title:</strong>
                <div style={{ color: '#374151', marginTop: '2px' }}>
                  {data.opera_name}
                </div>
              </div>
              {data.composer && (
                <div style={{ marginBottom: '12px' }}>
                  <strong style={{ color: '#1f2937' }}>Composer:</strong>
                  <div style={{ color: '#374151', marginTop: '2px' }}>
                    {data.composer}
                  </div>
                </div>
              )}
            </>
          )}

          {isBook && (
            <>
              <div style={{ marginBottom: '12px' }}>
                <strong style={{ color: '#1f2937' }}>Title:</strong>
                <div style={{ color: '#374151', marginTop: '2px' }}>
                  {data.title}
                </div>
              </div>
              {data.type && (
                <div style={{ marginBottom: '12px' }}>
                  <strong style={{ color: '#1f2937' }}>Type:</strong>
                  <div style={{ color: '#374151', marginTop: '2px' }}>
                    {data.type}
                  </div>
                </div>
              )}
              {data.link && (
                <div style={{ marginBottom: '12px' }}>
                  <strong style={{ color: '#1f2937' }}>Link:</strong>
                  <a href={data.link} target="_blank" rel="noopener noreferrer" style={{ color: '#059669', textDecoration: 'underline', marginLeft: '4px' }}>
                    View Book
                  </a>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    );
  };
  const AuthForm = ({ initialResetToken = '' }) => {
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [showPassword, setShowPassword] = useState(false);
    const [showTerms, setShowTerms] = useState(false);
    const [termsChecked, setTermsChecked] = useState(false);
    const [isForgotPassword, setIsForgotPassword] = useState(false);
    const [forgotEmail, setForgotEmail] = useState('');
    const [forgotLoading, setForgotLoading] = useState(false);
    const [forgotMessage, setForgotMessage] = useState('');
    const [forgotError, setForgotError] = useState('');
    const [showResetPasswordModal, setShowResetPasswordModal] = useState(Boolean(initialResetToken));
    const [resetTokenValue, setResetTokenValue] = useState(initialResetToken || '');
    const [resetPasswordValue, setResetPasswordValue] = useState('');
    const [resetPasswordConfirm, setResetPasswordConfirm] = useState('');
    const [resetStatus, setResetStatus] = useState({ loading: false, message: '', error: '' });
    const [tosAcceptError, setTosAcceptError] = useState('');
    const [acceptingTos, setAcceptingTos] = useState(false);

    useEffect(() => {
      try {
        const accepted = localStorage.getItem('tosAccepted') === '1';
        if (accepted) setTermsChecked(true);
      } catch (_) {}
    }, []);

    useEffect(() => {
      if (pendingTosToken) {
        setTosAcceptError('');
        setShowTerms(true);
      }
    }, [pendingTosToken]);

    const isTosAcceptanceRequired = Boolean(pendingTosToken);

    useEffect(() => {
      try {
        if (typeof window === 'undefined') return;

        const params = new URLSearchParams(window.location.search);
        const searchToken = params.get('resetToken') || params.get('token') || '';
        params.delete('resetToken');
        params.delete('token');
        const cleanedQuery = params.toString();

        const hash = window.location.hash || '';
        let hashToken = '';
        let cleanedHash = hash;
        if (hash.startsWith('#')) {
          const hashParams = new URLSearchParams(hash.slice(1));
          hashToken = hashParams.get('resetToken') || hashParams.get('token') || '';
          hashParams.delete('resetToken');
          hashParams.delete('token');
          const hashString = hashParams.toString();
          cleanedHash = hashString ? `#${hashString}` : '';
        }

        const rawPathSegments = window.location.pathname.split('/').filter(Boolean);
        const normalizedSegments = rawPathSegments.map((segment) => segment.toLowerCase());
        const resetSegmentIndex = normalizedSegments.findIndex(
          (segment) => segment === 'reset-password' || segment === 'forgot-password'
        );
        let pathToken = '';
        if (resetSegmentIndex !== -1 && rawPathSegments.length > resetSegmentIndex + 1) {
          pathToken = decodeURIComponent(rawPathSegments[resetSegmentIndex + 1] || '');
        }

        const resolvedToken = initialResetToken || searchToken || hashToken || pathToken;
        const hasResetPath = resetSegmentIndex !== -1;
        const isForgotPath = hasResetPath && normalizedSegments[resetSegmentIndex] === 'forgot-password';

        if (resolvedToken) {
          setResetTokenValue(resolvedToken);
          setShowResetPasswordModal(true);
        } else if (hasResetPath && !isForgotPath) {
          // Surface the reset modal even if we only have the path hint.
          setShowResetPasswordModal(true);
        }

        if (isForgotPath && !resolvedToken) {
          setIsForgotPassword(true);
        }

        let cleanedPath = window.location.pathname;
        if (hasResetPath) {
          const trimmedSegments = rawPathSegments.slice(0, resetSegmentIndex + 1);
          cleanedPath = `/${trimmedSegments.join('/')}`;
        }

        const newUrl = `${cleanedPath}${cleanedQuery ? `?${cleanedQuery}` : ''}${cleanedHash}`;
        const currentRelative = `${window.location.pathname}${window.location.search}${window.location.hash}`;
        if (newUrl !== currentRelative) {
          window.history.replaceState({}, document.title, newUrl);
        }
      } catch (_) {}
    }, [initialResetToken]);

    const handleCloseTerms = () => {
      setShowTerms(false);
      setTosAcceptError('');
      setAcceptingTos(false);
      if (pendingTosToken) {
        setPendingTosToken('');
        setPendingTosEmail('');
        setPendingTosRedirect('');
      }
    };

    const handleDisclaimerAgree = async () => {
      if (pendingTosToken) {
        setTosAcceptError('');
        setAcceptingTos(true);
        try {
          const response = await fetch(`${API_BASE}/auth/accept-tos`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ pendingToken: pendingTosToken })
          });
          const rateInfo = handleRateLimitResponse(response);
          if (rateInfo) {
            throw new Error(rateInfo.message);
          }
          const data = await response.json().catch(() => ({}));
          if (!response.ok) {
            throw new Error(data.error || 'Failed to record acknowledgement.');
          }
          const acceptedEmail = typeof data.email === 'string' ? data.email.trim() : '';
          if (acceptedEmail) {
            setEmail(acceptedEmail);
            setForgotEmail(acceptedEmail);
            try { setUserEmail(acceptedEmail); } catch (_) {}
            try { localStorage.setItem('userEmail', acceptedEmail); } catch (_) {}
          }
          const authToken = typeof data.token === 'string' ? data.token : '';
          if (authToken) {
            setToken(authToken);
            setJustLoggedIn(true);
            try { localStorage.setItem('token', authToken); } catch (_) {}
            try { localStorage.setItem(TOKEN_LOGIN_TS_KEY, String(Date.now())); } catch (_) {}
            setError('');
          }
          setPendingTosToken('');
          setPendingTosEmail('');
          setTosAcceptError('');
          setShowTerms(false);
          setAcceptingTos(false);
          try { localStorage.setItem('tosAccepted', '1'); } catch (_) {}
          setTermsChecked(true);

          // No page redirect; keep user in app after acceptance
          setPendingTosRedirect('');
        } catch (err) {
          setAcceptingTos(false);
          setTosAcceptError(err?.message || 'Failed to record acknowledgement.');
        }
        return;
      }

      try { localStorage.setItem('tosAccepted', '1'); } catch (_) {}
      setTermsChecked(true);
      setShowTerms(false);
    };

    const computeRedirectTarget = () => {
      if (typeof window === 'undefined' || !window?.location) {
        return 'https://theaspengrove.org/';
      }
      const origin = window.location.origin || '';
      if (!origin || origin === 'null') {
        return 'https://theaspengrove.org/';
      }
      if (/theaspengrove\.org$/i.test(origin.replace(/^https?:\/\//, ''))) {
        return `${origin.replace(/\/$/, '')}/`;
      }
      return `${origin.replace(/\/$/, '')}/`;
    };


    // Local registration is removed. Use the Sign in button to go through Auth0.

    const handleForgotSubmit = async () => {
      const targetEmail = (forgotEmail || email || '').trim();
      if (!targetEmail) {
        setForgotError('Please enter the email associated with your account.');
        return;
      }
      setForgotEmail(targetEmail);
      setForgotLoading(true);
      setForgotMessage('');
      setForgotError('');
    try {
      const response = await fetch(`${API_BASE}/auth/forgot-password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: targetEmail })
      });
      const rateInfo = handleRateLimitResponse(response);
      if (rateInfo) {
        throw new Error(rateInfo.message);
      }
      const data = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(data.error || 'Unable to send reset email. Please try again.');
        }
        setForgotMessage('If an account exists for that email, a reset link has been sent. Please check your spam folder before requesting an additional password reset.');
      } catch (err) {
        setForgotError(err?.message || 'Failed to send reset email.');
      } finally {
        setForgotLoading(false);
      }
    };

    const handleResetSubmit = async () => {
      if (!resetTokenValue.trim()) {
        setResetStatus({ loading: false, message: '', error: 'Reset code is required.' });
        return;
      }
      if (!resetPasswordValue || resetPasswordValue.length < 8) {
        setResetStatus({ loading: false, message: '', error: 'Password must be at least 8 characters.' });
        return;
      }
      if (resetPasswordValue !== resetPasswordConfirm) {
        setResetStatus({ loading: false, message: '', error: 'Passwords do not match.' });
        return;
      }
      setResetStatus({ loading: true, message: '', error: '' });
    try {
      const response = await fetch(`${API_BASE}/auth/reset-password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: resetTokenValue.trim(), password: resetPasswordValue })
      });
      const rateInfo = handleRateLimitResponse(response);
      if (rateInfo) {
        throw new Error(rateInfo.message);
      }
      const data = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(data.error || 'Unable to reset password. Please try again.');
        }

        const nextEmail = typeof data.email === 'string' ? data.email.trim() : '';
        if (nextEmail) {
          setEmail(nextEmail);
          setForgotEmail(nextEmail);
        }

        const nextToken = typeof data.token === 'string' ? data.token : '';
        const pendingTokenValue = typeof data.pendingToken === 'string' ? data.pendingToken : '';
        const requiresTos = Boolean(data.requiresTos && pendingTokenValue);

        setIsForgotPassword(false);
        setResetPasswordValue('');
        setResetPasswordConfirm('');
        setResetTokenValue('');

        if (requiresTos) {
          setResetStatus({
            loading: false,
            message: 'Password updated. Please review the disclaimer to finish signing in.',
            error: ''
          });
          setPendingTosToken(pendingTokenValue);
          setPendingTosEmail(nextEmail || email || '');
          setPendingTosRedirect(computeRedirectTarget());
          setShowResetPasswordModal(false);
          setShowTerms(true);
          return;
        }

        if (!nextToken) {
          setResetStatus({
            loading: false,
            message: 'Password updated. Please sign in with your new password.',
            error: ''
          });
          return;
        }

        setToken(nextToken);
        const resolvedEmail = (nextEmail || email || '').trim();
        try { setUserEmail(resolvedEmail); } catch (_) {}
        setJustLoggedIn(true);
        try { localStorage.setItem('token', nextToken); } catch (_) {}
        try { localStorage.setItem(TOKEN_LOGIN_TS_KEY, String(Date.now())); } catch (_) {}
        try { if (resolvedEmail) localStorage.setItem('userEmail', resolvedEmail); } catch (_) {}
        setError('');

        setResetStatus({ loading: false, message: 'Password updated.', error: '' });
        // Stay on the page; no redirect needed
        setPendingTosRedirect('');
      } catch (err) {
        setResetStatus({ loading: false, message: '', error: err?.message || 'Failed to reset password.' });
      }
    };

    const openForgotPassword = () => {
      setIsForgotPassword(true);
      setForgotEmail(email);
      setForgotMessage('');
      setForgotError('');
      setError('');
      setPendingTosToken('');
      setPendingTosEmail('');
      setPendingTosRedirect('');
      setTosAcceptError('');
      setAcceptingTos(false);
    };

    const closeForgotPassword = () => {
      setIsForgotPassword(false);
      setForgotLoading(false);
      setForgotMessage('');
      setForgotError('');
      setForgotEmail('');
      setPendingTosToken('');
      setPendingTosEmail('');
      setPendingTosRedirect('');
      setTosAcceptError('');
      setAcceptingTos(false);
    };

    const closeResetPasswordModal = () => {
      setShowResetPasswordModal(false);
      setResetStatus({ loading: false, message: '', error: '' });
      setResetPasswordValue('');
      setResetPasswordConfirm('');
      setPendingTosRedirect('');
    };



    // Auto-fill test credentials

    const backgroundDescriptionId = 'aspens-bg-description';
    const srOnlyStyle = {
      position: 'absolute',
      width: '1px',
      height: '1px',
      padding: 0,
      margin: '-1px',
      overflow: 'hidden',
      clip: 'rect(0, 0, 0, 0)',
      whiteSpace: 'nowrap',
      border: 0
    };

    const outerStyle = isMobileViewport ? {
      minHeight: backgroundMinHeight,
      width: '100%',
      backgroundImage: 'url(/aspens_2000.jpg)',
      backgroundSize: 'cover',
      backgroundPosition: 'center center',
      backgroundRepeat: 'no-repeat',
      backgroundAttachment: 'fixed',
      paddingLeft: 'var(--cmg-mobile-inline-padding)',
      paddingRight: 'var(--cmg-mobile-inline-padding-end)',
      paddingTop: 'var(--cmg-mobile-block-padding)',
      paddingBottom: 'var(--cmg-mobile-block-padding-end)',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'stretch',
      justifyContent: 'flex-start'
    } : {
      minHeight: backgroundMinHeight,
      backgroundImage: 'url(/aspens_2000.jpg)',
      backgroundSize: 'cover',
      backgroundPosition: 'center center',
      backgroundRepeat: 'no-repeat',
      backgroundAttachment: backgroundAttachmentMode,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      padding: '20px'
    };

    const wrapperProps = {
      className: isMobileViewport ? 'mobile-auth-wrapper' : undefined,
      style: isMobileViewport ? undefined : { maxWidth: '460px', width: '100%' }
    };

    const cardProps = {
      className: isMobileViewport ? 'mobile-auth-card' : undefined,
      style: {
        maxWidth: '460px',
        width: '100%',
        backgroundColor: 'rgba(255,255,255,0.82)',
        borderRadius: '12px',
        boxShadow: '0 18px 44px rgba(15, 23, 42, 0.22)',
        padding: isMobileViewport ? '24px 20px' : '36px'
      }
    };

    const inputStyle = isMobileViewport ? {
      width: '100%',
      padding: '12px 14px',
      border: '2px solid #3e96e2',
      borderRadius: '12px',
      fontSize: '16px',
      boxSizing: 'border-box'
    } : {
      width: '100%',
      padding: '10px',
      border: '2px solid #3e96e2',
      borderRadius: '8px',
      fontSize: '16px',
      boxSizing: 'border-box'
    };
  return (
    <div style={outerStyle} aria-describedby={backgroundDescriptionId}>
      <span id={backgroundDescriptionId} style={srOnlyStyle}>
        Aspen grove background photo with tall trunks and filtered sunlight setting a calm outdoor mood.
      </span>
      <div {...wrapperProps}>
        <div {...cardProps}>
          <div className={isMobileViewport ? 'mobile-auth-title' : undefined} style={{
            textAlign: 'center',
            marginBottom: '18px',
            backgroundColor: '#ffffff',
            padding: isMobileViewport ? '16px 14px' : '18px 16px',
            borderRadius: isMobileViewport ? '12px' : '12px',
            boxShadow: isMobileViewport ? '0 6px 18px rgba(15, 23, 42, 0.18)' : '0 8px 20px rgba(15, 23, 42, 0.15)'
          }}>
            <h1 style={{ fontSize: isMobileViewport ? '26px' : '30px', fontWeight: '700', color: '#111827', margin: 0, lineHeight: 1.25 }}>
              Welcome to<br/>
              The Aspen Grove of<br/>
              Opera Singers
            </h1>
            <img
              src="/logo.png"
              alt="The Aspen Grove logo"
              style={{
                width: isMobileViewport ? '96px' : '112px',
                height: 'auto',
                margin: '12px auto 0 auto',
                display: 'block'
              }}
            />
          </div>
  
          <>
            <div style={{ marginTop: 4, textAlign: 'center' }}>
              <button
                type="button"
                onClick={redirectToAuth0Login}
                style={{
                  backgroundColor: '#fff',
                  color: '#111',
                  border: '2px solid #3e96e2',
                  borderRadius: isMobileViewport ? '12px' : '8px',
                  padding: isMobileViewport ? '12px 18px' : '10px 16px',
                  fontWeight: 600,
                  cursor: 'pointer'
                }}
              >
                Sign in or create an account
              </button>
            </div>
            <div
              style={{
                marginTop: isMobileViewport ? '16px' : '20px',
                backgroundColor: '#ffffff',
                padding: isMobileViewport ? '14px 16px' : '16px 20px',
                borderRadius: isMobileViewport ? '12px' : '10px',
                boxShadow: '0 8px 20px rgba(15, 23, 42, 0.12)',
                color: '#1f2937',
                lineHeight: 1.5
              }}
            >
              <p style={{ margin: 0, fontSize: isMobileViewport ? '15px' : '16px' }}>
                The Aspen Grove of Opera Singers is an online database and data visualization project that illuminates relationships among opera singers and their teachers. Classical singing technique and tradition has been passed down in one-on-one lessons from its very origins in Florence in the late 16th century to the present day. The Aspen Grove makes this vast history available in text and in a visualization making exploration intuitive. Until now, this information has been scattered across online and print sources. Intended for students, teachers, scholars, and opera fans, this resource allows for a deep exploration of a singer’s pedagogical lineage and contextualizes the history of the entire operatic art form.
              </p>
            </div>
            {/* Disclaimer button removed per request */}
          </>
        </div>
      </div>
  
      {showTerms && (
        isMobileViewport ? (
          <>
            <div
              className="mobile-overlay-backdrop is-open"
              style={{ zIndex: 3000 }}
              onClick={handleCloseTerms}
            />
            <div
              className="mobile-sheet is-open"
              style={{ zIndex: 3001, paddingBottom: '24px' }}
              onClick={(e) => e.stopPropagation()}
            >
              <div className="mobile-sheet__header" style={{ paddingBottom: 0 }}>
                <div className="mobile-sheet__handle" />
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
                  <h3 style={{ margin: 0, fontSize: '20px', fontWeight: 600, color: '#0f172a' }}>
                    The Aspen Grove of Opera Singers, Disclaimer
                  </h3>
                  <button
                    onClick={handleCloseTerms}
                    style={{
                      background: 'none',
                      border: 'none',
                      color: '#1f2937',
                      fontSize: '24px',
                      cursor: 'pointer',
                      fontWeight: 600,
                      padding: 4,
                      width: 40,
                      height: 40,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center'
                    }}
                    disabled={acceptingTos}
                    aria-label="Close disclaimer"
                  >
                    ×
                  </button>
                </div>
              </div>
              <div className="mobile-sheet__content" style={{ paddingTop: 12 }}>
                <p className="mobile-auth-note">
                  Welcome to The Aspen Grove of Opera Singers, and thank you for your interest in this project.
                </p>
                <p className="mobile-auth-note">
                  I have spent the last three summers and this current sabbatical collecting information for this database. I have endeavored to include "successful" opera singers and their teachers. Success can, of course, be defined many ways. For the purposes of this tool, I have chosen to include singers who have sung roles at A- and B-level houses and their equivalents, singers who are managed, and singers who have been documented in reference books and websites specializing in classical singing and teaching history. Though the information is vast (15,000 singers, 6,100 relationships), it is far from exhaustive. I can make no claims about the quality of the teaching or of the quality of the relationship between teacher and student. Further, there is no guarantee that any teacher's methods carry forward to their students or the next generation, or to those that follow.
                </p>
                <p className="mobile-auth-note">
                  If you would like some or all of your personal information to be removed from the dataset for any reason, please <a href="mailto:classicalsinginghumanitieslab@gmail.com">let me know.</a> I will happily remove anyone's information. If you have a correction from a credible source, I will happily incorporate that too. If you have information to add that meets the criteria described above, please fill out <a href="https://forms.gle/TZmuaPpMUu9ob4jT8" target="_blank" rel="noopener noreferrer">this form</a>, and I will incorporate it as quickly as I can.
                </p>
                <p className="mobile-auth-note" style={{ color: '#111', fontSize: '14px' }}>
                  By tapping Agree &amp; Continue you acknowledge the extent of the site's current contents and the limitations described above.
                </p>
                {isTosAcceptanceRequired && (
                  <p className="mobile-auth-note" style={{ color: '#0f172a', fontSize: '14px', fontWeight: 600 }}>
                    To finish signing in{pendingTosEmail ? ` as ${pendingTosEmail}` : ''}, please tap Agree &amp; Continue below.
                  </p>
                )}
                {tosAcceptError && (
                  <div
                    style={{
                      backgroundColor: '#fef2f2',
                      border: '2px solid #fca5a5',
                      color: '#b91c1c',
                      padding: '10px',
                      borderRadius: 12,
                      marginTop: 8
                    }}
                  >
                    {tosAcceptError}
                  </div>
                )}
              </div>
              <div className="mobile-sheet__footer">
                <button
                  onClick={handleCloseTerms}
                  style={{
                    padding: '12px 16px',
                    backgroundColor: '#ffffff',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '12px'
                  }}
                  disabled={acceptingTos}
                >
                  Close
                </button>
                <button
                  onClick={handleDisclaimerAgree}
                  style={{
                    padding: '12px 16px',
                    backgroundColor: '#2563eb',
                    color: '#ffffff',
                    border: '2px solid #2563eb',
                    borderRadius: '12px',
                    cursor: acceptingTos ? 'not-allowed' : 'pointer',
                    opacity: acceptingTos ? 0.65 : 1
                  }}
                  disabled={acceptingTos}
                >
                  {acceptingTos ? 'Recording acknowledgement…' : 'Agree & Continue'}
                </button>
              </div>
            </div>
          </>
        ) : (
          <div style={{ position: 'fixed', inset: 0, backgroundColor: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 3000 }}>
            <div style={{ backgroundColor: 'white', width: 'min(720px, 92vw)', maxHeight: '80vh', overflowY: 'auto', borderRadius: 10, boxShadow: '0 20px 40px rgba(0,0,0,0.25)', padding: 20 }}>
              <h3 style={{ margin: 0, marginBottom: 10, fontSize: 18, color: '#111' }}>The Aspen Grove of Opera Singers, Disclaimer</h3>
              <p style={{ fontSize: 14, color: '#333', lineHeight: 1.6 }}>
                Welcome to The Aspen Grove of Opera Singers, and thank you for your interest in this project.
              </p>
              <p style={{ fontSize: 14, color: '#333', lineHeight: 1.6 }}>
                I have spent the last three summers and this current sabbatical collecting information for this database. I have endeavored to include "successful" opera singers and their teachers. Success can, of course, be defined many ways. For the purposes of this tool, I have chosen to include singers who have sung roles at A- and B-level houses and their equivalents, singers who are managed, and singers who have been documented in reference books and websites specializing in classical singing and teaching history. Though the information is vast (15,000 singers, 6,100 relationships), it is far from exhaustive. I can make no claims about the quality of the teaching or of the quality of the relationship between teacher and student. Further, there is no guarantee that any teacher's methods carry forward to their students or the next generation, or to those that follow.
              </p>
              <p style={{ fontSize: 14, color: '#333', lineHeight: 1.6 }}>
                If you would like some or all of your personal information to be removed from the dataset for any reason, please <a href="mailto:classicalsinginghumanitieslab@gmail.com">let me know</a>. I will happily remove anyone's information. If you have a correction from a credible source, I will happily incorporate that too. If you have information to add that meets the criteria described above, please fill out <a href="https://forms.gle/TZmuaPpMUu9ob4jT8" target="_blank">this form</a>, and I will incorporate it as quickly as I can.
              </p>
              <div style={{ marginTop: 12, paddingTop: 12, borderTop: '1px solid #eee', display: 'flex', flexDirection: 'column', gap: 12 }}>
                <p style={{ fontSize: 14, color: '#333', lineHeight: 1.6 }}>
                  By selecting Agree &amp; Continue you acknowledge the extent of the site's current contents and the limitations expressed in this disclaimer.
                </p>
                {isTosAcceptanceRequired && (
                  <p style={{ fontSize: 14, color: '#0f172a', fontWeight: 600 }}>
                    To finish signing in{pendingTosEmail ? ` as ${pendingTosEmail}` : ''}, please choose Agree &amp; Continue.
                  </p>
                )}
                {tosAcceptError && (
                  <div style={{ backgroundColor: '#fef2f2', border: '2px solid #fca5a5', color: '#b91c1c', padding: '10px', borderRadius: 8 }}>
                    {tosAcceptError}
                  </div>
                )}
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
                  <button
                    onClick={handleCloseTerms}
                    style={{ padding: '8px 12px', border: '2px solid #3e96e2', backgroundColor: '#fafafa', color: '#374151', borderRadius: 6, cursor: acceptingTos ? 'not-allowed' : 'pointer', opacity: acceptingTos ? 0.65 : 1 }}
                    disabled={acceptingTos}
                  >
                    Close
                  </button>
                  <button
                    onClick={handleDisclaimerAgree}
                    style={{ padding: '8px 12px', border: '2px solid #2563eb', backgroundColor: '#2563eb', color: '#ffffff', borderRadius: 6, cursor: acceptingTos ? 'not-allowed' : 'pointer', opacity: acceptingTos ? 0.65 : 1 }}
                    disabled={acceptingTos}
                  >
                    {acceptingTos ? 'Recording acknowledgement…' : 'Agree & Continue'}
                  </button>
                </div>
              </div>
            </div>
          </div>
        )
      )}

      {showResetPasswordModal && (
        <div style={{ position: 'fixed', inset: 0, backgroundColor: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 3005 }}>
          <div style={{ backgroundColor: '#ffffff', width: 'min(520px, 92vw)', maxHeight: '85vh', overflowY: 'auto', borderRadius: 12, boxShadow: '0 24px 50px rgba(0,0,0,0.25)', padding: isMobileViewport ? 24 : 28 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
              <h3 style={{ margin: 0, fontSize: 20, fontWeight: 600, color: '#0f172a' }}>Reset your password</h3>
              <button
                type="button"
                onClick={closeResetPasswordModal}
                style={{
                  background: 'none',
                  border: 'none',
                  color: '#1f2937',
                  fontSize: '24px',
                  cursor: 'pointer',
                  fontWeight: 600,
                  padding: 4,
                  width: 40,
                  height: 40,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center'
                }}
                aria-label="Close reset password modal"
              >
                ×
              </button>
            </div>
            <p style={{ marginTop: 0, marginBottom: 18, color: '#374151', lineHeight: 1.5, fontSize: isMobileViewport ? 15 : 14 }}>
              Paste the reset code from your email, choose a new password, and then sign in with your updated credentials.
            </p>
            {resetStatus.error && (
              <div style={{ backgroundColor: '#fef2f2', border: '2px solid #fca5a5', color: '#b91c1c', padding: '10px', borderRadius: isMobileViewport ? 12 : 8, marginBottom: 14 }}>
                {resetStatus.error}
              </div>
            )}
            {resetStatus.message && (
              <div style={{ backgroundColor: '#ecfdf5', border: '2px solid #6ee7b7', color: '#047857', padding: '10px', borderRadius: isMobileViewport ? 12 : 8, marginBottom: 14 }}>
                {resetStatus.message}
              </div>
            )}
            <div className={isMobileViewport ? 'mobile-auth-field' : undefined} style={{ marginBottom: 16 }}>
              <label style={{ fontWeight: 500, fontSize: isMobileViewport ? '15px' : '16px' }}>Reset code</label>
              <input
                type="text"
                value={resetTokenValue}
                onChange={(e) => setResetTokenValue(e.target.value)}
                style={inputStyle}
                placeholder="Paste your reset code"
              />
            </div>
            <div className={isMobileViewport ? 'mobile-auth-field' : undefined} style={{ marginBottom: 16 }}>
              <label style={{ fontWeight: 500, fontSize: isMobileViewport ? '15px' : '16px' }}>New password</label>
              <input
                type="password"
                value={resetPasswordValue}
                onChange={(e) => setResetPasswordValue(e.target.value)}
                style={inputStyle}
                placeholder="••••••••"
              />
            </div>
            <div className={isMobileViewport ? 'mobile-auth-field' : undefined} style={{ marginBottom: 20 }}>
              <label style={{ fontWeight: 500, fontSize: isMobileViewport ? '15px' : '16px' }}>Confirm new password</label>
              <input
                type="password"
                value={resetPasswordConfirm}
                onChange={(e) => setResetPasswordConfirm(e.target.value)}
                style={inputStyle}
                placeholder="Re-enter your new password"
              />
            </div>
            <button
              onClick={handleResetSubmit}
              disabled={resetStatus.loading}
              style={{
                width: '100%',
                backgroundColor: '#2563eb',
                color: '#ffffff',
                padding: isMobileViewport ? '14px' : '12px',
                border: 'none',
                borderRadius: isMobileViewport ? '12px' : '8px',
                fontSize: '16px',
                cursor: resetStatus.loading ? 'not-allowed' : 'pointer',
                opacity: resetStatus.loading ? 0.7 : 1
              }}
            >
              {resetStatus.loading ? 'Updating password…' : 'Update password'}
            </button>
          </div>
        </div>
      )}
    </div>
  );
  };

  const appBackgroundStyle = isMobileViewport ? {
    minHeight: backgroundMinHeight,
    width: '100%',
    backgroundImage: 'url(/aspens_2000.jpg)',
    backgroundSize: 'cover',
    backgroundPosition: 'center center',
    backgroundRepeat: 'no-repeat',
    backgroundAttachment: 'fixed',
    paddingLeft: 'var(--cmg-mobile-inline-padding)',
    paddingRight: 'var(--cmg-mobile-inline-padding-end)',
    paddingTop: 'var(--cmg-mobile-block-padding)',
    paddingBottom: 'var(--cmg-mobile-block-padding-end)'
  } : {
    minHeight: backgroundMinHeight,
    backgroundImage: 'url(/aspens_2000.jpg)',
    backgroundSize: 'cover',
    backgroundPosition: 'center center',
    backgroundRepeat: 'no-repeat',
    backgroundAttachment: backgroundAttachmentMode
  };

  if (!token) {
    return <AuthForm initialResetToken={initialResetToken} />;
  }
  const shouldBreakTitle = isHeaderMobile || (Number.isFinite(headerWidth) && headerWidth < 720);
  return (
    <div style={appBackgroundStyle}>
      {false && (<header style={{
        backgroundColor: 'white',
        borderBottom: '1px solid #dee2e6',
        padding: '15px 0'
      }}>
        <div style={{
          maxWidth: '1240px',
          margin: '0 auto',
          padding: '0 20px',
          display: 'flex',
          justifyContent: 'flex-start',
          alignItems: 'center',
          gap: 12
        }}>
          <h1 style={{ fontSize: '20px', fontWeight: 'bold', color: '#333' }}>
            Classical Singer Family Tree
          </h1>
          
          <div style={{ display: 'flex', alignItems: 'stretch', gap: '8px', flex: 1, justifyContent: 'flex-start' }}>
            

            {/* Saved view quick loader moved below Save/Export and Logout */}

            {currentView === 'network' && null}
            
            {currentView === 'network' && (
              <div
                style={{
                  display: 'flex',
                  columnGap: 8,
                  justifyContent: 'flex-start',
                  alignItems: 'stretch'
                }}
              >
                {/* Navigation buttons: Back, Forward, Filters, Path */}
                <button
                  onClick={() => { goBack(); }}
                  disabled={historyCounts.past === 0}
                  title={historyCounts.past ? `Back (${historyCounts.past})` : 'Back'}
                  style={{
                    padding: '8px 12px',
                    backgroundColor: historyCounts.past ? '#f3f4f6' : '#fafafa',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: historyCounts.past ? 'pointer' : 'not-allowed',
                    fontSize: '16px'
                  }}
                >
                  <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', lineHeight: 1 }}>
                    <span>Back</span>
                    <span style={{ fontSize: 12 }}>←</span>
                  </span>
                </button>
                <button
                  onClick={() => { goForward(); }}
                  disabled={historyCounts.future === 0}
                  title={historyCounts.future ? `Forward (${historyCounts.future})` : 'Forward'}
                  style={{
                    padding: '8px 12px',
                    backgroundColor: historyCounts.future ? '#f3f4f6' : '#fafafa',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: historyCounts.future ? 'pointer' : 'not-allowed',
                    fontSize: '16px'
                  }}
                >
                  <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', lineHeight: 1 }}>
                    <span>Forward</span>
                    <span style={{ fontSize: 12 }}>→</span>
                  </span>
                </button>
              <button
                onClick={() => toggleFilterPanel()}
                style={{
                  padding: '8px 16px',
                  backgroundColor: selectedVoiceTypes.size > 0 ? '#e3f2fd' : 'transparent',
                  color: selectedVoiceTypes.size > 0 ? '#1976d2' : '#666',
                  border: '2px solid #3e96e2',
                  borderRadius: '8px',
                  cursor: 'pointer',
                  fontSize: '16px',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px'
                }}
              >
                🔍 Filters
                {selectedVoiceTypes.size > 0 && (
                  <span style={{
                    backgroundColor: '#1976d2',
                    color: 'white',
                    borderRadius: '8px',
                    padding: '2px 6px',
                    fontSize: '12px',
                    fontWeight: 'bold'
                  }}>
                    {selectedVoiceTypes.size}
                  </span>
                )}
              </button>
                <button
                  onClick={togglePathPanel}
                  style={{
                    padding: '8px 16px',
                    backgroundColor: '#ffffff',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: 'pointer',
                    fontSize: '16px',
                    opacity: 1
                  }}
                >
                  Path
                </button>
              </div>
            )}

            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginLeft: 'auto' }}>
            {renderSaveExportToggle()}
            <button
              onClick={() => {
                setToken('');
                clearStoredToken();
                setCurrentView('search');
                setHasExecutedSearch(false);
              }}
              style={{
                backgroundColor: '#ffffff',
                color: '#374151',
                padding: '8px 16px',
                height: '48px',
                border: '2px solid #3e96e2',
                borderRadius: '8px',
                cursor: 'pointer',
                display: 'inline-flex',
                alignItems: 'center',
                boxSizing: 'border-box',
                fontSize: '16px'
              }}
            >
              Logout
            </button>
          </div>
        </div>
        </div>
      </header>)}
      <SavedViewDialog />
      <main
        style={{
          maxWidth: '1240px',
          margin: '0 auto',
          padding: '30px 20px',
          paddingBottom: isMobileViewport
            ? (currentView === 'network' ? '220px' : '140px')
            : '30px'
        }}
      >
        {/* Former header content moved into main */}
        <div
          ref={headerContainerRef}
          className={isHeaderMobile ? 'mobile-header-card' : undefined}
          style={{
            backgroundColor: 'rgba(255,255,255,0.9)',
            padding: isHeaderMobile
              ? 'calc(var(--cmg-mobile-block-padding) + 12px) max(16px, var(--cmg-mobile-inline-padding-end)) 20px max(16px, var(--cmg-mobile-inline-padding))'
              : '12px 16px',
            borderRadius: isHeaderMobile ? '18px' : '8px',
            marginBottom: 15,
            position: 'relative',
            boxShadow: isHeaderMobile ? '0 14px 32px rgba(15, 23, 42, 0.18)' : undefined,
            border: isHeaderMobile ? '2px solid #3e96e2' : undefined,
            paddingRight: isHeaderMobile ? undefined : 440,
            minHeight: isHeaderMobile ? 'auto' : 170
          }}
        >
          <div
            className={isHeaderMobile ? 'mobile-stack' : undefined}
            style={{
              maxWidth: '1240px',
              margin: '0 auto',
              padding: isHeaderMobile ? '0 calc(var(--cmg-mobile-inline-padding-end) + 32px) 0 0' : '0 20px',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'flex-start',
              alignItems: 'flex-start',
              gap: isHeaderMobile ? 16 : 12
            }}
          >
            <div
              role="group"
              aria-label="Site Titles"
              style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: isHeaderMobile ? 'flex-start' : 'flex-start',
                gap: 4,
                flex: 1,
                minWidth: 0
              }}
            >
              <h1
                className={isHeaderMobile ? 'mobile-heading' : undefined}
                style={{
                  fontSize: isHeaderMobile ? undefined : '40px',
                  fontWeight: 'bold',
                  color: isHeaderMobile ? undefined : '#333',
                  margin: 0,
                  whiteSpace: 'normal',
                  overflow: 'visible',
                  lineHeight: isHeaderMobile ? undefined : 1.2
                }}
              >
                {shouldBreakTitle ? (
                  <>The Aspen Grove<br />of Opera Singers</>
                ) : (
                  'The Aspen Grove of Opera Singers'
                )}
              </h1>
              <h2
                className={isHeaderMobile ? 'mobile-subheading mobile-muted' : undefined}
                style={{
                  fontSize: isHeaderMobile ? undefined : '24px',
                  fontWeight: 600,
                  color: isHeaderMobile ? undefined : '#374151',
                  margin: 0,
                  whiteSpace: 'normal',
                  overflow: 'visible',
                  lineHeight: isHeaderMobile ? undefined : 1.4
                }}
              >
                Discover Connections Among Classical Singers, <br/> Opera Premieres, and Vocal Pedagogy Books
              </h2>
            </div>
            {!isHeaderMobile && (
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', justifyContent: 'flex-start', flex: '0 0 auto' }} />
            )}
            {/* Right controls moved to absolute group */}
          </div>
          {/* Absolute top-right controls group */}
          <div
            style={{
              position: 'absolute',
              top: isHeaderMobile ? 'max(12px, var(--cmg-mobile-block-padding))' : 12,
              right: isHeaderMobile ? 'max(12px, var(--cmg-mobile-inline-padding-end))' : 20,
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'flex-end',
              gap: isHeaderMobile ? 6 : 8
            }}
          >
            {/* Welcome line above buttons / hamburger (hide on phones to avoid crowding) */}
            {!isHeaderMobile && (
              <div
                style={{
                  color: '#374151',
                  fontSize: '14px',
                  fontWeight: 600,
                  textAlign: 'right'
                }}
              >
                {`Welcome to The Aspen Grove${userEmail ? ", " + ((userEmail.split('@')[0]) || userEmail) : ''}`}
              </div>
            )}
            {isHeaderMobile ? (
              <button
                type="button"
                className="mobile-tap-target"
                onClick={() => {
                  setShowSaveExportMenu(false);
                  setShowHeaderMenu(prev => !prev);
                }}
                style={{
                  padding: '10px 12px',
                  backgroundColor: '#ffffff',
                  color: '#374151',
                  border: '2px solid #3e96e2',
                  borderRadius: '12px',
                  cursor: 'pointer',
                  fontSize: '20px',
                  lineHeight: 1,
                  width: '48px',
                  height: '48px',
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  boxSizing: 'border-box',
                  fontWeight: 700
                }}
                aria-label={showHeaderMenu ? 'Close menu' : 'Open menu'}
                aria-expanded={showHeaderMenu}
              >
                ☰
              </button>
            ) : (
              <>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8
                  }}
                >
                  <div style={{ position: 'relative' }}>
                    <button
                      onClick={() => {
                        if (showPathPanel) {
                          closePathPanel();
                        } else {
                          openPathPanel();
                        }
                      }}
                      style={{
                        padding: '8px 16px',
                        backgroundColor: showPathPanel ? '#f3f4f6' : '#ffffff',
                        color: '#374151',
                        border: '2px solid #3e96e2',
                        borderRadius: '8px',
                        cursor: 'pointer',
                        fontSize: '16px',
                        lineHeight: '20px',
                        height: '48px',
                        display: 'inline-flex',
                        alignItems: 'center',
                        boxSizing: 'border-box'
                      }}
                    >
                      Path ▾
                    </button>
                    {!viewportIsPhone && showPathPanel && (
                      <div
                        ref={pathPanelRef}
                        style={{
                          position: 'absolute',
                          top: '110%',
                          left: 0,
                          border: '2px solid #3e96e2',
                          borderRadius: 8,
                          boxShadow: '0 8px 20px rgba(0,0,0,0.12)',
                          padding: 12,
                          backgroundColor: 'white',
                          minWidth: 320,
                          zIndex: 1100
                        }}
                        onClick={(e) => e.stopPropagation()}
                      >
                        {renderPathPanelContent({
                          isMobile: false,
                          pathFromRef,
                          pathToRef,
                          pathFromValRef,
                          pathToValRef,
                          pathInfo,
                          pathListRef,
                          handleClearPath,
                          onFindPath: runPathFind,
                          renderRelationshipSourceLink,
                          onClose: closePathPanel
                        })}
                      </div>
                    )}
                  </div>
                  <div>
                    <button
                      onClick={() => { setCurrentView('help'); setShowSaveExportMenu(false); try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {} }}
                      style={{
                        padding: '8px 16px',
                        backgroundColor: '#ffffff',
                        color: '#374151',
                        border: '2px solid #3e96e2',
                        borderRadius: '8px',
                        cursor: 'pointer',
                        fontSize: '16px',
                        lineHeight: '20px',
                        height: '48px',
                        display: 'inline-flex',
                        alignItems: 'center',
                        boxSizing: 'border-box'
                      }}
                    >
                      Help center
                    </button>
                  </div>
                  <div>
                    <button
                      ref={logoutBtnRef}
                      onClick={() => { setToken(''); clearStoredToken(); setCurrentView('search'); setHasExecutedSearch(false); }}
                      style={{ backgroundColor: '#ffffff', color: '#374151', padding: '8px 16px', height: '48px', border: '2px solid #3e96e2', borderRadius: '8px', cursor: 'pointer', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box', opacity: 1, fontSize: '16px' }}
                    >
                      Logout
                    </button>
                  </div>
                </div>
                {(isSaveExportEligible || currentView === 'network') && (
                  <div
                    style={{
                      alignSelf: 'stretch',
                      display: 'flex',
                      alignItems: 'center',
                      gap: 8,
                      flexWrap: 'wrap'
                    }}
                  >
                    {currentView === 'network' && (
                      <div
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: 8,
                          flexWrap: 'wrap'
                        }}
                      >
                        <button onClick={() => { goBack(); }} disabled={historyCounts.past === 0} title={historyCounts.past ? `Back (${historyCounts.past})` : 'Back'} style={{ padding: '8px 12px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: '8px', cursor: historyCounts.past ? 'pointer' : 'not-allowed', fontSize: '16px', opacity: 1, height: '48px', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box' }}>
                          <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', lineHeight: 1 }}>
                            <span>Back</span>
                            <span style={{ fontSize: 12 }}>←</span>
                          </span>
                        </button>
                        <button onClick={() => { goForward(); }} disabled={historyCounts.future === 0} title={historyCounts.future ? `Forward (${historyCounts.future})` : 'Forward'} style={{ padding: '8px 12px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: '8px', cursor: historyCounts.future ? 'pointer' : 'not-allowed', fontSize: '16px', opacity: 1, height: '48px', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box' }}>
                          <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', lineHeight: 1 }}>
                            <span>Forward</span>
                            <span style={{ fontSize: 12 }}>→</span>
                          </span>
                        </button>
                        <button onClick={() => toggleFilterPanel()} style={{ padding: '8px 16px', backgroundColor: '#ffffff', color: selectedVoiceTypes.size > 0 ? '#1976d2' : '#666', border: '2px solid #3e96e2', borderRadius: '8px', cursor: 'pointer', fontSize: '16px', display: 'flex', alignItems: 'center', gap: '6px', opacity: 1, height: '48px', boxSizing: 'border-box' }}>
                          🔍 Filters
                          {selectedVoiceTypes.size > 0 && (
                            <span style={{ backgroundColor: '#1976d2', color: 'white', borderRadius: '8px', padding: '2px 6px', fontSize: '12px', fontWeight: 'bold' }}>
                              {selectedVoiceTypes.size}
                            </span>
                          )}
                        </button>
                      </div>
                    )}
                    {isSaveExportEligible && (
                      <div
                        style={{
                          marginLeft: 'auto'
                        }}
                      >
                        {renderSaveExportToggle({ isMobileLayout: isHeaderMobile })}
                      </div>
                    )}
                  </div>
                )}
                {!hasSearchResults && currentView !== 'network' && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <input
                      placeholder="Paste saved view string here"
                      value={loadToken}
                      onChange={e => setLoadToken(e.target.value)}
                      onKeyDown={(e) => { if (e.key === 'Enter') { attemptLoadSavedView(); } }}
                    style={{ padding: '6px 8px', border: '2px solid #3e96e2', backgroundColor: '#ffffff', color: '#374151', borderRadius: 8, width: savedInputBelowWidth, height: '48px', boxSizing: 'border-box', fontSize: '16px', textAlign: 'center' }}
                    />
                    <button
                      ref={openBtnBelowRef}
                      onClick={attemptLoadSavedView}
                      disabled={!token || !loadToken || isLoadingView}
                      style={{ padding: '8px 12px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: '8px', cursor: (token && loadToken && !isLoadingView) ? 'pointer' : 'not-allowed', fontSize: '16px', opacity: 1, height: '48px', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box' }}
                    >
                      {isLoadingView ? 'Opening…' : 'Open'}
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
        {isHeaderMobile && showHeaderMenu && (
          <div
            className="mobile-header-overlay"
            style={{
              position: 'fixed',
              inset: 0,
              backgroundColor: 'rgba(15,23,42,0.45)',
              zIndex: 2000,
              display: 'flex',
              justifyContent: 'center'
            }}
            onClick={() => setShowHeaderMenu(false)}
          >
            <div
              className="mobile-panel"
              role="dialog"
              aria-modal="true"
              aria-label="Navigation menu"
              style={{
                marginTop: 'calc(var(--cmg-mobile-block-padding) + 48px)'
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontSize: '18px', fontWeight: 600, color: '#0f172a' }}>Menu</span>
                <button
                  type="button"
                  onClick={() => setShowHeaderMenu(false)}
                  style={{
                    background: 'none',
                    border: 'none',
                    fontSize: '24px',
                    color: '#1f2937',
                    cursor: 'pointer',
                    fontWeight: 600
                  }}
                  aria-label="Close menu"
                >
                  ×
                </button>
              </div>
              {/* Signed-in identity for mobile, shown inside the menu */}
              {userEmail && (
                <div style={{ marginTop: 8, marginBottom: 8, color: '#374151', fontSize: 14, fontWeight: 600 }}>
                  {`Signed in as ${userEmail.split('@')[0]}`}
                </div>
              )}
              <button
                type="button"
                onClick={() => {
                  setCurrentView('help');
                  setShowHeaderMenu(false);
                }}
                style={{
                  padding: '12px 16px',
                  border: '2px solid #3e96e2',
                  borderRadius: 12,
                  backgroundColor: '#ffffff',
                  color: '#374151',
                  fontSize: '16px',
                  fontWeight: 600,
                  cursor: 'pointer'
                }}
              >
                Help center
              </button>
              <button
                type="button"
                onClick={() => {
                  if (showPathPanel) {
                    closePathPanel();
                  } else {
                    openPathPanel();
                  }
                  setShowHeaderMenu(false);
                }}
                style={{
                  padding: '12px 16px',
                  border: '2px solid #3e96e2',
                  borderRadius: 12,
                  backgroundColor: '#ffffff',
                  color: '#374151',
                  fontSize: '16px',
                  fontWeight: 600,
                  cursor: 'pointer'
                }}
              >
                Path
              </button>
              {(!hasSearchResults && currentView !== 'network') && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  <input
                    placeholder="Paste saved view string here"
                    value={loadToken}
                    onChange={e => setLoadToken(e.target.value)}
                    onKeyDown={(e) => { if (e.key === 'Enter') { attemptLoadSavedView(); } }}
                    style={{ padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: 12, height: '48px', boxSizing: 'border-box', fontSize: '16px', textAlign: 'center' }}
                  />
                  <button
                    type="button"
                    onClick={attemptLoadSavedView}
                    disabled={!token || !loadToken || isLoadingView}
                    style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: (token && loadToken && !isLoadingView) ? 'pointer' : 'not-allowed', opacity: (token && loadToken && !isLoadingView) ? 1 : 0.6 }}
                  >
                    {isLoadingView ? 'Opening…' : 'Open'}
                  </button>
                </div>
              )}
              {hasSearchResults && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  {renderSaveExportFields({ isMobileLayout: true })}
                </div>
              )}
              {currentView === 'network' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  <button type="button" onClick={() => { goBack(); }} disabled={historyCounts.past === 0} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: historyCounts.past ? 'pointer' : 'not-allowed', opacity: historyCounts.past ? 1 : 0.6 }}>Back</button>
                  <button type="button" onClick={() => { goForward(); }} disabled={historyCounts.future === 0} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: historyCounts.future ? 'pointer' : 'not-allowed', opacity: historyCounts.future ? 1 : 0.6 }}>Forward</button>
                  <button type="button" onClick={() => { toggleFilterPanel(); setShowHeaderMenu(false); }} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: selectedVoiceTypes.size > 0 ? '#1976d2' : '#374151', fontSize: '16px', fontWeight: 600, cursor: 'pointer', display: 'flex', justifyContent: 'center', gap: 8 }}>
                    🔍 Filters
                    {selectedVoiceTypes.size > 0 && (
                      <span style={{ backgroundColor: '#1976d2', color: '#ffffff', borderRadius: 8, padding: '2px 8px', fontSize: 12, fontWeight: 700 }}>
                        {selectedVoiceTypes.size}
                      </span>
                    )}
                  </button>
                </div>
              )}
              <button
                type="button"
                onClick={() => {
                  setToken('');
                  clearStoredToken();
                  setCurrentView('search');
                  setShowHeaderMenu(false);
                  setHasExecutedSearch(false);
                }}
                style={{
                  padding: '12px 16px',
                  border: '2px solid #3e96e2',
                  borderRadius: 12,
                  backgroundColor: '#ffffff',
                  color: '#cb1f1f',
                  fontSize: '16px',
                  fontWeight: 600,
                  cursor: 'pointer'
                }}
              >
                Logout
              </button>
            </div>
          </div>
        )}
        {/* Removed duplicate tagline block below header as titles are now in the header group */}

        {currentView !== 'help' && (
          <>
            <div style={{
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
              gap: '10px',
              marginBottom: '20px',
              justifyContent: 'center'
            }}>
              {[
                { key: 'singers', label: 'People', icon: '👤' },
                { key: 'operas', label: 'Operas', icon: '🎵' },
                { key: 'books', label: 'Books', icon: '📚' }
              ].map(type => (
                <button
                  key={type.key}
                  onClick={() => {
                    setSearchType(type.key);
                    setSearchResults([]);
                    setCurrentView('search');
                    setItemDetails(null);
                    setSelectedItem(null);
                    setError('');
                    setSearchQuery('');
                    setNetworkData(sanitizeGraphData({ nodes: [], links: [] }));
                    setHasExecutedSearch(false);
                  }}
                  style={{
                    padding: '10px 16px',
                    backgroundColor: 'white',
                    color: '#333',
                    border: searchType === type.key ? '4px solid #3e96e2' : '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: 'pointer',
                    fontSize: '14px',
                    fontWeight: (searchType === type.key ? '600' : '500'),
                    opacity: 1
                  }}
                >
                  {type.icon} {type.label}
                </button>
              ))}
            </div>

            <div style={{ maxWidth: '600px', margin: '0 auto 30px', width: '100%' }}>
              <div style={{ display: 'flex', flexDirection: isHeaderMobile ? 'column' : 'row', gap: '15px', width: '100%' }}>
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && performSearch()}
                  placeholder={
                    searchType === 'singers'
                      ? (isHeaderMobile ? 'Search people (e.g., Garcia II)' : 'Search for opera singers and teachers... (e.g., Garcia II)')
                      : searchType === 'operas'
                        ? 'Search for operas... (e.g., La Traviata)'
                        : (isHeaderMobile ? 'Search ped books (Vocal Wisdom)' : 'Search for vocal pedagogy books... (e.g., Vocal wisdom, c1931)')
                  }
                  spellCheck="false"
                  style={{
                    flex: 1,
                    padding: '15px',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    fontSize: '16px'
                  }}
                />
                <button
                  onClick={performSearch}
                  disabled={loading || !searchQuery.trim()}
                  style={{
                    backgroundColor: 'white',
                    color: '#333',
                    padding: '12px 20px',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: loading || !searchQuery.trim() ? 'not-allowed' : 'pointer',
                    fontSize: '16px',
                    fontWeight: 500,
                    // On mobile (stacked), keep the button narrower than the input and center it
                    width: isHeaderMobile ? 'auto' : 'auto',
                    alignSelf: isHeaderMobile ? 'center' : undefined
                  }}
                >
                  Search
                </button>
              </div>
            </div>
          </>
        )}

        {currentView === 'search' && (
          <>
            <div style={{ marginTop: isHeaderMobile ? '48px' : '100px', width: '100%', display: 'flex', justifyContent: 'center' }}>
              <div
                className={isHeaderMobile ? 'mobile-stack mobile-search-hero' : undefined}
                style={{
                  width: isHeaderMobile ? '100%' : 790,
                  display: 'flex',
                  flexDirection: 'column',
                  gap: isHeaderMobile ? '20px' : '18px',
                  padding: isHeaderMobile ? '0 var(--cmg-mobile-inline-padding)' : 0
                }}
              >
                <div style={{ textAlign: isHeaderMobile ? 'center' : 'left' }}>
                  <h3 style={{ display: 'inline-block', backgroundColor: '#ffffff', padding: '6px 10px', borderRadius: '8px' }}>Examples:</h3>
                </div>
                <div
                  className={isHeaderMobile ? 'mobile-search-example-grid' : undefined}
                  style={{
                    display: isHeaderMobile ? 'grid' : 'flex',
                    gridTemplateColumns: isHeaderMobile ? '1fr' : undefined,
                    gap: isHeaderMobile ? '16px' : '20px',
                    justifyContent: isHeaderMobile ? 'stretch' : 'flex-start',
                    alignItems: 'flex-start',
                    flexDirection: isHeaderMobile ? undefined : 'row',
                    width: '100%'
                  }}
                >
                  {[{
                    key: 'ailyn',
                    label: 'Ailyn Pérez',
                    image: '/Ailyn.png',
                    token: 'd4240ab6-d2a5-4199-8e06-6d24c01e3ad7'
                  }, {
                    key: 'longest',
                    label: 'Back almost 500 years',
                    image: '/Longest.png',
                    token: '20df2cc8-8add-45e4-a49c-5440d6347715'
                  }, {
                    key: 'books-premieres',
                    label: 'Books &\nPremieres',
                    image: '/BnP.png',
                    token: 'b5715a67-28a4-4add-975b-7a682bc64f4a'
                  }].map(example => (
                    <button
                      key={example.key}
                      type="button"
                      onClick={() => loadViewByToken(example.token, { treatAsSearch: true, centerOnLoad: isHeaderMobile })}
                      style={{
                        width: isHeaderMobile ? '100%' : 250,
                        height: isHeaderMobile ? 200 : 170,
                        padding: 0,
                        border: 'none',
                        background: 'none',
                        position: 'relative',
                        cursor: 'pointer',
                        borderRadius: '12px',
                        overflow: 'hidden',
                        boxShadow: '0 8px 18px rgba(0,0,0,0.25)'
                      }}
                      aria-label={`Load example view for ${example.label.replace(/\n/g, ' ')}`}
                    >
                      <img
                        src={example.image}
                        alt={example.label}
                        style={{
                          width: '100%',
                          height: '100%',
                          objectFit: 'cover',
                          display: 'block',
                          borderRadius: '12px',
                          border: '4px solid #ffffff'
                        }}
                      />
                      <span
                        style={{
                          position: 'absolute',
                          left: '50%',
                          top: '50%',
                          transform: 'translate(-50%, -50%)',
                          color: '#111827',
                          fontSize: '24px',
                          fontWeight: 700,
                          textShadow: '0 0 6px rgba(255,255,255,0.95), 0 1px 10px rgba(255,255,255,0.85)',
                          whiteSpace: 'nowrap'
                        }}
                      >
                        <span style={{ display: 'inline-block', textAlign: 'center', whiteSpace: 'pre-line' }}>
                          {example.label}
                        </span>
                      </span>
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {!isHeaderMobile && (
              <div style={{ maxWidth: '1200px', margin: '20px auto 0', padding: '0 20px', display: 'flex', justifyContent: 'flex-end', gap: '16px', alignItems: 'center' }}>
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '12px' }}>
                  <img
                    src="/paypal.png"
                    alt="Support via PayPal"
                    style={{
                      width: 160,
                      height: 160,
                      objectFit: 'contain',
                      border: '2px solid #3e96e2',
                      borderRadius: '12px',
                      padding: '8px',
                      backgroundColor: '#f9fafb'
                    }}
                  />
                  <a
                    href="https://www.paypal.biz/sethkeetonvoice"
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{
                      width: '160px',
                      display: 'inline-flex',
                      justifyContent: 'center',
                      alignItems: 'center',
                      padding: '12px 16px',
                      backgroundColor: '#ffffff',
                      color: '#374151',
                      border: '2px solid #3e96e2',
                      borderRadius: '8px',
                      cursor: 'pointer',
                      fontSize: '16px',
                      fontWeight: 600,
                      textDecoration: 'none',
                      boxSizing: 'border-box'
                    }}
                  >
                    Link to Paypal
                  </a>
                </div>
              </div>
            )}
            {isHeaderMobile && (
              <div style={{ margin: '12px auto 0', display: 'flex', justifyContent: 'center', width: '100%' }}>
                <a
                  className="mobile-donate-card"
                  href="https://www.paypal.biz/sethkeetonvoice"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Link to donate on PayPal
                </a>
              </div>
            )}
          </>
        )}

        {currentView === 'search' && (
          isMobileViewport ? (
            <>
              <div
                className={`mobile-overlay-backdrop${showSupportPanel ? ' is-open' : ''}`}
                onClick={() => setShowSupportPanel(false)}
                style={{ zIndex: 1199 }}
              />
              <div
                className={`mobile-sheet${showSupportPanel ? ' is-open' : ''}`}
                style={{ paddingBottom: '24px', zIndex: 1200 }}
                aria-label="Support panel"
              >
                <div className="mobile-sheet__header" style={{ paddingBottom: 0 }}>
                  <div className="mobile-sheet__handle" />
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
                    <h3 style={{ margin: 0, fontSize: '20px', fontWeight: 600, color: '#0f172a' }}>
                      Support The Aspen Grove of Opera Singers
                    </h3>
                    <button
                      onClick={() => setShowSupportPanel(false)}
                      style={{
                        background: 'none',
                        border: 'none',
                        color: '#1f2937',
                        fontSize: '22px',
                        cursor: 'pointer',
                        fontWeight: 600,
                        padding: 4,
                        width: 40,
                        height: 40,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center'
                      }}
                      aria-label="Hide support panel"
                    >
                      ×
                    </button>
                  </div>
                </div>
                <div className="mobile-sheet__content" style={{ paddingTop: 12 }}>
                  <p style={{ margin: 0, fontSize: '16px', color: '#374151', lineHeight: 1.5 }}>
                    We depend on your support to maintain and grow the Aspen Grove. Proceeds from your donation pay for hosting costs and the ability to hire an assistant. Server costs are modest, but ongoing. To help with these and to keep the site ad free, a suggested $10/year donation is incredibly appreciated. Any amount is a great help. Thank you!
                  </p>
                </div>
                <div className="mobile-sheet__footer">
                  <a
                    className="mobile-donate-card"
                    href="https://www.paypal.biz/sethkeetonvoice"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    Link to donate on PayPal
                  </a>
                </div>
              </div>
            </>
          ) : (
            <div
              style={{
                position: 'fixed',
                left: '50%',
                bottom: showSupportPanel ? '40px' : '-1000px',
                transform: 'translateX(-50%)',
                width: headerWidth ? `${headerWidth}px` : 'min(1200px, calc(100vw - 40px))',
                backgroundColor: '#ffffff',
                borderRadius: '16px',
                border: '2px solid #3e96e2',
                boxShadow: '0 18px 44px rgba(0,0,0,0.25)',
                padding: '24px 28px',
                display: 'grid',
                gridTemplateColumns: 'minmax(0, 1fr) auto auto',
                columnGap: '24px',
                rowGap: '16px',
                alignItems: 'start',
                transition: 'bottom 0.6s cubic-bezier(0.22, 1, 0.36, 1)',
                zIndex: 1200,
                opacity: showSupportPanel ? 1 : 0,
                pointerEvents: showSupportPanel ? 'auto' : 'none'
              }}
            >
              <div
                style={{
                  gridColumn: '1',
                  gridRow: '1',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '12px',
                  minWidth: 0
                }}
              >
                <h3 style={{ margin: 0, fontSize: '22px', color: '#0f172a' }}>
                  Support The Aspen Grove of Opera Singers
                </h3>
                <p style={{ margin: 0, fontSize: '16px', color: '#374151', lineHeight: 1.5 }}>
                  We depend on your support to maintain and grow the Aspen Grove. Proceeds from your donation pay for hosting costs and the ability to hire an assistant. Server costs are modest, but ongoing. To help with these and to keep the site ad free, a suggested $10/year donation is incredibly appreciated. Any amount is a great help. Thank you!
                </p>
              </div>
              <div
                style={{
                  gridColumn: '2',
                  gridRow: '1',
                  justifySelf: 'end',
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  gap: '12px'
                }}
              >
                <img
                  src="/paypal.png"
                  alt="Support via PayPal"
                  style={{
                    width: '160px',
                    height: '160px',
                    objectFit: 'contain',
                    border: '2px solid #3e96e2',
                    borderRadius: '12px',
                    padding: '8px',
                    backgroundColor: '#f9fafb'
                  }}
                />
                <a
                  href="https://www.paypal.biz/sethkeetonvoice"
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{
                    width: '160px',
                    display: 'inline-flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    padding: '12px 16px',
                    backgroundColor: '#ffffff',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: 'pointer',
                    fontSize: '16px',
                    fontWeight: 600,
                    textDecoration: 'none',
                    textAlign: 'center',
                    boxSizing: 'border-box'
                  }}
                >
                  Link to Paypal
                </a>
              </div>
              <button
                onClick={() => setShowSupportPanel(false)}
                style={{
                  gridColumn: '3',
                  gridRow: '1',
                  justifySelf: 'end',
                  alignSelf: 'start',
                  background: 'none',
                  border: 'none',
                  color: '#1f2937',
                  fontSize: '22px',
                  cursor: 'pointer',
                  fontWeight: 600,
                  padding: 0
                }}
                aria-label="Hide support panel"
              >
                ×
              </button>
            </div>
          )
        )}

        {currentView === 'help' && (
          <Suspense fallback={<div style={{ color: '#fff', textAlign: 'center', marginTop: 40 }}>Loading help…</div>}>
            <HelpCenter onBack={() => setCurrentView('search')} />
          </Suspense>
        )}

        {/* removed legacy help_bak_never_shown block */}

        {/* Active Filter Indicators - show when in network view and filters are active */}
        {currentView === 'network' && selectedVoiceTypes.size > 0 && (
          <div style={{
            maxWidth: '640px',
            margin: '0 auto 20px',
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            flexWrap: isMobileViewport ? 'nowrap' : 'wrap',
            overflowX: isMobileViewport ? 'auto' : 'visible',
            padding: isMobileViewport ? '10px 14px' : '12px 18px',
            backgroundColor: 'rgba(255, 255, 255, 0.82)',
            borderRadius: '12px',
            boxShadow: '0 18px 44px rgba(15, 23, 42, 0.18)',
            border: '2px solid rgba(62, 150, 226, 0.4)'
          }}>
            <span style={{
              fontSize: '16px',
              color: '#6b7280',
              fontWeight: '500'
            }}>
              Active filters:
            </span>
            {Array.from(selectedVoiceTypes).map(voiceType => {
              const voiceTypeConfig = VOICE_TYPES.find(vt => vt.name === voiceType);
              return (
                <div
                  key={voiceType}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    backgroundColor: '#e3f2fd',
                    border: '1px solid #bbdefb',
                    borderRadius: '8px',
                    padding: '4px 8px 4px 6px',
                    fontSize: '16px',
                    gap: '6px',
                    flex: '0 0 auto'
                  }}
                >
                  <div
                    style={{
                      width: '12px',
                      height: '12px',
                      backgroundColor: voiceTypeConfig?.color || '#6b7280',
                      borderRadius: '50%',
                      border: '2px solid #3e96e2',
                      boxShadow: '0 0 0 1px rgba(0,0,0,0.1)'
                    }}
                  />
                  <span style={{ color: '#1976d2', fontWeight: '500' }}>
                    {voiceType}
                  </span>
                  <button
                    onClick={() => toggleVoiceTypeFilter(voiceType)}
                    style={{
                      background: 'none',
                      border: 'none',
                      color: '#666',
                      cursor: 'pointer',
                      fontSize: '16px',
                      lineHeight: '1',
                      padding: '0',
                      width: '16px',
                      height: '16px',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center'
                    }}
                    title={`Remove ${voiceType} filter`}
                  >
                    ×
                  </button>
                </div>
              );
            })}
            <button
              onClick={clearAllFilters}
              style={{
                background: 'none',
                border: '1px solid #dc2626',
                color: '#dc2626',
                padding: '4px 8px',
                borderRadius: '8px',
                fontSize: '12px',
                cursor: 'pointer',
                fontWeight: '500'
              }}
            >
              Clear all
            </button>
          </div>
        )}

        {error && (
          <div style={{
            maxWidth: '600px',
            margin: '0 auto',
            backgroundColor: '#fee',
            border: '2px solid #3e96e2',
            color: '#c33',
            padding: '15px',
            borderRadius: '8px'
          }}>
            {error}
          </div>
        )}

        {helperMessage && (
          <div style={{
            maxWidth: '600px',
            margin: '12px auto 0 auto',
            backgroundColor: '#ecfeff',
            border: '2px solid #22d3ee',
            color: '#0f172a',
            padding: '12px 15px',
            borderRadius: '8px',
            fontWeight: 500,
            textAlign: 'center'
          }}>
            {helperMessage}
          </div>
        )}

        {currentView === 'results' && searchResults.length > 0 && (
          <div style={{ marginBottom: '30px', padding: isHeaderMobile ? '0 var(--cmg-mobile-inline-padding)' : 0 }}>
            <h3 style={{ display: 'inline-block', backgroundColor: '#ffffff', padding: '6px 10px', borderRadius: '8px' }}>Search Results ({searchResults.length})</h3>
            <div
              className={isHeaderMobile ? 'mobile-search-results-grid' : undefined}
              style={{
                display: 'grid',
                gridTemplateColumns: isHeaderMobile ? '1fr' : 'repeat(auto-fill, minmax(300px, 1fr))',
                gap: isHeaderMobile ? '16px' : '20px'
              }}
            >
              {searchResults.map((item, index) => (
                <div
                  key={index}
                  onClick={() => getItemDetails(item)}
                  className={isHeaderMobile ? 'mobile-card mobile-search-result-card' : undefined}
                  style={{
                    backgroundColor: 'white',
                    borderRadius: '8px',
                    padding: isHeaderMobile ? '10px 12px' : '10px 12px',
                    boxShadow: showResultsHalo ? '0 0 12px 2px rgba(255,255,255,0.85), 0 0 18px 6px rgba(62,150,226,0.45), 0 0 22px 9px rgba(228,162,1,0.35), 0 0 28px 12px rgba(62,150,226,0.25)' : '0 2px 4px rgba(0,0,0,0.1)',
                    border: '2px solid #3e96e2',
                    cursor: 'pointer',
                    transition: 'box-shadow 0.35s ease',
                    minHeight: '68px',
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'center',
                    rowGap: '2px'
                  }}
                  onMouseOver={(e) => e.currentTarget.style.boxShadow = showResultsHalo ? '0 0 12px 2px rgba(255,255,255,0.90), 0 0 22px 8px rgba(62,150,226,0.50), 0 0 26px 10px rgba(228,162,1,0.40), 0 10px 22px rgba(0,0,0,0.12)' : '0 4px 8px rgba(0,0,0,0.15)'}
                  onMouseOut={(e) => e.currentTarget.style.boxShadow = showResultsHalo ? '0 0 12px 2px rgba(255,255,255,0.85), 0 0 18px 6px rgba(62,150,226,0.45), 0 0 22px 9px rgba(228,162,1,0.35), 0 0 28px 12px rgba(62,150,226,0.25)' : '0 2px 4px rgba(0,0,0,0.1)'}
                >
                  <h4 style={{ margin: '0 0 2px 0', fontSize: '15px', lineHeight: 1.2 }}>
                    {searchType === 'singers' ? (item.name || item.properties.full_name) : searchType === 'operas' ? item.properties.opera_name : item.properties.title}
                  </h4>
                  {searchType === 'singers' && item.properties.voice_type && (
                    <p style={{ margin: 0, fontSize: '13px', color: '#555', lineHeight: 1.2 }}>
                      <strong>Voice type:</strong> {item.properties.voice_type}
                    </p>
                  )}
                  {searchType === 'singers' && (item.properties.birth_year || item.properties.death_year) && (
                    <p style={{ margin: '1px 0', fontSize: '13px', color: '#555', lineHeight: 1.2 }}>
                  <strong>Dates:</strong> {
                        item.properties.birth_year && item.properties.death_year
                          ? `${item.properties.birth_year}-${item.properties.death_year}`
                          : item.properties.birth_year
                          ? `${item.properties.birth_year}-`
                          : item.properties.death_year
                          ? `-${item.properties.death_year}`
                          : ''
                      }
                  </p>
                  )}
                  {searchType === 'operas' && item.properties.composer && (
                    <p style={{ margin: '1px 0', fontSize: '13px', color: '#555', lineHeight: 1.2 }}>
                      <strong>Composer:</strong> {item.properties.composer}
                    </p>
                  )}
                  {searchType === 'books' && item.properties.author && (
                    <p style={{ margin: '1px 0', fontSize: '13px', color: '#555', lineHeight: 1.2 }}>
                      <strong>Author:</strong> {item.properties.author}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {(currentView === 'results' || currentView === 'network') && (networkData.nodes.length > 0 || showPathPanel) && (
          <div
            style={{ width: '100%', marginBottom: '30px', paddingLeft: 0, paddingRight: 0 }}
          >
            <NetworkVisualization viewport={viewport} />
            <div style={{ display: 'flex', justifyContent: 'center', marginTop: '12px' }}>
              <div
                className="network-hint"
                style={{
                  textAlign: 'center',
                  display: 'inline-block',
                  backgroundColor: 'rgba(255,255,255,0.9)',
                  border: 'none',
                  borderRadius: 12,
                  padding: '8px 12px',
                  color: '#111827'
                }}
              >
                {isMobileViewport ? (
                  <>
                    Drag nodes to reposition • Pinch to zoom • Drag to pan • Long-press a node or relationship for more information
                  </>
                ) : (
                  <>
                    Drag nodes to reposition • Scroll to zoom • Drag to pan
                    <span style={{ display: 'block', marginTop: 4 }}>
                      Single-click to expand a node • Double-click to clear and start a new search with this node
                    </span>
                    <span style={{ display: 'block', marginTop: 4 }}>
                      Right-click on a node or relationship for more information
                    </span>
                  </>
                )}
              </div>
            </div>
          </div>
        )}

        {currentView === 'results' && searchResults.length === 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '20px' }}>
            <div style={{ display: 'inline-block', backgroundColor: '#ffffff', color: '#000000', padding: '10px 16px', borderRadius: '8px' }}>
              <p style={{ fontSize: '18px', color: '#000000', margin: 0 }}>No search results to display.</p>
            </div>
            {/* Back to Search button removed as requested */}
          </div>
        )}
        {currentView === 'network' && itemDetails && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
              <h2 style={{ fontSize: '24px', fontWeight: 'bold', backgroundColor: '#ffffff', padding: '6px 10px', borderRadius: '8px' }}>
                {searchType === 'singers' && itemDetails.center ? itemDetails.center.full_name : 
                 searchType === 'operas' && itemDetails.opera ? itemDetails.opera.opera_name :
                 searchType === 'books' && itemDetails.book ? itemDetails.book.title :
                 selectedItem.name || (selectedItem.properties && selectedItem.properties.title)} - Details
              </h2>
            </div>

            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
              gap: '20px'
            }}>
              {searchType === 'singers' && itemDetails.center && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    👤 Singer Profile
                  </h3>
                  <p style={{ margin: '8px 0' }}>
                    <strong>Name:</strong> {itemDetails.center.full_name}
                  </p>
                  {itemDetails.center.voice_type && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Voice type:</strong> {itemDetails.center.voice_type}
                    </p>
                  )}
                  {(itemDetails.center.birth_year || itemDetails.center.death_year || itemDetails.center.birth || itemDetails.center.death) && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Dates:</strong> {
                        itemDetails.center.birth_year && itemDetails.center.death_year
                          ? `${itemDetails.center.birth_year}-${itemDetails.center.death_year}`
                          : itemDetails.center.birth_year
                          ? `${itemDetails.center.birth_year}-`
                          : itemDetails.center.death_year
                          ? `-${itemDetails.center.death_year}`
                          : (itemDetails.center.birth && itemDetails.center.death)
                          ? `${itemDetails.center.birth.low}-${itemDetails.center.death.low}`
                          : itemDetails.center.birth
                          ? `${itemDetails.center.birth.low}-`
                          : itemDetails.center.death
                          ? `-${itemDetails.center.death.low}`
                          : ''
                      }
                    </p>
                  )}
                  {(itemDetails.center.birthplace || itemDetails.center.citizen) && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Birthplace:</strong> {itemDetails.center.birthplace || itemDetails.center.citizen}
                    </p>
                  )}
                  {(itemDetails.center.spotify_link || itemDetails.center.youtube_search) && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Spotify:</strong>{' '}
                      <a
                        href={itemDetails.center.spotify_link || itemDetails.center.youtube_search}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                          color: '#2563eb',
                          textDecoration: 'underline',
                          overflowWrap: 'anywhere',
                          wordBreak: 'break-word',
                          display: 'inline-block'
                        }}
                        onMouseOver={(e) => (e.target.style.color = '#1d4ed8')}
                        onMouseOut={(e) => (e.target.style.color = '#2563eb')}
                      >
                        {itemDetails.center.spotify_link || itemDetails.center.youtube_search}
                      </a>
                    </p>
                  )}
                  {itemDetails.center.underrepresented_group && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Underrepresented group:</strong> {itemDetails.center.underrepresented_group}
                    </p>
                  )}
                  
                  {/* Sources section */}
                  <div style={{ marginTop: '15px', paddingTop: '15px', borderTop: '1px solid #e5e7eb' }}>
                    {(itemDetails.center.spelling_source_text || itemDetails.center.spelling_source_url || itemDetails.center.spelling_source) && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Spelling source:{' '}
                        {renderRelationshipSourceLink(
                          itemDetails.center.spelling_source_text,
                          itemDetails.center.spelling_source_url,
                          itemDetails.center.spelling_source
                        )}
                      </p>
                    )}
                    {(itemDetails.center.voice_type_source_text || itemDetails.center.voice_type_source_url || itemDetails.center.voice_type_source) && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Voice type source:{' '}
                        {renderRelationshipSourceLink(
                          itemDetails.center.voice_type_source_text,
                          itemDetails.center.voice_type_source_url,
                          itemDetails.center.voice_type_source
                        )}
                      </p>
                    )}
                    {(itemDetails.center.dates_source_text || itemDetails.center.dates_source_url || itemDetails.center.dates_source) && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Dates source:{' '}
                        {renderRelationshipSourceLink(
                          itemDetails.center.dates_source_text,
                          itemDetails.center.dates_source_url,
                          itemDetails.center.dates_source
                        )}
                      </p>
                    )}
                    {(itemDetails.center.birthplace_source_text || itemDetails.center.birthplace_source_url || itemDetails.center.birthplace_source) && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Birthplace source:{' '}
                        {renderRelationshipSourceLink(
                          itemDetails.center.birthplace_source_text,
                          itemDetails.center.birthplace_source_url,
                          itemDetails.center.birthplace_source
                        )}
                      </p>
                    )}
                    {(itemDetails.center.underrepresented_source_text || itemDetails.center.underrepresented_source_url || itemDetails.center.underrepresented_source) && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Underrepresented group source:{' '}
                        {renderRelationshipSourceLink(
                          itemDetails.center.underrepresented_source_text,
                          itemDetails.center.underrepresented_source_url,
                          itemDetails.center.underrepresented_source
                        )}
                      </p>
                    )}
                  </div>
                  </div>
                </div>
              )}

              {searchType === 'singers' && itemDetails.teachers && itemDetails.teachers.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
              👤 Teachers ({itemDetails.teachers.length})
                  </h3>
                  {(() => {
                    if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                      window.__CMG_CARD_SOURCES = window.__CMG_CARD_SOURCES || {};
                      window.__CMG_CARD_SOURCES.teachers = [];
                    }
                    return itemDetails.teachers;
                  })().map((teacher, index) => {
                    const relationshipSourceArgs = [
                      { text: teacher.teacher_rel_source_text, url: teacher.teacher_rel_source_url },
                      teacher.teacher_rel_source_text,
                      teacher.teacher_rel_source_url,
                      teacher.relationshipSourceDisplay,
                      teacher.teacher_rel_source,
                      teacher.relationship_source,
                      teacher.source
                    ];
                    const derivedSourceText = deriveRelationshipSourceText(...relationshipSourceArgs);
                    const derivedSourceUrl = deriveRelationshipSourceUrl(...relationshipSourceArgs);
                    const relationshipSourceContent = renderRelationshipSourceLink(...relationshipSourceArgs);
                    if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                      window.__CMG_CARD_SOURCES.teachers.push({
                        full_name: teacher.full_name,
                        teacher_rel_source_text: teacher.teacher_rel_source_text,
                        teacher_rel_source_url: teacher.teacher_rel_source_url,
                        derivedSourceText,
                        derivedSourceUrl,
                        hasRenderedContent: Boolean(relationshipSourceContent)
                      });
                    }
                    return (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.teachers.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        pushHistory('card-click-teacher');
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(teacher.full_name);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{teacher.full_name}</p>
                      {teacher.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {teacher.voice_type}
                        </p>
                      )}
                      {(teacher.birth_year || teacher.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Dates:</strong> {
                            teacher.birth_year && teacher.death_year
                              ? `${teacher.birth_year} - ${teacher.death_year}`
                              : teacher.birth_year
                              ? `${teacher.birth_year} - `
                              : teacher.death_year
                              ? ` - ${teacher.death_year}`
                              : ''
                          }
                        </p>
                      )}
                      {relationshipSourceContent && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                          Relationship source:{' '}
                          {relationshipSourceContent}
                        </p>
                      )}
                    </div>
                    );
                  })}
                  </div>
                </div>
              )}

              {searchType === 'singers' && itemDetails.students && itemDetails.students.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                  👤 Students ({itemDetails.students.length})
                  </h3>
                  {(() => {
                    if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                      window.__CMG_CARD_SOURCES = window.__CMG_CARD_SOURCES || {};
                      window.__CMG_CARD_SOURCES.students = [];
                    }
                    return itemDetails.students;
                  })().map((student, index) => {
                    const relationshipSourceArgs = [
                      { text: student.teacher_rel_source_text, url: student.teacher_rel_source_url },
                      student.teacher_rel_source_text,
                      student.teacher_rel_source_url,
                      student.relationshipSourceDisplay,
                      student.teacher_rel_source,
                      student.relationship_source,
                      student.source
                    ];
                    const derivedSourceText = deriveRelationshipSourceText(...relationshipSourceArgs);
                    const derivedSourceUrl = deriveRelationshipSourceUrl(...relationshipSourceArgs);
                    const relationshipSourceContent = renderRelationshipSourceLink(...relationshipSourceArgs);
                    if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                      window.__CMG_CARD_SOURCES.students.push({
                        full_name: student.full_name,
                        teacher_rel_source_text: student.teacher_rel_source_text,
                        teacher_rel_source_url: student.teacher_rel_source_url,
                        derivedSourceText,
                        derivedSourceUrl,
                        hasRenderedContent: Boolean(relationshipSourceContent)
                      });
                    }
                    return (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.students.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        pushHistory('card-click-student');
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(student.full_name);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{student.full_name}</p>
                      {student.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {student.voice_type}
                        </p>
                      )}
                      {(student.birth_year || student.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                  <strong>Dates:</strong> {
                            student.birth_year && student.death_year
                              ? `${student.birth_year}-${student.death_year}`
                              : student.birth_year
                              ? `${student.birth_year}-`
                              : student.death_year
                              ? `-${student.death_year}`
                              : ''
                            }
                  </p>
                      )}
                      {relationshipSourceContent && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                          Relationship source:{' '}
                          {relationshipSourceContent}
                        </p>
                      )}
                    </div>
                    );
                  })}
                  </div>
                </div>
              )}

              {(() => { const fam = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : []; return fam.length > 0; })() && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    {(() => { const fam = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : []; return `👤 Family (${fam.length})`; })()}
                  </h3>
                  {(() => {
                    const fam = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : [];
                    if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                      window.__CMG_CARD_SOURCES = window.__CMG_CARD_SOURCES || {};
                      window.__CMG_CARD_SOURCES.family = [];
                    }
                    return fam;
                  })().map((relative, index) => {
                    const relationshipSourceArgs = [
                      { text: relative.teacher_rel_source_text, url: relative.teacher_rel_source_url },
                      relative.teacher_rel_source_text,
                      relative.teacher_rel_source_url,
                      relative.relationshipSourceDisplay,
                      relative.teacher_rel_source,
                      relative.relationship_source,
                      relative.source
                    ];
                    const derivedSourceText = deriveRelationshipSourceText(...relationshipSourceArgs);
                    const derivedSourceUrl = deriveRelationshipSourceUrl(...relationshipSourceArgs);
                    const relationshipSourceContent = renderRelationshipSourceLink(...relationshipSourceArgs);
                    if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                      window.__CMG_CARD_SOURCES.family.push({
                        full_name: relative.full_name,
                        relationship_type: relative.relationship_type,
                        teacher_rel_source_text: relative.teacher_rel_source_text,
                        teacher_rel_source_url: relative.teacher_rel_source_url,
                        derivedSourceText,
                        derivedSourceUrl,
                        hasRenderedContent: Boolean(relationshipSourceContent)
                      });
                    }
                    return (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: (() => { const fam = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : []; return index < fam.length - 1 ? '1px solid #e5e7eb' : 'none'; })(),
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        pushHistory('card-click-family');
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(relative.full_name);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{relative.full_name}</p>
                      {relative.relationship_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Relationship:</strong> {relative.relationship_type}
                        </p>
                      )}
                      {relative.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {relative.voice_type}
                        </p>
                      )}
                      {(relative.birth_year || relative.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                  <strong>Dates:</strong> {
                            relative.birth_year && relative.death_year
                              ? `${relative.birth_year}-${relative.death_year}`
                              : relative.birth_year
                              ? `${relative.birth_year}-`
                              : relative.death_year
                              ? `-${relative.death_year}`
                              : ''
                            }
                  </p>
                      )}
                      {relationshipSourceContent && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                          Relationship source:{' '}
                          {relationshipSourceContent}
                        </p>
                      )}
                    </div>
                    );
                  })}
                  </div>
                </div>
              )}

              {/* Roles premiered card */}
              {searchType === 'singers' && itemDetails.premieredRoles && itemDetails.premieredRoles.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    🎭 Operas Premiered ({itemDetails.premieredRoles.length})
                  </h3>
                  {itemDetails.premieredRoles.map((role, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.premieredRoles.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={async () => {
                        pushHistory('card-click-opera-premiered');
                        try {
                          setLoading(true);
                          const typedIdRaw = role?.id || (role?.opera_id ? `opera:${role.opera_id}` : '');
                          const payload = { operaName: role.opera_name };
                          if (role?.opera_id) {
                            payload.operaId = String(role.opera_id).trim();
                            payload.opera_id = String(role.opera_id).trim();
                          } else if (typedIdRaw) {
                            const { type: payloadType, value: payloadValue } = parseTypedId(typedIdRaw);
                            if (payloadType === 'opera' && payloadValue) {
                              payload.operaId = payloadValue;
                              payload.opera_id = payloadValue;
                            }
                          }
                          const response = await fetch(`${API_BASE}/opera/details`, {
                            method: 'POST',
                            headers: {
                              'Content-Type': 'application/json',
                              'Authorization': `Bearer ${token}`
                            },
                            body: JSON.stringify(payload)
                          });
                          const rateInfo = handleRateLimitResponse(response);
                          if (rateInfo) {
                            throw new Error(rateInfo.message);
                          }

                          const data = await response.json();
                          if (response.ok) {
                            setItemDetails(data);
                            setSelectedItem({
                              id: typedIdRaw,
                              properties: { opera_name: role.opera_name }
                            });
                            setSearchType('operas');
                            setCurrentView('network');
                            generateNetworkFromDetails(data, role.opera_name, 'operas');
                            setShouldRunSimulation(true);
                          } else {
                            setError(data.error);
                          }
                        } catch (err) {
                          setError('Failed to fetch opera details');
                        } finally {
                          setLoading(false);
                        }
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{role.opera_name}</p>
                      {role.role && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Role premiered:</strong> {role.role}
                        </p>
                      )}
                      {(() => {
                        const roleSourceText = deriveRelationshipSourceText(
                          role.opera_source_text,
                          role.relationshipSourceDisplay,
                          role.relationship_source,
                          role.source
                        );
                        if (!roleSourceText) return null;
                        const roleSourceUrl = deriveRelationshipSourceUrl(role.opera_source_url);
                        return (
                          <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                            Source:{' '}
                            {roleSourceUrl ? (
                              <a
                                href={roleSourceUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                style={{ color: '#2563eb', textDecoration: 'underline', overflowWrap: 'anywhere', wordBreak: 'break-word', display: 'inline-block' }}
                              >
                                {roleSourceText}
                              </a>
                            ) : (
                              roleSourceText
                            )}
                          </p>
                        );
                      })()}
                    </div>
                  ))}
                  </div>
                </div>
              )}
              {searchType === 'singers' && itemDetails.works && (
                <>

                  {itemDetails.works.books && itemDetails.works.books.length > 0 && (
                    <div style={{
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      padding: 0,
                      boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                      border: '2px solid #3e96e2',
                      height: '300px',
                      overflow: 'hidden'
                    }}>
                      <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                      <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                        📚 Books ({itemDetails.works.books.length})
                      </h3>
                      {itemDetails.works.books.map((book, index) => (
                        <div 
                          key={index} 
                          style={{ 
                            marginBottom: '12px', 
                            paddingBottom: '12px', 
                            borderBottom: index < itemDetails.works.books.length - 1 ? '1px solid #e5e7eb' : 'none',
                            cursor: 'pointer',
                            padding: '8px',
                            borderRadius: '8px',
                            transition: 'background-color 0.2s'
                          }}
                          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                          onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                          onClick={async () => {
                            pushHistory('card-click-book');
                            try {
                              setLoading(true);
                            const response = await fetch(`${API_BASE}/book/details`, {
                              method: 'POST',
                              headers: {
                                'Content-Type': 'application/json',
                                'Authorization': `Bearer ${token}`
                              },
                              body: JSON.stringify({ bookTitle: book.title })
                            });
                            const rateInfo = handleRateLimitResponse(response);
                            if (rateInfo) {
                              throw new Error(rateInfo.message);
                            }

                            const data = await response.json();
                              if (response.ok) {
                                setItemDetails(data);
                                setSelectedItem({ properties: { title: book.title } });
                                setSearchType('books');
                                setCurrentView('network');
                                generateNetworkFromDetails(data, book.title, 'books');
                                setShouldRunSimulation(true); // Trigger simulation for clicked book
                              } else {
                                setError(data.error);
                              }
                            } catch (err) {
                              setError('Failed to fetch book details');
                            } finally {
                              setLoading(false);
                            }
                          }}
                        >
                          <p style={{ margin: '4px 0', fontWeight: '500' }}>{book.title}</p>
                        </div>
                      ))}
                      </div>
                    </div>
                  )}

                  {itemDetails.works.editedBooks && itemDetails.works.editedBooks.length > 0 && (
                    <div style={{
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      padding: 0,
                      boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                      border: '2px solid #3e96e2',
                      height: '300px',
                      overflow: 'hidden',
                      marginTop: 16
                    }}>
                      <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                        <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                          ✏️ Edited Books ({itemDetails.works.editedBooks.length})
                        </h3>
                        {itemDetails.works.editedBooks.map((book, index) => (
                          <div
                            key={`edited-${index}`}
                            style={{
                              marginBottom: '12px',
                              paddingBottom: '12px',
                              borderBottom: index < itemDetails.works.editedBooks.length - 1 ? '1px solid #e5e7eb' : 'none',
                              cursor: 'pointer',
                              padding: '8px',
                              borderRadius: '8px',
                              transition: 'background-color 0.2s'
                            }}
                            onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                            onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                            onClick={async () => {
                              pushHistory('card-click-book-edited');
                              try {
                                setLoading(true);
                                const response = await fetch(`${API_BASE}/book/details`, {
                                  method: 'POST',
                                  headers: {
                                    'Content-Type': 'application/json',
                                    'Authorization': `Bearer ${token}`
                                  },
                                  body: JSON.stringify({ bookTitle: book.title })
                                });
                                const rateInfo = handleRateLimitResponse(response);
                                if (rateInfo) {
                                  throw new Error(rateInfo.message);
                                }
                                const data = await response.json();
                                if (response.ok) {
                                  setItemDetails(data);
                                  setSelectedItem({ properties: { title: book.title } });
                                  setSearchType('books');
                                  setCurrentView('network');
                                  generateNetworkFromDetails(data, book.title, 'books');
                                  setShouldRunSimulation(true);
                                } else {
                                  setError(data.error);
                                }
                              } catch (err) {
                                setError('Failed to fetch book details');
                              } finally {
                                setLoading(false);
                              }
                            }}
                          >
                            <p style={{ margin: '4px 0', fontWeight: '500' }}>{book.title}</p>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {itemDetails.works.composedOperas && itemDetails.works.composedOperas.length > 0 && (
                    <div style={{
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      padding: '20px',
                      boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                      height: '300px',
                      overflowY: 'auto'
                    }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                        🎼 Composed Operas ({itemDetails.works.composedOperas.length})
                      </h3>
                      {itemDetails.works.composedOperas.map((opera, index) => {
                        const operaLabel = String(opera?.title || opera?.opera_name || opera?.name || opera?.operaTitle || '').trim();
                        const safeLabel = operaLabel || `Opera ${index + 1}`;
                        return (
                        <div 
                          key={index} 
                          style={{ 
                            marginBottom: '12px', 
                            paddingBottom: '12px', 
                            borderBottom: index < itemDetails.works.composedOperas.length - 1 ? '1px solid #e5e7eb' : 'none',
                            cursor: 'pointer',
                            padding: '8px',
                            borderRadius: '8px',
                            transition: 'background-color 0.2s'
                          }}
                          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                          onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                          onClick={async () => {
                            pushHistory('card-click-opera-composed');
                        try {
                          setLoading(true);
                          const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
                          const payload = { operaName: safeLabel };
                          if (opera?.opera_id) {
                            const operaIdString = String(opera.opera_id).trim();
                            payload.operaId = operaIdString;
                            payload.opera_id = operaIdString;
                          } else if (typedIdRaw) {
                            const { type: payloadType, value: payloadValue } = parseTypedId(typedIdRaw);
                            if (payloadType === 'opera' && payloadValue) {
                              payload.operaId = payloadValue;
                              payload.opera_id = payloadValue;
                            }
                          }
                          const response = await fetch(`${API_BASE}/opera/details`, {
                            method: 'POST',
                            headers: {
                              'Content-Type': 'application/json',
                              'Authorization': `Bearer ${token}`
                            },
                            body: JSON.stringify(payload)
                          });
                          const rateInfo = handleRateLimitResponse(response);
                          if (rateInfo) {
                            throw new Error(rateInfo.message);
                          }

                          const data = await response.json();
                          if (response.ok) {
                                setItemDetails(data);
                                setSelectedItem({
                                  id: typedIdRaw,
                                  properties: { title: safeLabel }
                                });
                                setSearchType('operas');
                                setCurrentView('network');
                                generateNetworkFromDetails(data, safeLabel, 'operas');
                                setShouldRunSimulation(true); // Trigger simulation for clicked composed opera
                              } else {
                                setError(data.error);
                              }
                            } catch (err) {
                              setError('Failed to fetch opera details');
                            } finally {
                              setLoading(false);
                            }
                          }}
                        >
                          <p style={{ margin: '4px 0', fontWeight: '500' }}>{safeLabel}</p>
                          {(() => {
                            const composedSourceText = deriveRelationshipSourceText(
                              opera.opera_source_text,
                              opera.relationshipSourceDisplay,
                              opera.relationship_source,
                              opera.source
                            );
                            if (!composedSourceText) return null;
                            const composedSourceUrl = deriveRelationshipSourceUrl(opera.opera_source_url);
                            return (
                              <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                                Source:{' '}
                                {composedSourceUrl ? (
                                  <a
                                    href={composedSourceUrl}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    style={{ color: '#2563eb', textDecoration: 'underline', overflowWrap: 'anywhere', wordBreak: 'break-word', display: 'inline-block' }}
                                  >
                                    {composedSourceText}
                                  </a>
                                ) : (
                                  composedSourceText
                                )}
                              </p>
                            );
                          })()}
                        </div>
                      );})}
                    </div>
                  )}
                </>
              )}

              {/* Opera detail cards */}
              {searchType === 'operas' && itemDetails.opera && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#a09602', marginBottom: '15px' }}>
                    🎵 Opera Profile
                  </h3>
                  <p style={{ margin: '8px 0' }}>
                    <strong>Title:</strong> {itemDetails.opera.opera_name}
                  </p>
                  {itemDetails.opera.composer && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Composer:</strong> 
                      <span 
                        style={{ 
                          color: '#059669', 
                          cursor: 'pointer', 
                          textDecoration: 'underline',
                          marginLeft: '4px'
                        }}
                        onClick={() => {
                          // Store the current opera for context
                          const currentOpera = {
                            name: itemDetails.opera.opera_name,
                            composer: itemDetails.opera.composer
                          };
                          searchForPersonFromOpera(itemDetails.opera.composer, currentOpera);
                        }}
                        onMouseOver={(e) => e.target.style.color = '#047857'}
                        onMouseOut={(e) => e.target.style.color = '#059669'}
                      >
                        {itemDetails.opera.composer}
                      </span>
                    </p>
                  )}
                  {itemDetails.opera.premiere_year && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Premiere:</strong> {itemDetails.opera.premiere_year}
                    </p>
                  )}
                  </div>
                </div>
              )}

              {/* Opera Composer card (Wrote) */}
              {searchType === 'operas' && ((itemDetails.opera && itemDetails.opera.composer) || (Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0)) && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: '20px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflowY: 'auto'
                }}>
                <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    ✍️ Wrote ({Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0 ? itemDetails.wrote.length : 1})
                  </h3>
                  {Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0 ? (
                    itemDetails.wrote.map((row, index) => (
                      <div key={index}
                        style={{ marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.wrote.length - 1 ? '1px solid #e5e7eb' : 'none', cursor: 'pointer', padding: '8px', borderRadius: '8px', transition: 'background-color 0.2s' }}
                        onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                        onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                        onClick={() => {
                          const name = row && (row.composer || row.name || row.full_name);
                          if (!name) return;
                          setSearchType('singers');
                          searchForPerson(name);
                        }}
                      >
                        <p style={{ margin: '4px 0', fontWeight: '500' }}>{row && (row.composer || row.name || row.full_name)}</p>
                      </div>
                    ))
                  ) : (
                    <div 
                      style={{ marginBottom: '12px', paddingBottom: '12px', borderBottom: 'none', cursor: 'pointer', padding: '8px', borderRadius: '8px', transition: 'background-color 0.2s' }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => { setSearchType('singers'); searchForPerson(itemDetails.opera.composer); }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{itemDetails.opera.composer}</p>
                    </div>
                  )}
                </div>
              )}


              {/* Opera Roles premiered card */}
              {searchType === 'operas' && itemDetails.premieredRoles && itemDetails.premieredRoles.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                  👤 Roles Premiered ({itemDetails.premieredRoles.length})
                  </h3>
                  {itemDetails.premieredRoles.map((performer, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.premieredRoles.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(performer.singer);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{performer.singer}</p>
                      {performer.role && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Role premiered:</strong> {performer.role}
                        </p>
                      )}
                      {performer.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {performer.voice_type}
                        </p>
                      )}
                      {(() => {
                        const sourceNode = renderRelationshipSourceLink(
                          performer.opera_source_text,
                          performer.opera_source_url,
                          performer.relationshipSourceDisplay,
                          performer.source,
                          performer.relationship_source
                        );
                        if (!sourceNode) return null;
                        return (
                          <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                            Source: {sourceNode}
                          </p>
                        );
                      })()}
                    </div>
                  ))}
                  </div>
                </div>
              )}
              {/* Book detail cards */}
              {searchType === 'books' && itemDetails.book && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    📚 Book Profile
                  </h3>
                  <p style={{ margin: '8px 0' }}>
                    <strong>Title:</strong> {itemDetails.book.title}
                  </p>
                  {itemDetails.book.type && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Type:</strong> {itemDetails.book.type}
                    </p>
                  )}
                  {itemDetails.book.link && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Link:</strong> 
                      <a 
                        href={itemDetails.book.link} 
                        target="_blank" 
                        rel="noopener noreferrer"
                        style={{ 
                          color: '#059669', 
                          textDecoration: 'underline',
                          marginLeft: '4px'
                        }}
                      >
                        View Book
                      </a>
                    </p>
                  )}
                  </div>
                </div>
              )}

              {/* Book Authors card */}
              {searchType === 'books' && itemDetails.authors && itemDetails.authors.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    ✍️ Authors ({itemDetails.authors.length})
                  </h3>
                  {itemDetails.authors.map((author, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.authors.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(author.author);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{author.author}</p>
                      {author.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {author.voice_type}
                        </p>
                      )}
                    </div>
                  ))}
                  </div>
                </div>
              )}

              {/* Book Editors card */}
              {searchType === 'books' && itemDetails.editors && itemDetails.editors.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#a09602', marginBottom: '15px' }}>
                    ✏️ Editors ({itemDetails.editors.length})
                  </h3>
                  {itemDetails.editors.map((editor, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.editors.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(editor.editor);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{editor.editor}</p>
                      {editor.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {editor.voice_type}
                        </p>
                      )}
                    </div>
                  ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </main>
      {currentView === 'network' && isMobileViewport && !(showFilterPanel || showPathPanel) && (
        <>
          <div className="mobile-toolbar" role="toolbar">
            <button
              type="button"
              className="mobile-toolbar__button"
              onClick={() => {
                if (historyCounts.past > 0) {
                  goBack();
                }
              }}
              disabled={historyCounts.past === 0}
              style={{
                opacity: historyCounts.past === 0 ? 0.5 : 1,
                cursor: historyCounts.past === 0 ? 'not-allowed' : 'pointer'
              }}
            >
              Back
            </button>
            <button
              type="button"
              className="mobile-toolbar__button"
              onClick={() => {
                if (historyCounts.future > 0) {
                  goForward();
                }
              }}
              disabled={historyCounts.future === 0}
              style={{
                opacity: historyCounts.future === 0 ? 0.5 : 1,
                cursor: historyCounts.future === 0 ? 'not-allowed' : 'pointer'
              }}
            >
              Forward
            </button>
            <button
              type="button"
              className="mobile-toolbar__button"
              onClick={() => {
                toggleFilterPanel();
              }}
            >
              Filters
            </button>
          </div>
        </>
      )}
      {currentView === 'search' && !isMobileViewport && (
        <footer style={{ position: 'fixed', bottom: 0, left: 0, width: '100%', padding: '8px 12px', color: '#e5e7eb', fontSize: 12, textAlign: 'right' }}>
          Photo by <a href="https://unsplash.com/@fortuitousfoto?utm_content=creditCopyText&utm_medium=referral&utm_source=unsplash" target="_blank" rel="noopener noreferrer" style={{ color: '#e5e7eb', textDecoration: 'underline' }}>Richard Hedrick</a> on <a href="https://unsplash.com/photos/a-group-of-tall-trees-with-yellow-leaves-VcrxHU4iSgM?utm_content=creditCopyText&utm_medium=referral&utm_source=unsplash" target="_blank" rel="noopener noreferrer" style={{ color: '#e5e7eb', textDecoration: 'underline' }}>Unsplash</a>
        </footer>
      )}
      {/* Filter Panel */}
      <FilterPanel />

      
    </div>
  );
};


export default ClassicalMusicGenealogy;
