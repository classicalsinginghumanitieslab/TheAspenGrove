import React from 'react';
import { normalizeSourceValue, deriveRelationshipSourceText } from './normalization';
import {
  URL_DETECT_REGEX,
  TRAILING_PUNCTUATION_REGEX,
  DOMAIN_DETECT_REGEX,
  isDebugRelSourcesEnabled,
  isProbablyHttpUrl,
  extractFirstUrlFromValue,
} from './urlUtils';

const SOURCE_LINK_STYLE = {
  color: '#2563eb',
  textDecoration: 'underline',
  overflowWrap: 'anywhere',
  wordBreak: 'break-word',
  display: 'inline-block'
};

export const renderRelationshipSourceLink = (...values) => {
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
