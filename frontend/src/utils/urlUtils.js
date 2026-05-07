// URL detection and sanitization utilities.
// Pure functions — no React dependencies.

export const URL_DETECT_REGEX = /https?:\/\/[^\s)]+/i;
export const TRAILING_PUNCTUATION_REGEX = /[),.;:]+$/g;
export const WWW_URL_REGEX = /^www\.[^\s)]+/i;
// Basic bare-domain detector (example.com, sub.example.co.uk, with optional path)
export const DOMAIN_ONLY_REGEX = /^(?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/[^[\s)]]*)?$/i;
// Detect bare domain occurrence inside text (avoid matching within words by requiring start or whitespace/paren)
export const DOMAIN_DETECT_REGEX = /(?:^|[\s(])((?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/[^[\s)]]*)?)/i;

export const isDebugRelSourcesEnabled = () =>
  typeof window !== 'undefined' && window.__CMG_DEBUG_REL_SOURCES === true;

export const isProbablyHttpUrl = (value) => {
  if (value == null) return false;
  if (typeof value !== 'string') return false;
  return /^https?:\/\//i.test(value.trim());
};

export const sanitizeUrlCandidate = (value) => {
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

export const extractFirstUrlFromValue = (value) => {
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

export const deriveRelationshipSourceUrl = (...values) => {
  for (const value of values) {
    const possibleUrl = extractFirstUrlFromValue(value);
    if (possibleUrl) return possibleUrl;
  }
  return '';
};
