// Link context menu state and source extraction utilities.
// Pure functions — no React dependencies.

import { deriveRelationshipSourceText } from './normalization';
import { deriveRelationshipSourceUrl } from './urlUtils';

export const createLinkContextMenuState = () => ({
  show: false,
  x: 0,
  y: 0,
  role: '',
  sourceValues: [],
  sourceText: '',
  sourceUrl: ''
});

export const buildLinkContextSource = (link) => {
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
