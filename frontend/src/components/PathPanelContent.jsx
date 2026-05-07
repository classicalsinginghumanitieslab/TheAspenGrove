import React from 'react';
import { renderRelationshipSourceLink } from '../utils/renderHelpers';

const PathPanelContent = ({
  isMobile,
  pathFromRef,
  pathToRef,
  pathFromValRef,
  pathToValRef,
  pathInfo,
  pathListRef,
  handleClearPath,
  onFindPath,
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

export default PathPanelContent;
