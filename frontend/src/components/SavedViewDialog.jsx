import React, { useState } from 'react';

const SavedViewDialog = ({ show, onClose, savedViewToken, savedViewLabel, isMobileViewport }) => {
  const [copied, setCopied] = useState(false);
  const [copyMessage, setCopyMessage] = useState('');

  if (!show) return null;

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
        <div style={{ display: 'grid', gridTemplateColumns: isMobileViewport ? '1fr' : '1fr auto', gap: 8 }}>
          <textarea
            readOnly
            value={savedViewToken}
            style={{
              width: '100%',
              margin: 0,
              padding: 10,
              border: '2px solid #3e96e2',
              borderRadius: 10,
              fontFamily: 'monospace',
              fontSize: 13,
              resize: 'vertical',
              minHeight: isMobileViewport ? 100 : 60,
              color: '#111',
              boxSizing: 'border-box',
              appearance: 'none',
              WebkitAppearance: 'none',
              MozAppearance: 'none'
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
            <div style={{ display: 'grid', gridTemplateColumns: isMobileViewport ? '1fr' : '1fr auto', gap: 8 }}>
              <input
                readOnly
                value={savedViewLabel}
                style={{ width: '100%', margin: 0, padding: 10, border: '2px solid #3e96e2', borderRadius: 10, fontSize: 13, color: '#111', boxSizing: 'border-box', appearance: 'none', WebkitAppearance: 'none', MozAppearance: 'none' }}
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
        onClick={onClose}
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
          onClick={onClose}
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
                onClick={onClose}
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
      onClick={onClose}
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

export default SavedViewDialog;
