import React from 'react';
import { VOICE_TYPES } from '../constants/voiceTypes';

const ActiveFilterBar = ({ selectedVoiceTypes, toggleVoiceTypeFilter, clearAllFilters, isMobileViewport }) => {
  if (!selectedVoiceTypes || selectedVoiceTypes.size === 0) return null;

  return (
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
  );
};

export default ActiveFilterBar;
