import React from 'react';
import { renderRelationshipSourceLink } from '../utils/renderHelpers';

const ProfileCard = ({ show, data, onClose, isMobileViewport, showPathPanel }) => {
  if (!show || !data) return null;

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
    <div
      className={cardClassName}
      style={cardStyle}
      onClick={(e) => e.stopPropagation()}
    >
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
          onClick={onClose}
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

export default ProfileCard;
