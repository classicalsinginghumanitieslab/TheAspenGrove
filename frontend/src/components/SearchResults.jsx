import React from 'react';

const HALO_SHADOW = '0 0 12px 2px rgba(255,255,255,0.85), 0 0 18px 6px rgba(62,150,226,0.45), 0 0 22px 9px rgba(228,162,1,0.35), 0 0 28px 12px rgba(62,150,226,0.25)';
const HALO_HOVER_SHADOW = '0 0 12px 2px rgba(255,255,255,0.90), 0 0 22px 8px rgba(62,150,226,0.50), 0 0 26px 10px rgba(228,162,1,0.40), 0 10px 22px rgba(0,0,0,0.12)';
const DEFAULT_SHADOW = '0 2px 4px rgba(0,0,0,0.1)';
const DEFAULT_HOVER_SHADOW = '0 4px 8px rgba(0,0,0,0.15)';

const SearchResults = ({ searchResults, searchType, showResultsHalo, isHeaderMobile, onSelectItem }) => {
  if (!searchResults || searchResults.length === 0) return null;

  return (
    <div style={{ marginBottom: '30px', padding: isHeaderMobile ? '0 var(--cmg-mobile-inline-padding)' : 0 }}>
      <h3 style={{ display: 'inline-block', backgroundColor: '#ffffff', padding: '6px 10px', borderRadius: '8px' }}>
        Search Results ({searchResults.length})
      </h3>
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
            onClick={() => onSelectItem(item)}
            className={isHeaderMobile ? 'mobile-card mobile-search-result-card' : undefined}
            style={{
              backgroundColor: 'white',
              borderRadius: '8px',
              padding: '10px 12px',
              boxShadow: showResultsHalo ? HALO_SHADOW : DEFAULT_SHADOW,
              border: '2px solid #3e96e2',
              cursor: 'pointer',
              transition: 'box-shadow 0.35s ease',
              minHeight: '68px',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              rowGap: '2px'
            }}
            onMouseOver={(e) => e.currentTarget.style.boxShadow = showResultsHalo ? HALO_HOVER_SHADOW : DEFAULT_HOVER_SHADOW}
            onMouseOut={(e) => e.currentTarget.style.boxShadow = showResultsHalo ? HALO_SHADOW : DEFAULT_SHADOW}
          >
            <h4 style={{ margin: '0 0 2px 0', fontSize: '15px', lineHeight: 1.2 }}>
              {searchType === 'singers'
                ? (item.name || item.properties.full_name)
                : searchType === 'operas'
                ? item.properties.opera_name
                : item.properties.title}
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
  );
};

export default SearchResults;
