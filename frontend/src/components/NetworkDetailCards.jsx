import React from 'react';
import { deriveRelationshipSourceText } from '../utils/normalization';
import { isDebugRelSourcesEnabled, deriveRelationshipSourceUrl } from '../utils/urlUtils';
import { renderRelationshipSourceLink } from '../utils/renderHelpers';

const CARD_STYLE = {
  backgroundColor: 'white',
  borderRadius: '8px',
  padding: 0,
  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
  border: '2px solid #3e96e2',
  height: '300px',
  overflow: 'hidden',
};

const CARD_INNER = { height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' };
const CARD_HEADER = { fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' };

const ROW_STYLE_BASE = {
  cursor: 'pointer',
  padding: '8px',
  borderRadius: '8px',
  transition: 'background-color 0.2s',
};

const hoverOn = (e) => (e.currentTarget.style.backgroundColor = '#f3f4f6');
const hoverOff = (e) => (e.currentTarget.style.backgroundColor = 'transparent');

const NetworkDetailCards = ({
  itemDetails,
  searchType,
  selectedItem,
  pushHistory,
  searchForPerson,
  searchForPersonFromOpera,
  setSearchType,
  setLoading,
  setError,
  setItemDetails,
  setSelectedItem,
  setCurrentView,
  generateNetworkFromDetails,
  setShouldRunSimulation,
  API_BASE,
  token,
  handleRateLimitResponse,
  parseTypedId,
}) => {
  if (!itemDetails) return null;

  const title =
    searchType === 'singers' && itemDetails.center
      ? itemDetails.center.full_name
      : searchType === 'operas' && itemDetails.opera
      ? itemDetails.opera.opera_name
      : searchType === 'books' && itemDetails.book
      ? itemDetails.book.title
      : selectedItem?.name || (selectedItem?.properties && selectedItem.properties.title);

  const family = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : [];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
        <h2 style={{ fontSize: '24px', fontWeight: 'bold', backgroundColor: '#ffffff', padding: '6px 10px', borderRadius: '8px' }}>
          {title} - Details
        </h2>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '20px' }}>

        {/* Singer Profile */}
        {searchType === 'singers' && itemDetails.center && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={CARD_HEADER}>👤 Singer Profile</h3>
              <p style={{ margin: '8px 0' }}><strong>Name:</strong> {itemDetails.center.full_name}</p>
              {itemDetails.center.voice_type && (
                <p style={{ margin: '8px 0' }}><strong>Voice type:</strong> {itemDetails.center.voice_type}</p>
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
                <p style={{ margin: '8px 0' }}><strong>Birthplace:</strong> {itemDetails.center.birthplace || itemDetails.center.citizen}</p>
              )}
              {(itemDetails.center.spotify_link || itemDetails.center.youtube_search) && (
                <p style={{ margin: '8px 0' }}>
                  <strong>Spotify:</strong>{' '}
                  <a
                    href={itemDetails.center.spotify_link || itemDetails.center.youtube_search}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ color: '#2563eb', textDecoration: 'underline', overflowWrap: 'anywhere', wordBreak: 'break-word', display: 'inline-block' }}
                    onMouseOver={(e) => (e.target.style.color = '#1d4ed8')}
                    onMouseOut={(e) => (e.target.style.color = '#2563eb')}
                  >
                    {itemDetails.center.spotify_link || itemDetails.center.youtube_search}
                  </a>
                </p>
              )}
              {itemDetails.center.underrepresented_group && (
                <p style={{ margin: '8px 0' }}><strong>Underrepresented group:</strong> {itemDetails.center.underrepresented_group}</p>
              )}
              <div style={{ marginTop: '15px', paddingTop: '15px', borderTop: '1px solid #e5e7eb' }}>
                {(itemDetails.center.spelling_source_text || itemDetails.center.spelling_source_url || itemDetails.center.spelling_source) && (
                  <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                    Spelling source:{' '}
                    {renderRelationshipSourceLink(itemDetails.center.spelling_source_text, itemDetails.center.spelling_source_url, itemDetails.center.spelling_source)}
                  </p>
                )}
                {(itemDetails.center.voice_type_source_text || itemDetails.center.voice_type_source_url || itemDetails.center.voice_type_source) && (
                  <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                    Voice type source:{' '}
                    {renderRelationshipSourceLink(itemDetails.center.voice_type_source_text, itemDetails.center.voice_type_source_url, itemDetails.center.voice_type_source)}
                  </p>
                )}
                {(itemDetails.center.dates_source_text || itemDetails.center.dates_source_url || itemDetails.center.dates_source) && (
                  <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                    Dates source:{' '}
                    {renderRelationshipSourceLink(itemDetails.center.dates_source_text, itemDetails.center.dates_source_url, itemDetails.center.dates_source)}
                  </p>
                )}
                {(itemDetails.center.birthplace_source_text || itemDetails.center.birthplace_source_url || itemDetails.center.birthplace_source) && (
                  <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                    Birthplace source:{' '}
                    {renderRelationshipSourceLink(itemDetails.center.birthplace_source_text, itemDetails.center.birthplace_source_url, itemDetails.center.birthplace_source)}
                  </p>
                )}
                {(itemDetails.center.underrepresented_source_text || itemDetails.center.underrepresented_source_url || itemDetails.center.underrepresented_source) && (
                  <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                    Underrepresented group source:{' '}
                    {renderRelationshipSourceLink(itemDetails.center.underrepresented_source_text, itemDetails.center.underrepresented_source_url, itemDetails.center.underrepresented_source)}
                  </p>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Teachers */}
        {searchType === 'singers' && itemDetails.teachers && itemDetails.teachers.length > 0 && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={CARD_HEADER}>👤 Teachers ({itemDetails.teachers.length})</h3>
              {(() => {
                if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                  window.__CMG_CARD_SOURCES = window.__CMG_CARD_SOURCES || {};
                  window.__CMG_CARD_SOURCES.teachers = [];
                }
                return itemDetails.teachers;
              })().map((teacher, index) => {
                const args = [
                  { text: teacher.teacher_rel_source_text, url: teacher.teacher_rel_source_url },
                  teacher.teacher_rel_source_text, teacher.teacher_rel_source_url,
                  teacher.relationshipSourceDisplay, teacher.teacher_rel_source,
                  teacher.relationship_source, teacher.source
                ];
                const derivedSourceText = deriveRelationshipSourceText(...args);
                const derivedSourceUrl = deriveRelationshipSourceUrl(...args);
                const sourceContent = renderRelationshipSourceLink(...args);
                if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                  window.__CMG_CARD_SOURCES.teachers.push({ full_name: teacher.full_name, teacher_rel_source_text: teacher.teacher_rel_source_text, teacher_rel_source_url: teacher.teacher_rel_source_url, derivedSourceText, derivedSourceUrl, hasRenderedContent: Boolean(sourceContent) });
                }
                return (
                  <div key={index}
                    style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.teachers.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                    onMouseOver={hoverOn} onMouseOut={hoverOff}
                    onClick={() => { pushHistory('card-click-teacher'); setSearchType('singers'); searchForPerson(teacher.full_name); }}
                  >
                    <p style={{ margin: '4px 0', fontWeight: '500' }}>{teacher.full_name}</p>
                    {teacher.voice_type && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Voice type:</strong> {teacher.voice_type}</p>}
                    {(teacher.birth_year || teacher.death_year) && (
                      <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                        <strong>Dates:</strong> {teacher.birth_year && teacher.death_year ? `${teacher.birth_year} - ${teacher.death_year}` : teacher.birth_year ? `${teacher.birth_year} - ` : ` - ${teacher.death_year}`}
                      </p>
                    )}
                    {sourceContent && <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>Relationship source: {sourceContent}</p>}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Students */}
        {searchType === 'singers' && itemDetails.students && itemDetails.students.length > 0 && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={CARD_HEADER}>👤 Students ({itemDetails.students.length})</h3>
              {(() => {
                if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                  window.__CMG_CARD_SOURCES = window.__CMG_CARD_SOURCES || {};
                  window.__CMG_CARD_SOURCES.students = [];
                }
                return itemDetails.students;
              })().map((student, index) => {
                const args = [
                  { text: student.teacher_rel_source_text, url: student.teacher_rel_source_url },
                  student.teacher_rel_source_text, student.teacher_rel_source_url,
                  student.relationshipSourceDisplay, student.teacher_rel_source,
                  student.relationship_source, student.source
                ];
                const derivedSourceText = deriveRelationshipSourceText(...args);
                const derivedSourceUrl = deriveRelationshipSourceUrl(...args);
                const sourceContent = renderRelationshipSourceLink(...args);
                if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                  window.__CMG_CARD_SOURCES.students.push({ full_name: student.full_name, teacher_rel_source_text: student.teacher_rel_source_text, teacher_rel_source_url: student.teacher_rel_source_url, derivedSourceText, derivedSourceUrl, hasRenderedContent: Boolean(sourceContent) });
                }
                return (
                  <div key={index}
                    style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.students.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                    onMouseOver={hoverOn} onMouseOut={hoverOff}
                    onClick={() => { pushHistory('card-click-student'); setSearchType('singers'); searchForPerson(student.full_name); }}
                  >
                    <p style={{ margin: '4px 0', fontWeight: '500' }}>{student.full_name}</p>
                    {student.voice_type && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Voice type:</strong> {student.voice_type}</p>}
                    {(student.birth_year || student.death_year) && (
                      <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                        <strong>Dates:</strong> {student.birth_year && student.death_year ? `${student.birth_year}-${student.death_year}` : student.birth_year ? `${student.birth_year}-` : `-${student.death_year}`}
                      </p>
                    )}
                    {sourceContent && <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>Relationship source: {sourceContent}</p>}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Family */}
        {family.length > 0 && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={CARD_HEADER}>👤 Family ({family.length})</h3>
              {(() => {
                if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                  window.__CMG_CARD_SOURCES = window.__CMG_CARD_SOURCES || {};
                  window.__CMG_CARD_SOURCES.family = [];
                }
                return family;
              })().map((relative, index) => {
                const args = [
                  { text: relative.teacher_rel_source_text, url: relative.teacher_rel_source_url },
                  relative.teacher_rel_source_text, relative.teacher_rel_source_url,
                  relative.relationshipSourceDisplay, relative.teacher_rel_source,
                  relative.relationship_source, relative.source
                ];
                const derivedSourceText = deriveRelationshipSourceText(...args);
                const derivedSourceUrl = deriveRelationshipSourceUrl(...args);
                const sourceContent = renderRelationshipSourceLink(...args);
                if (isDebugRelSourcesEnabled() && typeof window !== 'undefined') {
                  window.__CMG_CARD_SOURCES.family.push({ full_name: relative.full_name, relationship_type: relative.relationship_type, teacher_rel_source_text: relative.teacher_rel_source_text, teacher_rel_source_url: relative.teacher_rel_source_url, derivedSourceText, derivedSourceUrl, hasRenderedContent: Boolean(sourceContent) });
                }
                return (
                  <div key={index}
                    style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < family.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                    onMouseOver={hoverOn} onMouseOut={hoverOff}
                    onClick={() => { pushHistory('card-click-family'); setSearchType('singers'); searchForPerson(relative.full_name); }}
                  >
                    <p style={{ margin: '4px 0', fontWeight: '500' }}>{relative.full_name}</p>
                    {relative.relationship_type && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Relationship:</strong> {relative.relationship_type}</p>}
                    {relative.voice_type && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Voice type:</strong> {relative.voice_type}</p>}
                    {(relative.birth_year || relative.death_year) && (
                      <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                        <strong>Dates:</strong> {relative.birth_year && relative.death_year ? `${relative.birth_year}-${relative.death_year}` : relative.birth_year ? `${relative.birth_year}-` : `-${relative.death_year}`}
                      </p>
                    )}
                    {sourceContent && <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>Relationship source: {sourceContent}</p>}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Roles Premiered (singer view) */}
        {searchType === 'singers' && itemDetails.premieredRoles && itemDetails.premieredRoles.length > 0 && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={CARD_HEADER}>🎭 Operas Premiered ({itemDetails.premieredRoles.length})</h3>
              {itemDetails.premieredRoles.map((role, index) => (
                <div key={index}
                  style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.premieredRoles.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                  onMouseOver={hoverOn} onMouseOut={hoverOff}
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
                        if (payloadType === 'opera' && payloadValue) { payload.operaId = payloadValue; payload.opera_id = payloadValue; }
                      }
                      const response = await fetch(`${API_BASE}/opera/details`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
                        body: JSON.stringify(payload)
                      });
                      const rateInfo = handleRateLimitResponse(response);
                      if (rateInfo) throw new Error(rateInfo.message);
                      const data = await response.json();
                      if (response.ok) {
                        setItemDetails(data);
                        setSelectedItem({ id: typedIdRaw, properties: { opera_name: role.opera_name } });
                        setSearchType('operas');
                        setCurrentView('network');
                        generateNetworkFromDetails(data, role.opera_name, 'operas');
                        setShouldRunSimulation(true);
                      } else { setError(data.error); }
                    } catch (err) { setError('Failed to fetch opera details'); }
                    finally { setLoading(false); }
                  }}
                >
                  <p style={{ margin: '4px 0', fontWeight: '500' }}>{role.opera_name}</p>
                  {role.role && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Role premiered:</strong> {role.role}</p>}
                  {(() => {
                    const roleSourceText = deriveRelationshipSourceText(role.opera_source_text, role.relationshipSourceDisplay, role.relationship_source, role.source);
                    if (!roleSourceText) return null;
                    const roleSourceUrl = deriveRelationshipSourceUrl(role.opera_source_url);
                    return (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                        Source:{' '}
                        {roleSourceUrl
                          ? <a href={roleSourceUrl} target="_blank" rel="noopener noreferrer" style={{ color: '#2563eb', textDecoration: 'underline', overflowWrap: 'anywhere', wordBreak: 'break-word', display: 'inline-block' }}>{roleSourceText}</a>
                          : roleSourceText}
                      </p>
                    );
                  })()}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Singer Works */}
        {searchType === 'singers' && itemDetails.works && (
          <>
            {itemDetails.works.books && itemDetails.works.books.length > 0 && (
              <div style={CARD_STYLE}>
                <div style={CARD_INNER}>
                  <h3 style={CARD_HEADER}>📚 Books ({itemDetails.works.books.length})</h3>
                  {itemDetails.works.books.map((book, index) => (
                    <div key={index}
                      style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.works.books.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                      onMouseOver={hoverOn} onMouseOut={hoverOff}
                      onClick={async () => {
                        pushHistory('card-click-book');
                        try {
                          setLoading(true);
                          const response = await fetch(`${API_BASE}/book/details`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
                            body: JSON.stringify({ bookTitle: book.title })
                          });
                          const rateInfo = handleRateLimitResponse(response);
                          if (rateInfo) throw new Error(rateInfo.message);
                          const data = await response.json();
                          if (response.ok) {
                            setItemDetails(data);
                            setSelectedItem({ properties: { title: book.title } });
                            setSearchType('books');
                            setCurrentView('network');
                            generateNetworkFromDetails(data, book.title, 'books');
                            setShouldRunSimulation(true);
                          } else { setError(data.error); }
                        } catch (err) { setError('Failed to fetch book details'); }
                        finally { setLoading(false); }
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{book.title}</p>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {itemDetails.works.editedBooks && itemDetails.works.editedBooks.length > 0 && (
              <div style={{ ...CARD_STYLE, marginTop: 16 }}>
                <div style={CARD_INNER}>
                  <h3 style={CARD_HEADER}>✏️ Edited Books ({itemDetails.works.editedBooks.length})</h3>
                  {itemDetails.works.editedBooks.map((book, index) => (
                    <div key={`edited-${index}`}
                      style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.works.editedBooks.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                      onMouseOver={hoverOn} onMouseOut={hoverOff}
                      onClick={async () => {
                        pushHistory('card-click-book-edited');
                        try {
                          setLoading(true);
                          const response = await fetch(`${API_BASE}/book/details`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
                            body: JSON.stringify({ bookTitle: book.title })
                          });
                          const rateInfo = handleRateLimitResponse(response);
                          if (rateInfo) throw new Error(rateInfo.message);
                          const data = await response.json();
                          if (response.ok) {
                            setItemDetails(data);
                            setSelectedItem({ properties: { title: book.title } });
                            setSearchType('books');
                            setCurrentView('network');
                            generateNetworkFromDetails(data, book.title, 'books');
                            setShouldRunSimulation(true);
                          } else { setError(data.error); }
                        } catch (err) { setError('Failed to fetch book details'); }
                        finally { setLoading(false); }
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{book.title}</p>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {itemDetails.works.composedOperas && itemDetails.works.composedOperas.length > 0 && (
              <div style={{ backgroundColor: 'white', borderRadius: '8px', padding: '20px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)', border: '2px solid #3e96e2', height: '300px', overflowY: 'auto' }}>
                <h3 style={CARD_HEADER}>🎼 Composed Operas ({itemDetails.works.composedOperas.length})</h3>
                {itemDetails.works.composedOperas.map((opera, index) => {
                  const operaLabel = String(opera?.title || opera?.opera_name || opera?.name || opera?.operaTitle || '').trim();
                  const safeLabel = operaLabel || `Opera ${index + 1}`;
                  return (
                    <div key={index}
                      style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.works.composedOperas.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                      onMouseOver={hoverOn} onMouseOut={hoverOff}
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
                            if (payloadType === 'opera' && payloadValue) { payload.operaId = payloadValue; payload.opera_id = payloadValue; }
                          }
                          const response = await fetch(`${API_BASE}/opera/details`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
                            body: JSON.stringify(payload)
                          });
                          const rateInfo = handleRateLimitResponse(response);
                          if (rateInfo) throw new Error(rateInfo.message);
                          const data = await response.json();
                          if (response.ok) {
                            setItemDetails(data);
                            setSelectedItem({ id: typedIdRaw, properties: { title: safeLabel } });
                            setSearchType('operas');
                            setCurrentView('network');
                            generateNetworkFromDetails(data, safeLabel, 'operas');
                            setShouldRunSimulation(true);
                          } else { setError(data.error); }
                        } catch (err) { setError('Failed to fetch opera details'); }
                        finally { setLoading(false); }
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{safeLabel}</p>
                      {(() => {
                        const composedSourceText = deriveRelationshipSourceText(opera.opera_source_text, opera.relationshipSourceDisplay, opera.relationship_source, opera.source);
                        if (!composedSourceText) return null;
                        const composedSourceUrl = deriveRelationshipSourceUrl(opera.opera_source_url);
                        return (
                          <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>
                            Source:{' '}
                            {composedSourceUrl
                              ? <a href={composedSourceUrl} target="_blank" rel="noopener noreferrer" style={{ color: '#2563eb', textDecoration: 'underline', overflowWrap: 'anywhere', wordBreak: 'break-word', display: 'inline-block' }}>{composedSourceText}</a>
                              : composedSourceText}
                          </p>
                        );
                      })()}
                    </div>
                  );
                })}
              </div>
            )}
          </>
        )}

        {/* Opera Profile */}
        {searchType === 'operas' && itemDetails.opera && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#a09602', marginBottom: '15px' }}>🎵 Opera Profile</h3>
              <p style={{ margin: '8px 0' }}><strong>Title:</strong> {itemDetails.opera.opera_name}</p>
              {itemDetails.opera.composer && (
                <p style={{ margin: '8px 0' }}>
                  <strong>Composer:</strong>
                  <span
                    style={{ color: '#059669', cursor: 'pointer', textDecoration: 'underline', marginLeft: '4px' }}
                    onClick={() => searchForPersonFromOpera(itemDetails.opera.composer, { name: itemDetails.opera.opera_name, composer: itemDetails.opera.composer })}
                    onMouseOver={(e) => (e.target.style.color = '#047857')}
                    onMouseOut={(e) => (e.target.style.color = '#059669')}
                  >
                    {itemDetails.opera.composer}
                  </span>
                </p>
              )}
              {itemDetails.opera.premiere_year && (
                <p style={{ margin: '8px 0' }}><strong>Premiere:</strong> {itemDetails.opera.premiere_year}</p>
              )}
            </div>
          </div>
        )}

        {/* Opera Wrote (composers) */}
        {searchType === 'operas' && ((itemDetails.opera && itemDetails.opera.composer) || (Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0)) && (
          <div style={{ backgroundColor: 'white', borderRadius: '8px', padding: '20px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)', border: '2px solid #3e96e2', height: '300px', overflowY: 'auto' }}>
            <h3 style={CARD_HEADER}>✍️ Wrote ({Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0 ? itemDetails.wrote.length : 1})</h3>
            {Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0 ? (
              itemDetails.wrote.map((row, index) => (
                <div key={index}
                  style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.wrote.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                  onMouseOver={hoverOn} onMouseOut={hoverOff}
                  onClick={() => { const name = row && (row.composer || row.name || row.full_name); if (!name) return; setSearchType('singers'); searchForPerson(name); }}
                >
                  <p style={{ margin: '4px 0', fontWeight: '500' }}>{row && (row.composer || row.name || row.full_name)}</p>
                </div>
              ))
            ) : (
              <div
                style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: 'none' }}
                onMouseOver={hoverOn} onMouseOut={hoverOff}
                onClick={() => { setSearchType('singers'); searchForPerson(itemDetails.opera.composer); }}
              >
                <p style={{ margin: '4px 0', fontWeight: '500' }}>{itemDetails.opera.composer}</p>
              </div>
            )}
          </div>
        )}

        {/* Opera Roles Premiered */}
        {searchType === 'operas' && itemDetails.premieredRoles && itemDetails.premieredRoles.length > 0 && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={CARD_HEADER}>👤 Roles Premiered ({itemDetails.premieredRoles.length})</h3>
              {itemDetails.premieredRoles.map((performer, index) => (
                <div key={index}
                  style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.premieredRoles.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                  onMouseOver={hoverOn} onMouseOut={hoverOff}
                  onClick={() => { setSearchType('singers'); searchForPerson(performer.singer); }}
                >
                  <p style={{ margin: '4px 0', fontWeight: '500' }}>{performer.singer}</p>
                  {performer.role && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Role premiered:</strong> {performer.role}</p>}
                  {performer.voice_type && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Voice type:</strong> {performer.voice_type}</p>}
                  {(() => {
                    const sourceNode = renderRelationshipSourceLink(performer.opera_source_text, performer.opera_source_url, performer.relationshipSourceDisplay, performer.source, performer.relationship_source);
                    if (!sourceNode) return null;
                    return <p style={{ margin: '4px 0', fontSize: '12px', color: '#888' }}>Source: {sourceNode}</p>;
                  })()}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Book Profile */}
        {searchType === 'books' && itemDetails.book && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={CARD_HEADER}>📚 Book Profile</h3>
              <p style={{ margin: '8px 0' }}><strong>Title:</strong> {itemDetails.book.title}</p>
              {itemDetails.book.type && <p style={{ margin: '8px 0' }}><strong>Type:</strong> {itemDetails.book.type}</p>}
              {itemDetails.book.link && (
                <p style={{ margin: '8px 0' }}>
                  <strong>Link:</strong>
                  <a href={itemDetails.book.link} target="_blank" rel="noopener noreferrer" style={{ color: '#059669', textDecoration: 'underline', marginLeft: '4px' }}>View Book</a>
                </p>
              )}
            </div>
          </div>
        )}

        {/* Book Authors */}
        {searchType === 'books' && itemDetails.authors && itemDetails.authors.length > 0 && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={CARD_HEADER}>✍️ Authors ({itemDetails.authors.length})</h3>
              {itemDetails.authors.map((author, index) => (
                <div key={index}
                  style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.authors.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                  onMouseOver={hoverOn} onMouseOut={hoverOff}
                  onClick={() => { setSearchType('singers'); searchForPerson(author.author); }}
                >
                  <p style={{ margin: '4px 0', fontWeight: '500' }}>{author.author}</p>
                  {author.voice_type && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Voice type:</strong> {author.voice_type}</p>}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Book Editors */}
        {searchType === 'books' && itemDetails.editors && itemDetails.editors.length > 0 && (
          <div style={CARD_STYLE}>
            <div style={CARD_INNER}>
              <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#a09602', marginBottom: '15px' }}>✏️ Editors ({itemDetails.editors.length})</h3>
              {itemDetails.editors.map((editor, index) => (
                <div key={index}
                  style={{ ...ROW_STYLE_BASE, marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.editors.length - 1 ? '1px solid #e5e7eb' : 'none' }}
                  onMouseOver={hoverOn} onMouseOut={hoverOff}
                  onClick={() => { setSearchType('singers'); searchForPerson(editor.editor); }}
                >
                  <p style={{ margin: '4px 0', fontWeight: '500' }}>{editor.editor}</p>
                  {editor.voice_type && <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}><strong>Voice type:</strong> {editor.voice_type}</p>}
                </div>
              ))}
            </div>
          </div>
        )}

      </div>
    </div>
  );
};

export default NetworkDetailCards;
