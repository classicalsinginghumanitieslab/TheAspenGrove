import React from 'react';

const DisclaimerPage = () => {
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
};

export default DisclaimerPage;
