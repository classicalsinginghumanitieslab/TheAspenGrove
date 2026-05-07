import React from 'react';

const SupportPanel = ({ show, onClose, isMobileViewport, headerWidth }) => {
  if (!isMobileViewport) {
    return (
      <div
        style={{
          position: 'fixed',
          left: '50%',
          bottom: show ? '40px' : '-1000px',
          transform: 'translateX(-50%)',
          width: headerWidth ? `${headerWidth}px` : 'min(1200px, calc(100vw - 40px))',
          backgroundColor: '#ffffff',
          borderRadius: '16px',
          border: '2px solid #3e96e2',
          boxShadow: '0 18px 44px rgba(0,0,0,0.25)',
          padding: '24px 28px',
          display: 'grid',
          gridTemplateColumns: 'minmax(0, 1fr) auto auto',
          columnGap: '24px',
          rowGap: '16px',
          alignItems: 'start',
          transition: 'bottom 0.6s cubic-bezier(0.22, 1, 0.36, 1)',
          zIndex: 1200,
          opacity: show ? 1 : 0,
          pointerEvents: show ? 'auto' : 'none'
        }}
      >
        <div
          style={{
            gridColumn: '1',
            gridRow: '1',
            display: 'flex',
            flexDirection: 'column',
            gap: '12px',
            minWidth: 0
          }}
        >
          <h3 style={{ margin: 0, fontSize: '22px', color: '#0f172a' }}>
            Support The Aspen Grove of Opera Singers
          </h3>
          <p style={{ margin: 0, fontSize: '16px', color: '#374151', lineHeight: 1.5 }}>
            We depend on your support to maintain and grow the Aspen Grove. Proceeds from your donation pay for hosting costs and the ability to hire an assistant. Server costs are modest, but ongoing. To help with these and to keep the site ad free, a suggested $10/year donation is incredibly appreciated. Any amount is a great help. Thank you!
          </p>
        </div>
        <div
          style={{
            gridColumn: '2',
            gridRow: '1',
            justifySelf: 'end',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            gap: '12px'
          }}
        >
          <img
            src="/paypal.png"
            alt="Support via PayPal"
            style={{
              width: '160px',
              height: '160px',
              objectFit: 'contain',
              border: '2px solid #3e96e2',
              borderRadius: '12px',
              padding: '8px',
              backgroundColor: '#f9fafb'
            }}
          />
          <a
            href="https://www.paypal.biz/sethkeetonvoice"
            target="_blank"
            rel="noopener noreferrer"
            style={{
              width: '160px',
              display: 'inline-flex',
              justifyContent: 'center',
              alignItems: 'center',
              padding: '12px 16px',
              backgroundColor: '#ffffff',
              color: '#374151',
              border: '2px solid #3e96e2',
              borderRadius: '8px',
              cursor: 'pointer',
              fontSize: '16px',
              fontWeight: 600,
              textDecoration: 'none',
              textAlign: 'center',
              boxSizing: 'border-box'
            }}
          >
            Link to Paypal
          </a>
        </div>
        <button
          onClick={onClose}
          style={{
            gridColumn: '3',
            gridRow: '1',
            justifySelf: 'end',
            alignSelf: 'start',
            background: 'none',
            border: 'none',
            color: '#1f2937',
            fontSize: '22px',
            cursor: 'pointer',
            fontWeight: 600,
            padding: 0
          }}
          aria-label="Hide support panel"
        >
          ×
        </button>
      </div>
    );
  }

  return (
    <>
      <div
        className={`mobile-overlay-backdrop${show ? ' is-open' : ''}`}
        onClick={onClose}
        style={{ zIndex: 1199 }}
      />
      <div
        className={`mobile-sheet${show ? ' is-open' : ''}`}
        style={{ paddingBottom: '24px', zIndex: 1200 }}
        aria-label="Support panel"
      >
        <div className="mobile-sheet__header" style={{ paddingBottom: 0 }}>
          <div className="mobile-sheet__handle" />
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
            <h3 style={{ margin: 0, fontSize: '20px', fontWeight: 600, color: '#0f172a' }}>
              Support The Aspen Grove of Opera Singers
            </h3>
            <button
              onClick={onClose}
              style={{
                background: 'none',
                border: 'none',
                color: '#1f2937',
                fontSize: '22px',
                cursor: 'pointer',
                fontWeight: 600,
                padding: 4,
                width: 40,
                height: 40,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
              aria-label="Hide support panel"
            >
              ×
            </button>
          </div>
        </div>
        <div className="mobile-sheet__content" style={{ paddingTop: 12 }}>
          <p style={{ margin: 0, fontSize: '16px', color: '#374151', lineHeight: 1.5 }}>
            We depend on your support to maintain and grow the Aspen Grove. Proceeds from your donation pay for hosting costs and the ability to hire an assistant. Server costs are modest, but ongoing. To help with these and to keep the site ad free, a suggested $10/year donation is incredibly appreciated. Any amount is a great help. Thank you!
          </p>
        </div>
        <div className="mobile-sheet__footer">
          <a
            className="mobile-donate-card"
            href="https://www.paypal.biz/sethkeetonvoice"
            target="_blank"
            rel="noopener noreferrer"
          >
            Link to donate on PayPal
          </a>
        </div>
      </div>
    </>
  );
};

export default SupportPanel;
