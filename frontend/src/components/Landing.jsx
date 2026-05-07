import React from 'react';

// Landing screen shown on first visit after the login removal (2026-05-06).
// Replaces AuthForm.jsx. Shows the project description, the disclaimer text
// inline, and a single "Accept terms and Enter" button. Acceptance is stored
// in localStorage; the user only sees this on first visit per device.

const Landing = ({ onEnter, isMobileViewport }) => {
  const handleEnter = () => {
    try { localStorage.setItem('tosAccepted', '1'); } catch (_) {}
    if (typeof onEnter === 'function') onEnter();
  };

  // Match the rest of the site's aspens background.
  const backgroundStyle = {
    minHeight: '100vh',
    width: '100%',
    backgroundImage: 'url(/aspens_2000.jpg)',
    backgroundSize: 'cover',
    backgroundPosition: 'center center',
    backgroundRepeat: 'no-repeat',
    backgroundAttachment: 'fixed',
  };

  // Logo size (2x the previous 96/112). The wrapper div is sized so its width
  // = logo + 18px padding on all sides (per the design request).
  const logoSize = isMobileViewport ? 192 : 224;

  return (
    <div style={backgroundStyle}>
      <div
        style={{
          maxWidth: 760,
          width: '100%',
          margin: '0 auto',
          padding: isMobileViewport ? '24px 12px' : '40px 16px',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
        }}
      >
        {/* Logo card: tight to the logo + 18px on all sides. */}
        <div
          style={{
            backgroundColor: '#ffffff',
            padding: 18,
            borderRadius: 12,
            boxShadow: '0 8px 20px rgba(15, 23, 42, 0.18)',
            marginBottom: 18,
            display: 'inline-block',
            lineHeight: 0,
          }}
        >
          <img
            src="/logo.png"
            alt="The Aspen Grove logo"
            style={{
              width: logoSize,
              height: 'auto',
              display: 'block',
            }}
          />
        </div>

        <div
          style={{
            backgroundColor: '#ffffff',
            padding: isMobileViewport ? '14px 16px' : '16px 20px',
            borderRadius: 10,
            boxShadow: '0 8px 20px rgba(15, 23, 42, 0.12)',
            color: '#1f2937',
            lineHeight: 1.5,
            marginBottom: 16,
            width: '100%',
            boxSizing: 'border-box',
          }}
        >
          <p style={{ margin: 0, fontSize: isMobileViewport ? '15px' : '16px' }}>
            The Aspen Grove of Opera Singers is an online database and data visualization project that illuminates relationships among opera singers and their teachers. Classical singing technique and tradition has been passed down in one-on-one lessons from its very origins in Florence in the late 16th century to the present day. The Aspen Grove makes this vast history available in text and in a visualization making exploration intuitive. Until now, this information has been scattered across online and print sources. Intended for students, teachers, scholars, and opera fans, this resource allows for a deep exploration of a singer's pedagogical lineage and contextualizes the history of the entire operatic art form.
          </p>
        </div>

        <details
          style={{
            backgroundColor: '#f6faff',
            border: '2px solid #cbdaf7',
            borderRadius: 12,
            padding: isMobileViewport ? '14px 16px' : '16px 20px',
            color: '#1f2937',
            lineHeight: 1.5,
            marginBottom: 16,
            width: '100%',
            boxSizing: 'border-box',
          }}
        >
          <summary style={{ cursor: 'pointer', fontWeight: 600, color: '#1d4ed8', fontSize: '18px' }}>Disclaimer</summary>
          <p style={{ margin: '12px 0 0 0', color: '#374151' }}>
            Thank you for your interest in this project.
          </p>
          <p style={{ margin: '8px 0 0 0', color: '#374151' }}>
            I have spent the last three summers and this current sabbatical collecting information for this database. I have endeavored to include "successful" opera singers and their teachers. Success can, of course, be defined many ways. For the purposes of this tool, I have chosen to include singers who have sung roles at A- and B-level houses and their equivalents, singers who are managed, and singers who have been documented in reference books and websites specializing in classical singing and teaching history. Though the information is vast (15,000 singers, 6,100 relationships), it is far from exhaustive. I can make no claims about the quality of the teaching or of the quality of the relationship between teacher and student. Further, there is no guarantee that any teacher's methods carry forward to their students or the next generation, or to those that follow.
          </p>
          <p style={{ margin: '8px 0 0 0', color: '#374151' }}>
            If you would like some or all of your personal information to be removed from the dataset for any reason, please <a href="mailto:classicalsinginghumanitieslab@gmail.com">let me know</a>. I will happily remove anyone's information. If you have a correction from a credible source, I will happily incorporate that too. If you have information to add that meets the criteria described above, please fill out <a href="https://forms.gle/TZmuaPpMUu9ob4jT8" target="_blank" rel="noopener noreferrer">this form</a>, and I will incorporate it as quickly as I can.
          </p>
          <p style={{ margin: '8px 0 0 0', color: '#374151', fontWeight: 600 }}>
            By selecting Accept terms and Enter you acknowledge the extent of the site's current contents and the limitations expressed in this disclaimer.
          </p>
        </details>

        {/* White bg, blue text matching the Disclaimer heading color. */}
        <button
          type="button"
          onClick={handleEnter}
          style={{
            padding: '12px 24px',
            backgroundColor: '#ffffff',
            color: '#1d4ed8',
            border: '2px solid #3e96e2',
            borderRadius: 10,
            cursor: 'pointer',
            fontSize: '16px',
            fontWeight: 600,
            height: '48px',
            display: 'inline-flex',
            alignItems: 'center',
            boxSizing: 'border-box',
          }}
        >
          Accept terms and Enter
        </button>
      </div>
    </div>
  );
};

export default Landing;
