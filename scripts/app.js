/**
 * ZenML 2025 Unwrapped
 * Main Application Logic - Schema v2.0
 */

// ==============================================
// Global State
// ==============================================

let metricsData = null;
let isAnonymized = false;
let currentLeaderboard = 'most_runs';
let leaderboardExpanded = false;

// ==============================================
// Initialization
// ==============================================

document.addEventListener('DOMContentLoaded', async () => {
  try {
    // Load metrics data
    metricsData = await loadMetrics();

    // Validate schema version
    if (metricsData.schema_version !== '2.0') {
      throw new Error(`Unsupported schema version: ${metricsData.schema_version || '1.0'}. Please re-run extract_metrics.py to generate v2.0 data.`);
    }

    // Initialize all sections
    initParticles();
    initHero();
    initAnonymizationToggle();
    renderBigNumbers(metricsData.core_stats);
    renderLeaderboards();
    renderTimeline(metricsData.time_analytics);
    renderPipelines(metricsData.top_pipelines);
    renderAwards(metricsData.awards);
    renderFunFacts();
    renderShareCards(metricsData.core_stats, metricsData.time_analytics);

    // Setup scroll animations
    initScrollAnimations();

    // Setup share functionality
    initShareButtons();

    // Setup leaderboard tabs
    initLeaderboardTabs();

  } catch (error) {
    console.error('Failed to initialize:', error);
    document.body.innerHTML = `
      <div style="min-height: 100vh; display: flex; align-items: center; justify-content: center; flex-direction: column; gap: 1rem; padding: 2rem; text-align: center;">
        <h1 style="font-size: 2rem; color: #F9FAFB;">Unable to load data</h1>
        <p style="color: #9CA3AF;">Make sure metrics.json exists in the data/ folder.</p>
        <code style="color: #A78BFA; background: rgba(124, 58, 237, 0.1); padding: 0.5rem 1rem; border-radius: 0.5rem;">${error.message}</code>
      </div>
    `;
  }
});

// ==============================================
// Data Loading
// ==============================================

async function loadMetrics() {
  const response = await fetch('./data/metrics.json');
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  return response.json();
}

// ==============================================
// Anonymization
// ==============================================

function initAnonymizationToggle() {
  const checkbox = document.getElementById('anonCheckbox');
  const label = document.getElementById('anonLabel');
  const toggle = document.getElementById('anonToggle');

  checkbox?.addEventListener('change', (e) => {
    isAnonymized = e.target.checked;
    label.textContent = isAnonymized ? 'Anonymized' : 'Show Real Names';
    toggle.classList.toggle('active', isAnonymized);

    // Re-render affected sections
    renderLeaderboards();
    renderPipelines(metricsData.top_pipelines);
    renderAwards(metricsData.awards);
    renderFunFacts();
    renderShareCards(metricsData.core_stats, metricsData.time_analytics);

    // Re-apply animations to newly rendered elements (they're already in view)
    applyVisibleAnimations();
  });
}

function applyVisibleAnimations() {
  // Elements that were re-rendered need .animate-in class since they're already visible
  const animatableSelectors = [
    '.pipeline-item',
    '.award-card',
    '.fact-card',
    '.leaderboard-row',
    '[data-animate]'
  ];

  animatableSelectors.forEach(selector => {
    document.querySelectorAll(selector).forEach(el => {
      el.classList.add('animate-in');
    });
  });
}

function getProjectDisplayName(projectName) {
  if (!isAnonymized || !metricsData.anonymized?.projects) {
    return projectName;
  }
  return metricsData.anonymized.projects[projectName] || projectName;
}

function getPipelineDisplayName(pipelineName) {
  if (!isAnonymized || !metricsData.anonymized?.pipelines) {
    return pipelineName;
  }
  return metricsData.anonymized.pipelines[pipelineName] || pipelineName;
}

// ==============================================
// Particles Background
// ==============================================

function initParticles() {
  const container = document.getElementById('particles');
  const particleCount = 50;

  for (let i = 0; i < particleCount; i++) {
    const particle = document.createElement('div');
    particle.className = 'particle';
    particle.style.left = `${Math.random() * 100}%`;
    particle.style.top = `${Math.random() * 100}%`;
    particle.style.animationDelay = `${Math.random() * 4}s`;
    particle.style.animationDuration = `${3 + Math.random() * 3}s`;
    container.appendChild(particle);
  }
}

// ==============================================
// Hero Section
// ==============================================

function initHero() {
  const startButton = document.getElementById('startJourney');
  startButton?.addEventListener('click', () => {
    document.getElementById('bigNumbers')?.scrollIntoView({
      behavior: 'smooth'
    });
  });
}

// ==============================================
// Big Numbers Section
// ==============================================

function renderBigNumbers(stats) {
  document.getElementById('totalRuns').dataset.counter = stats.total_runs;
  document.getElementById('successRate').dataset.counter = stats.success_rate.toFixed(1);
  document.getElementById('uniquePipelines').dataset.counter = stats.unique_pipelines;
  document.getElementById('artifactsProduced').dataset.counter = stats.artifacts_produced;
  document.getElementById('modelsCreated').dataset.counter = stats.models_created;
  document.getElementById('uniqueUsers').dataset.counter = stats.unique_users;
}

// ==============================================
// Project Leaderboards Section
// ==============================================

function initLeaderboardTabs() {
  const tabs = document.querySelectorAll('.leaderboard-tab');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      tabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      currentLeaderboard = tab.dataset.tab;
      leaderboardExpanded = false;
      renderLeaderboards();
    });
  });

  const moreBtn = document.getElementById('leaderboardMore');
  moreBtn?.addEventListener('click', () => {
    leaderboardExpanded = !leaderboardExpanded;
    moreBtn.classList.toggle('expanded', leaderboardExpanded);
    moreBtn.querySelector('span').textContent = leaderboardExpanded ? 'Show less' : 'See all projects';
    renderLeaderboards();
  });
}

function renderLeaderboards() {
  const container = document.getElementById('leaderboardList');
  const section = document.getElementById('leaderboards');
  const moreBtn = document.getElementById('leaderboardMore');
  const projects = metricsData.projects || [];
  const leaderboards = metricsData.project_leaderboards || {};

  // Hide section if single project or no projects
  if (projects.length <= 1) {
    section?.classList.add('hidden');
    return;
  }
  section?.classList.remove('hidden');

  // Get ranked project names for current tab
  const rankedNames = leaderboards[currentLeaderboard] || [];
  const projectMap = {};
  projects.forEach(p => { projectMap[p.name] = p; });

  // Determine how many to show
  const showCount = leaderboardExpanded ? rankedNames.length : Math.min(5, rankedNames.length);

  // Hide "See all" if 5 or fewer projects
  if (rankedNames.length <= 5) {
    moreBtn?.classList.add('hidden');
  } else {
    moreBtn?.classList.remove('hidden');
  }

  // Render rows
  container.innerHTML = rankedNames.slice(0, showCount).map((name, index) => {
    const project = projectMap[name];
    if (!project) return '';

    const rankClass = index === 0 ? 'gold' : index === 1 ? 'silver' : index === 2 ? 'bronze' : 'default';
    const displayName = getProjectDisplayName(name);

    // Get the value based on current leaderboard
    let value = '';
    switch (currentLeaderboard) {
      case 'most_runs':
        value = `${project.total_runs.toLocaleString()} runs`;
        break;
      case 'highest_success_rate':
        value = `${project.success_rate}%`;
        break;
      case 'most_users':
        value = `${project.unique_users} users`;
        break;
    }

    return `
      <div class="leaderboard-row" data-animate>
        <span class="leaderboard-rank ${rankClass}">#${index + 1}</span>
        <span class="leaderboard-name">${escapeHtml(displayName)}</span>
        <span class="leaderboard-value">${value}</span>
      </div>
    `;
  }).join('');

  // Re-observe for animations
  initLeaderboardAnimations();
}

function initLeaderboardAnimations() {
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('animate-in');
        observer.unobserve(entry.target);
      }
    });
  }, { threshold: 0.1 });

  document.querySelectorAll('.leaderboard-row').forEach(el => {
    observer.observe(el);
  });
}

// ==============================================
// Timeline Section
// ==============================================

function renderTimeline(timeAnalytics) {
  const chart = document.getElementById('timelineChart');
  const safeTimeAnalytics = timeAnalytics && typeof timeAnalytics === 'object' ? timeAnalytics : {};
  const runsPerMonth = Array.isArray(safeTimeAnalytics.runs_per_month)
    ? safeTimeAnalytics.runs_per_month.slice(0, 12)
    : [];
  const normalizedRuns = Array.from({ length: 12 }, (_, index) => {
    const value = Number(runsPerMonth[index]);
    return Number.isFinite(value) ? value : 0;
  });
  const maxRuns = Math.max(0, ...normalizedRuns);

  chart.innerHTML = normalizedRuns.map((count, index) => {
    const height = maxRuns > 0 ? (count / maxRuns) * 100 : 0;
    const isPeak = count === maxRuns && count > 0;
    return `
      <div class="timeline-bar ${isPeak ? 'peak' : ''}"
           style="height: ${Math.max(height, 2)}%;"
           data-count="${count.toLocaleString()}"
           data-animate></div>
    `;
  }).join('');

  document.getElementById('busiestMonth').textContent = safeTimeAnalytics.busiest_month || 'N/A';
  document.getElementById('busiestDay').textContent = safeTimeAnalytics.busiest_day || 'N/A';
  document.getElementById('busiestHour').textContent =
    safeTimeAnalytics.busiest_hour != null
      ? `${String(safeTimeAnalytics.busiest_hour).padStart(2, '0')}:00`
      : 'N/A';
}

// ==============================================
// Top Pipelines Section
// ==============================================

function renderPipelines(pipelines) {
  const container = document.getElementById('pipelinesList');
  const maxRuns = pipelines.length > 0 ? pipelines[0].runs : 1;

  container.innerHTML = pipelines.slice(0, 5).map((pipeline, index) => {
    const rankClass = index === 0 ? 'gold' : index === 1 ? 'silver' : index === 2 ? 'bronze' : 'default';
    const barWidth = (pipeline.runs / maxRuns) * 100;
    const displayName = getPipelineDisplayName(pipeline.name);
    const projectName = pipeline.project ? getProjectDisplayName(pipeline.project) : null;

    return `
      <div class="pipeline-item" data-animate>
        <span class="pipeline-rank ${rankClass}">${index + 1}</span>
        <span class="pipeline-name">
          ${escapeHtml(displayName)}
          ${projectName ? `<span class="pipeline-project">in ${escapeHtml(projectName)}</span>` : ''}
        </span>
        <div class="pipeline-bar">
          <div class="pipeline-bar-fill" style="width: ${barWidth}%;"></div>
        </div>
        <span class="pipeline-runs">${pipeline.runs} runs</span>
      </div>
    `;
  }).join('');
}

// ==============================================
// Awards Section
// ==============================================

function renderAwards(awards) {
  const container = document.getElementById('awardsGrid');

  // User awards first, then project awards
  const userAwardKeys = [
    'pipeline_overlord',
    'failure_champion',
    'success_streak',
    'early_bird',
    'night_owl',
    'weekend_warrior',
    'variety_pack'
  ];

  const projectAwardKeys = [
    'workhorse_project',
    'rising_star_project'
  ];

  const allAwards = [];

  // Process user awards
  userAwardKeys.forEach(key => {
    if (awards[key]) {
      allAwards.push({ key, award: awards[key], isProject: false });
    }
  });

  // Process project awards
  projectAwardKeys.forEach(key => {
    if (awards[key]) {
      allAwards.push({ key, award: awards[key], isProject: true });
    }
  });

  container.innerHTML = allAwards.map(({ award, isProject }) => {
    if (isProject) {
      // Project award
      const projectName = getProjectDisplayName(award.project);
      return `
        <div class="award-card project-award" data-animate>
          <span class="award-badge">Project Award</span>
          <div class="award-icon">${award.icon}</div>
          <h3 class="award-title">${escapeHtml(award.title)}</h3>
          <p class="award-description">"${escapeHtml(award.description)}"</p>
          <div class="award-user">
            <span class="award-project-name">${escapeHtml(projectName)}</span>
            <span class="award-value">${escapeHtml(award.value)}</span>
          </div>
        </div>
      `;
    } else {
      // User award
      const initials = getInitials(award.user);
      const avatarHtml = award.avatar
        ? `<img class="award-avatar" src="${escapeHtml(award.avatar)}" alt="${escapeHtml(award.user)}" onerror="this.outerHTML='<div class=\\'award-avatar-placeholder\\'>${initials}</div>'">`
        : `<div class="award-avatar-placeholder">${initials}</div>`;

      return `
        <div class="award-card" data-animate>
          <div class="award-icon">${award.icon}</div>
          <h3 class="award-title">${escapeHtml(award.title)}</h3>
          <p class="award-description">"${escapeHtml(award.description)}"</p>
          <div class="award-user">
            ${avatarHtml}
            <span class="award-name">${escapeHtml(award.user)}</span>
            <span class="award-value">${escapeHtml(award.value)}</span>
          </div>
        </div>
      `;
    }
  }).join('');
}

function getInitials(name) {
  return name
    .split(' ')
    .map(part => part[0])
    .slice(0, 2)
    .join('')
    .toUpperCase();
}

// ==============================================
// Fun Facts Section
// ==============================================

function renderFunFacts() {
  const container = document.getElementById('factsContainer');
  const funFacts = metricsData.fun_facts || {};
  const icons = ['✨', '🎯', '📊', '🏆', '🚀', '💡', '🎉'];

  // Choose specific or generic based on anonymization
  const facts = isAnonymized
    ? (funFacts.generic || funFacts.specific || [])
    : (funFacts.specific || funFacts.generic || []);

  // Handle old schema format (array instead of object)
  const factsList = Array.isArray(facts) ? facts : (Array.isArray(funFacts) ? funFacts : []);

  container.innerHTML = factsList.map((fact, index) => `
    <div class="fact-card" data-animate>
      <span class="fact-icon">${icons[index % icons.length]}</span>
      <p class="fact-text">${escapeHtml(fact)}</p>
    </div>
  `).join('');
}

// ==============================================
// Share Cards Section
// ==============================================

function renderShareCards(stats, timeAnalytics) {
  // Minimal card
  document.getElementById('shareMinimalRuns').textContent = stats.total_runs.toLocaleString();

  // Standard card
  document.getElementById('shareStandardRuns').textContent = stats.total_runs.toLocaleString();
  document.getElementById('shareStandardSuccess').textContent = `${stats.success_rate.toFixed(1)}%`;
  document.getElementById('shareStandardPipelines').textContent = stats.unique_pipelines.toLocaleString();

  // Detailed card
  document.getElementById('shareDetailedRuns').textContent = stats.total_runs.toLocaleString();
  document.getElementById('shareDetailedSuccess').textContent = `${stats.success_rate.toFixed(1)}%`;
  document.getElementById('shareDetailedPipelines').textContent = stats.unique_pipelines.toLocaleString();
  document.getElementById('shareDetailedModels').textContent = stats.models_created.toLocaleString();
  document.getElementById('shareDetailedUsers').textContent = stats.unique_users.toLocaleString();
  document.getElementById('shareDetailedProjects').textContent = (stats.active_projects || metricsData.workspace?.project_count || 1).toLocaleString();

  // Highlights
  document.getElementById('shareDetailedMonth').textContent = `Busiest: ${timeAnalytics?.busiest_month || '--'}`;
  document.getElementById('shareDetailedHour').textContent = `Peak: ${timeAnalytics?.busiest_hour != null ? `${timeAnalytics.busiest_hour}:00` : '--'}`;
}

function initShareButtons() {
  // Individual card download buttons
  document.querySelectorAll('.share-download-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const cardId = btn.dataset.card;
      const card = document.getElementById(cardId);
      if (!card) return;

      try {
        btn.disabled = true;
        const originalText = btn.innerHTML;
        btn.innerHTML = '<span>...</span>';

        const canvas = await html2canvas(card, {
          scale: 2,
          backgroundColor: '#0F0F1A',
          logging: false,
          useCORS: true,
          allowTaint: true,
        });

        const link = document.createElement('a');
        const variant = card.dataset.variant || 'card';
        link.download = `zenml-2025-unwrapped-${variant}.png`;
        link.href = canvas.toDataURL('image/png');
        link.click();

        btn.innerHTML = originalText;
      } catch (error) {
        console.error('Failed to generate card:', error);
        alert('Failed to generate image. Please try again.');
      } finally {
        btn.disabled = false;
      }
    });
  });

  // Copy stats button
  document.getElementById('copyLink')?.addEventListener('click', async () => {
    const stats = metricsData.core_stats;
    const text = `My ZenML 2025 Unwrapped:\n\n` +
      `🚀 ${stats.total_runs.toLocaleString()} pipeline runs\n` +
      `✅ ${stats.success_rate.toFixed(1)}% success rate\n` +
      `📦 ${stats.unique_pipelines} unique pipelines\n` +
      `🧠 ${stats.models_created} models created\n` +
      `👥 ${stats.unique_users} team members\n\n` +
      `#ZenML #MLOps #2025Unwrapped`;

    try {
      await navigator.clipboard.writeText(text);

      const button = document.getElementById('copyLink');
      const originalText = button.innerHTML;
      button.innerHTML = `
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M20 6L9 17l-5-5"/>
        </svg>
        <span>Copied!</span>
      `;

      setTimeout(() => {
        button.innerHTML = originalText;
      }, 2000);

    } catch (error) {
      console.error('Failed to copy:', error);
    }
  });
}

// ==============================================
// Scroll Animations
// ==============================================

function initScrollAnimations() {
  const observerOptions = {
    root: null,
    rootMargin: '0px',
    threshold: 0.1
  };

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('animate-in');

        const counterEl = entry.target.querySelector('[data-counter]') ||
                         (entry.target.dataset.counter ? entry.target : null);

        if (counterEl?.dataset.counter) {
          animateCounter(counterEl);
        }

        observer.unobserve(entry.target);
      }
    });
  }, observerOptions);

  // Observe all animatable elements
  document.querySelectorAll('[data-animate]').forEach(el => observer.observe(el));
  document.querySelectorAll('.stat-card').forEach(el => observer.observe(el));
  document.querySelectorAll('.timeline-bar').forEach(el => observer.observe(el));
  document.querySelectorAll('.pipeline-item').forEach(el => observer.observe(el));
  document.querySelectorAll('.award-card').forEach(el => observer.observe(el));
  document.querySelectorAll('.fact-card').forEach(el => observer.observe(el));
  document.querySelectorAll('.highlight-card').forEach(el => observer.observe(el));
}

// ==============================================
// Counter Animation
// ==============================================

function animateCounter(element) {
  const target = parseFloat(element.dataset.counter);
  const duration = 2000;
  const startTime = performance.now();
  const isFloat = target % 1 !== 0;

  function easeOutExpo(x) {
    return x === 1 ? 1 : 1 - Math.pow(2, -10 * x);
  }

  function update(currentTime) {
    const elapsed = currentTime - startTime;
    const progress = Math.min(elapsed / duration, 1);
    const easedProgress = easeOutExpo(progress);

    const currentValue = target * easedProgress;

    if (isFloat) {
      element.textContent = currentValue.toFixed(1);
    } else {
      element.textContent = Math.floor(currentValue).toLocaleString();
    }

    if (progress < 1) {
      requestAnimationFrame(update);
    } else {
      element.textContent = isFloat ? target.toFixed(1) : target.toLocaleString();
    }
  }

  requestAnimationFrame(update);
}

// ==============================================
// Utility Functions
// ==============================================

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}
