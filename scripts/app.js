/**
 * ZenML 2025 Unwrapped
 * Main Application Logic
 */

// ==============================================
// Global State
// ==============================================

let metricsData = null;

// ==============================================
// Initialization
// ==============================================

document.addEventListener('DOMContentLoaded', async () => {
  try {
    // Load metrics data
    metricsData = await loadMetrics();

    // Initialize all sections
    initParticles();
    initHero();
    renderBigNumbers(metricsData.core_stats);
    renderTimeline(metricsData.time_analytics);
    renderPipelines(metricsData.top_pipelines);
    renderAwards(metricsData.awards);
    renderFunFacts(metricsData.fun_facts);
    renderShareCard(metricsData.core_stats);

    // Setup scroll animations
    initScrollAnimations();

    // Setup share functionality
    initShareButtons();

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
  // Set the data-counter values for animation
  document.getElementById('totalRuns').dataset.counter = stats.total_runs;
  document.getElementById('successRate').dataset.counter = stats.success_rate.toFixed(1);
  document.getElementById('uniquePipelines').dataset.counter = stats.unique_pipelines;
  document.getElementById('artifactsProduced').dataset.counter = stats.artifacts_produced;
  document.getElementById('modelsCreated').dataset.counter = stats.models_created;
  document.getElementById('uniqueUsers').dataset.counter = stats.unique_users;
}

// ==============================================
// Timeline Section
// ==============================================

function renderTimeline(timeAnalytics) {
  const chart = document.getElementById('timelineChart');
  const safeTimeAnalytics = timeAnalytics && typeof timeAnalytics === 'object'
    ? timeAnalytics
    : {};
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

  // Update highlights
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

    return `
      <div class="pipeline-item" data-animate>
        <span class="pipeline-rank ${rankClass}">${index + 1}</span>
        <span class="pipeline-name">${escapeHtml(pipeline.name)}</span>
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

  const awardKeys = [
    'pipeline_overlord',
    'failure_champion',
    'success_streak',
    'early_bird',
    'weekend_warrior',
    'variety_pack'
  ];

  container.innerHTML = awardKeys
    .filter(key => awards[key])
    .map(key => {
      const award = awards[key];
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

function renderFunFacts(facts) {
  const container = document.getElementById('factsContainer');
  const icons = ['✨', '🎯', '📊', '🏆', '🚀', '💡', '🎉'];

  container.innerHTML = facts.map((fact, index) => `
    <div class="fact-card" data-animate>
      <span class="fact-icon">${icons[index % icons.length]}</span>
      <p class="fact-text">${escapeHtml(fact)}</p>
    </div>
  `).join('');
}

// ==============================================
// Share Card Section
// ==============================================

function renderShareCard(stats) {
  document.getElementById('shareRuns').textContent = stats.total_runs.toLocaleString();
  document.getElementById('shareSuccess').textContent = `${stats.success_rate.toFixed(1)}%`;
  document.getElementById('sharePipelines').textContent = stats.unique_pipelines.toLocaleString();
}

function initShareButtons() {
  // Download card button
  document.getElementById('downloadCard')?.addEventListener('click', async () => {
    const card = document.getElementById('shareCard');
    const button = document.getElementById('downloadCard');

    try {
      button.disabled = true;
      button.innerHTML = '<span>Generating...</span>';

      // Remove 3D transform for clean capture
      const originalTransform = card.style.transform;
      card.style.transform = 'none';

      // Use html2canvas to capture the card
      const canvas = await html2canvas(card, {
        scale: 2,
        backgroundColor: '#0F0F1A',
        logging: false,
        useCORS: true,
        allowTaint: true,
        width: card.offsetWidth,
        height: card.offsetHeight,
      });

      // Restore transform
      card.style.transform = originalTransform;

      // Download the image
      const link = document.createElement('a');
      link.download = 'zenml-2025-unwrapped.png';
      link.href = canvas.toDataURL('image/png');
      link.click();

    } catch (error) {
      console.error('Failed to generate card:', error);
      alert('Failed to generate image. Please try again.');
    } finally {
      button.disabled = false;
      button.innerHTML = `
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M7 10l5 5 5-5M12 15V3"/>
        </svg>
        <span>Download Card</span>
      `;
    }
  });

  // Copy stats button
  document.getElementById('copyLink')?.addEventListener('click', async () => {
    const stats = metricsData.core_stats;
    const text = `My ZenML 2025 Unwrapped:\n\n` +
      `🚀 ${stats.total_runs.toLocaleString()} pipeline runs\n` +
      `✅ ${stats.success_rate.toFixed(1)}% success rate\n` +
      `📦 ${stats.unique_pipelines} unique pipelines\n` +
      `🧠 ${stats.models_created} models created\n\n` +
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
        // Add animate-in class
        entry.target.classList.add('animate-in');

        // If it's a counter element, animate the number
        const counterEl = entry.target.querySelector('[data-counter]') ||
                         (entry.target.dataset.counter ? entry.target : null);

        if (counterEl?.dataset.counter) {
          animateCounter(counterEl);
        }

        // Unobserve after animation (one-time)
        observer.unobserve(entry.target);
      }
    });
  }, observerOptions);

  // Observe all animatable elements
  document.querySelectorAll('[data-animate]').forEach(el => {
    observer.observe(el);
  });

  // Also observe stat cards specifically for counter animation
  document.querySelectorAll('.stat-card').forEach(el => {
    observer.observe(el);
  });

  // Observe timeline bars
  document.querySelectorAll('.timeline-bar').forEach(el => {
    observer.observe(el);
  });

  // Observe pipeline items
  document.querySelectorAll('.pipeline-item').forEach(el => {
    observer.observe(el);
  });

  // Observe award cards
  document.querySelectorAll('.award-card').forEach(el => {
    observer.observe(el);
  });

  // Observe fact cards
  document.querySelectorAll('.fact-card').forEach(el => {
    observer.observe(el);
  });

  // Observe highlight cards
  document.querySelectorAll('.highlight-card').forEach(el => {
    observer.observe(el);
  });
}

// ==============================================
// Counter Animation
// ==============================================

function animateCounter(element) {
  const target = parseFloat(element.dataset.counter);
  const duration = 2000; // 2 seconds
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
      // Ensure we end on the exact target
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
