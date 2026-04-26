// growth-lock.js — Shared concurrency lock for Growth Engine jobs
// Imported by both growth-routes.js AND server.js so the lock is truly shared
// in the same Node.js process. Prevents two LinkedIn jobs running at the same time.

// Jobs that use the LinkedIn browser session — only one can run at a time
const LINKEDIN_JOBS = new Set(['crawl', 'fetch', 'invite', 'message', 'followup', 'linkedin_analytics']);

const _locks = new Map(); // jobName → { startedAt: ISO string }

/**
 * Try to acquire a lock for a job.
 * Returns true if acquired, false if blocked.
 */
export function acquireLock(jobName) {
  // Already running
  if (_locks.has(jobName)) return false;

  // If this job uses LinkedIn, block if ANY other LinkedIn job is running
  if (LINKEDIN_JOBS.has(jobName)) {
    for (const [running] of _locks) {
      if (LINKEDIN_JOBS.has(running)) return false;
    }
  }

  _locks.set(jobName, { startedAt: new Date().toISOString() });
  return true;
}

export function releaseLock(jobName) {
  _locks.delete(jobName);
}

export function isLocked(jobName) {
  return _locks.has(jobName);
}

export function getActiveLocks() {
  return Object.fromEntries(_locks);
}

export function isAnyGrowthJobRunning() {
  const growthJobs = ['crawl', 'fetch', 'enrich', 'pulse', 'invite', 'connections', 'messages', 'message', 'followup'];
  return growthJobs.some(j => _locks.has(j));
}
