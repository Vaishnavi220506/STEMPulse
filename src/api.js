// Port 8000 is commonly used by other local projects. StemPulse uses 8001 by default.
const API = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8001'

export async function call(path, options = {}) {
  try {
    const response = await fetch(`${API}${path}`, {
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
      ...options,
    })
    if (!response.ok) throw new Error(`StemPulse service error (${response.status})`)
    return response.json()
  } catch (error) {
    if (error instanceof TypeError) {
      throw new Error('Cannot reach the StemPulse service. Start the backend on port 8001, then retry.')
    }
    throw error
  }
}

export const api = {
  login: (payload) => call('/api/auth/login', { method: 'POST', body: JSON.stringify(payload) }),
  metrics: () => call('/api/metrics'),
  assess: (payload) => call('/api/learn/assess', { method: 'POST', body: JSON.stringify(payload) }),
  evidenceScan: (payload) => call('/api/confidence/evidence', { method: 'POST', body: JSON.stringify(payload) }),
  latestEvidenceScan: (email, skill) => call(`/api/confidence/evidence?email=${encodeURIComponent(email)}&stem_field=${encodeURIComponent(skill)}`),
  progress: (payload) => call('/api/roadmap/progress', { method: 'POST', body: JSON.stringify(payload) }),
  matches: (category, profile) => call(`/api/opportunities/${category}`, { method: 'POST', body: JSON.stringify({ profile }) }),
  savedOpportunities: (email) => call(`/api/opportunities/saved?email=${encodeURIComponent(email)}`),
  saveOpportunity: (payload) => call('/api/opportunities/saved', { method: 'POST', body: JSON.stringify(payload) }),
  removeSavedOpportunity: (id, email) => call(`/api/opportunities/saved/${id}?email=${encodeURIComponent(email)}`, { method: 'DELETE' }),
  strengths: (profile) => call('/api/restart/analyze', { method: 'POST', body: JSON.stringify({ profile }) }),
  mentorRecommendations: (payload) => call('/api/mentors/recommendations', { method: 'POST', body: JSON.stringify(payload) }),
  requestMentorGuidance: (payload) => call('/api/mentors/requests', { method: 'POST', body: JSON.stringify(payload) }),
  impactTimeline: (window) => call(`/api/impact/timeline?window=${window}`),
  anonymousScore: (payload) => call('/api/anonymous-score', { method: 'POST', body: JSON.stringify(payload) }),
  regenerateAnonymousId: (payload) => call('/api/anonymous-score/regenerate', { method: 'POST', body: JSON.stringify(payload) }),
  setLeaderboardPrivacy: (payload) => call('/api/anonymous-score/privacy', { method: 'POST', body: JSON.stringify(payload) }),
  recordAnonymousActivity: (payload) => call('/api/anonymous-score/activity', { method: 'POST', body: JSON.stringify(payload) }),
  socketUrl: () => `${API.replace(/^http/, 'ws')}/ws/impact`,
}
