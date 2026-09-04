# STEMPulse

STEMPulse is a women-focused STEM growth and career support platform designed to help women and girls learn, enter, grow, and restart their journey in STEM.

The platform provides different pathways based on the user's stage in life and career, along with personalized opportunities, learning support, career guidance, and a live impact dashboard.

---

## Core Pathways

### 1. Learn & Enter
For school and college women who want to upskill and enter STEM careers.

Features:
- Personalized STEM learning paths
- Skill-gap analysis
- Coding practice and guided learning
- Internship and early-career opportunities
- Mentor and role-model discovery
- Progress tracking

### 2. Work & Fund
For working women and women looking for jobs, internships, scholarships, and funding opportunities.

Features:
- STEM job discovery
- Internship recommendations
- Scholarship and funding opportunities
- Career growth recommendations
- Opportunity matching
- Application tracking

### 3. Career Re-entry (Restart & Grow)
For women returning to technical work after marriage, childcare, or another career break—without treating their earlier experience as zero.

Features:
- Unified profile for prior work, current skills, rusty skills, and recent evidence
- Transparent Retained / Rusty / Missing skill classification against a target role
- Shortest practical route: refresh → evidence → returnship → job
- Re-entry-friendly internship and returnship matches
- Optional remote, hybrid, part-time, flexible-hours, and childcare-support ranking
- Progress view: Skills → Projects → Funding → Returnship → Applications → Employment

---

## Live Impact Dashboard

STEMPulse includes a live dashboard to show measurable impact.

Possible metrics include:
- Total registered women
- Active users
- Learning sessions completed
- Coding activities completed
- Mentoring sessions
- Jobs and internships applied for
- Scholarships discovered
- Projects completed
- Skill improvement
- Career re-entry progress
- Successful employment outcomes

Example journey:

```text
Registered
   ↓
Active
   ↓
Learning
   ↓
Skill Improvement
   ↓
Opportunity
   ↓
Employment / Career Growth
```

## Personalized career intelligence

Work & Fund and Career Re-entry use four fast, deterministic stages:

1. Profile & Evidence
2. Skill Freshness & Gap Analysis
3. Role & Flexibility Matching
4. Shortest-Path Planning

The Career Re-entry schema captures earlier work, current and rusty skills, new project evidence, target role, and optional work preferences. Match score is explicit: 70% technical fit (rusty skills count at 45%) + 30% stated work-fit; an unmet education baseline caps the score at 58%. No preference is inferred from gender, motherhood, or a career break.

This rule-based path keeps results quick and explainable; it does not require an Ollama call.

## Evidence-aware confidence scan

After the five confidence questions, a learner can optionally connect public profiles from GitHub, GitLab, or Codeforces. STEMPulse reads public project and activity metadata, detects visible skill signals, and shows three separate numbers:

- Self-rating: what the learner reported.
- Public evidence: what the connected profile made visible.
- Evidence-adjusted confidence: 40% self-rating + 60% public evidence when a source is connected.

The report explains the evidence gap as aligned, hidden confidence, or a prompt to add visible proof. A missing, private, or unconnected profile is never treated as proof that the learner lacks ability. No password, personal access token, or private repository access is requested or stored.

This creates a strong demo moment: compare a learner who rates every skill 5/5 with a quiet learner who rates herself 2/5 but has substantial public work. STEMPulse can then recommend a concrete next step instead of judging either person.

### Synthetic opportunity universe

The Work & Fund flow now asks six concise questions. Those answers are mapped into the existing profile fields, so the matcher still considers direction, education, evidence, life context, work fit and confidence without making the user complete a 13-field interview.

Recommendations come from `backend/synthetic_opportunities.json`, a seeded demo dataset with distinct records for jobs, internships, scholarships and funding. Every record has its own organization, location, work mode, compensation or award range, description, eligibility baseline, technical requirements, transferable signals and domain tags. The matching engine ranks the records against the profile and returns the top three, so changing the user's direction, experience or constraints changes the opportunity set and the explanation shown in the UI.

Run the career-pipeline regression checks with:

```powershell
python -m unittest backend.test_career_pipeline -v
```

## 🚀 How to Run STEMPulse

Run the backend and frontend in **two separate terminals**.

### Terminal 1 — Backend

Move to the project folder:

```powershell
cd STEMPulse
```

Install the backend dependencies:

```powershell
python -m pip install -r backend\requirements.txt
```

Start the backend server:

```powershell
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8001
```

Wait until the terminal shows that the server is running at:

```text
http://127.0.0.1:8001
```

Keep this terminal running.

---

### Terminal 2 — Frontend

Open a second terminal and move to the project folder:

```powershell
cd STEMPulse
```

Install the frontend dependencies:

```powershell
npm install
```

Start the frontend:

```powershell
npm run dev
```

Then open:

```text
http://127.0.0.1:5173
```

in your browser.

Keep both the frontend and backend terminals running while using STEMPulse.
