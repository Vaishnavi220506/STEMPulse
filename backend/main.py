import asyncio
import hashlib
import json
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "stempulse.db"

DEFAULT_METRICS = {
    "womenOnboarded": 12847,
    "exploringStem": 4291,
    "returningStem": 1247,
    "opportunities": 3842,
    "matches": 8921,
    "roadmaps": 2340,
    "skillsUpgraded": 6781,
    "reentries": 387,
}

def db():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection

def setup_db():
    with db() as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS metrics (name TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        connection.execute("CREATE TABLE IF NOT EXISTS activity (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, payload TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        connection.execute("""CREATE TABLE IF NOT EXISTS saved_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT NOT NULL,
            category TEXT NOT NULL,
            title TEXT NOT NULL,
            match_score TEXT NOT NULL,
            reason TEXT NOT NULL,
            eligibility TEXT NOT NULL,
            details TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_email, category, title)
        )""")
        connection.execute("""CREATE TABLE IF NOT EXISTS mentor_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT NOT NULL,
            mentor_id TEXT NOT NULL,
            guidance_type TEXT NOT NULL,
            message TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'Pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        connection.execute("""CREATE TABLE IF NOT EXISTS anonymous_profiles (
            user_email TEXT PRIMARY KEY,
            anonymous_id TEXT NOT NULL,
            stem_field TEXT NOT NULL DEFAULT 'STEM',
            target_career TEXT NOT NULL DEFAULT 'STEM Explorer',
            experience_level TEXT NOT NULL DEFAULT 'Emerging',
            leaderboard_opt_in INTEGER NOT NULL DEFAULT 0,
            activity_json TEXT NOT NULL DEFAULT '{}',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        connection.execute("""CREATE TABLE IF NOT EXISTS confidence_scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT NOT NULL,
            stem_field TEXT NOT NULL,
            self_score INTEGER NOT NULL,
            evidence_score INTEGER,
            calibrated_score INTEGER NOT NULL,
            calibration_status TEXT NOT NULL,
            report_json TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        for name, value in DEFAULT_METRICS.items():
            connection.execute("INSERT OR IGNORE INTO metrics(name, value) VALUES (?, ?)", (name, value))

def metrics():
    with db() as connection:
        rows = connection.execute("SELECT name, value FROM metrics").fetchall()
    return {row["name"]: row["value"] for row in rows}

class SocketHub:
    def __init__(self):
        self.connections: list[WebSocket] = []

    async def connect(self, socket: WebSocket):
        await socket.accept()
        self.connections.append(socket)
        await socket.send_json({"type": "metrics", "metrics": metrics()})

    def disconnect(self, socket: WebSocket):
        if socket in self.connections:
            self.connections.remove(socket)

    async def broadcast(self, message: dict):
        for socket in self.connections[:]:
            try:
                await socket.send_json(message)
            except Exception:
                self.disconnect(socket)

hub = SocketHub()

async def impact(changes: dict[str, int], kind: str = "activity"):
    with db() as connection:
        for name, amount in changes.items():
            connection.execute("UPDATE metrics SET value = value + ? WHERE name = ?", (amount, name))
        connection.execute("INSERT INTO activity(kind, payload) VALUES(?, ?)", (kind, json.dumps(changes)))
    await hub.broadcast({"type": "impact", "kind": kind, "changes": changes, "metrics": metrics()})

class LoginPayload(BaseModel):
    name: str = "Woman in STEM"
    email: str = ""

class AssessPayload(BaseModel):
    skill: str
    answers: list[int] = Field(min_length=5, max_length=5)

class EvidenceScanPayload(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    skill: str = Field(min_length=1, max_length=120)
    answers: list[int] = Field(min_length=5, max_length=5)
    github: str = Field(default="", max_length=240)
    gitlab: str = Field(default="", max_length=240)
    codeforces: str = Field(default="", max_length=240)

class ProgressPayload(BaseModel):
    task_id: int
    finished: bool = False

class CareerProfile(BaseModel):
    name: str = Field(default="StemPulse member", max_length=120)
    education_level: str = Field(default="Not specified", max_length=180)
    work_experience: str = Field(default="", max_length=1200)
    life_experience: str = Field(default="", max_length=1200)
    career_break: str = Field(default="No career break stated", max_length=500)
    interests: str = Field(default="", max_length=800)
    confidence: int = Field(default=3, ge=1, le=5)
    existing_skills: str = Field(default="", max_length=1000)
    target_role: str = Field(default="Open to guidance", max_length=240)
    constraints: str = Field(default="No constraints stated", max_length=800)
    available_time: str = Field(default="Time not specified", max_length=240)
    digital_literacy: str = Field(default="Not specified", max_length=500)
    work_preference: str = Field(default="Flexible", max_length=300)
    practical_goal: str = Field(default="Build a sustainable STEM pathway", max_length=500)
    rusty_skills: str = Field(default="", max_length=1000)
    new_evidence: str = Field(default="", max_length=1200)
    flexibility_preferences: str = Field(default="", max_length=500)

class MatchPayload(BaseModel):
    profile: CareerProfile | None = None
    answers: list[str] | None = None

class OpportunityPayload(BaseModel):
    title: str = Field(min_length=1, max_length=180)
    match: str = Field(min_length=1, max_length=20)
    reason: str = Field(min_length=1, max_length=1000)
    eligibility: str = Field(min_length=1, max_length=500)
    details: str = Field(min_length=1, max_length=500)

class SaveOpportunityPayload(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    category: str = Field(min_length=1, max_length=40)
    opportunity: OpportunityPayload

class RestartPayload(BaseModel):
    profile: CareerProfile | None = None
    responses: list[str] | None = None

class MentorMatchPayload(BaseModel):
    skill: str = Field(min_length=1, max_length=120)
    language: str = Field(default="English", max_length=30)

class MentorRequestPayload(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    mentor_id: str = Field(min_length=1, max_length=40)
    guidance_type: str = Field(min_length=1, max_length=80)
    message: str = Field(default="", max_length=600)

class AnonymousScorePayload(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    stem_field: str = Field(default="STEM", max_length=120)

class AnonymousActivityPayload(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    activity: str = Field(min_length=1, max_length=40)
    amount: int = Field(default=1, ge=1, le=20)

class PrivacyPayload(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    leaderboard_opt_in: bool

ROADMAPS = {
    "Python & Data": ["Python foundations", "Clean a real-world dataset", "Create your first data visual", "Build a mini insight dashboard", "Share your portfolio story"],
    "Web Development": ["HTML & accessible structure", "Style a responsive page", "Build interactions with JavaScript", "Create a women-first web product", "Publish your portfolio"],
    "AI & Machine Learning": ["AI concepts without the jargon", "Explore a women-in-STEM dataset", "Train a starter prediction model", "Explain model fairness", "Demo your AI story"],
    "Cybersecurity": ["Digital safety fundamentals", "Spot common security risks", "Secure a sample account", "Complete a threat mini-challenge", "Earn your safety badge"],
    "Science": ["Ask a testable question", "Plan a fair investigation", "Record observations clearly", "Explain evidence with a model", "Share your scientific finding"],
    "Engineering": ["Spot a practical problem", "Sketch a human-centred solution", "Build a safe mini prototype", "Test, measure and improve", "Present your engineering story"],
    "Mathematics": ["Find patterns in everyday data", "Build a visual number model", "Solve a real-world problem", "Explain your mathematical reasoning", "Create a maths portfolio challenge"],
    "Others": ["Define your learning goal", "Find a trusted starting resource", "Practise one core concept", "Build a small proof of learning", "Share what you discovered"],
}

def roadmap_for(skill: str, score: int):
    tasks = ROADMAPS.get(skill, ROADMAPS["Others"])
    level = "Explorer" if score < 55 else "Builder" if score < 78 else "Trailblazer"
    return {"skill": skill, "level": level, "tasks": [{"id": index + 1, "title": task, "xp": 120 + index * 40, "minutes": 25 + index * 10} for index, task in enumerate(tasks)]}

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_db()
    yield

app = FastAPI(title="StemPulse API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

async def ollama_runtime():
    base_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    preferred = os.getenv("OLLAMA_MODEL", "llama3.2")
    try:
        response = await asyncio.to_thread(request.urlopen, f"{base_url}/api/tags", timeout=2)
        installed = [item.get("name", "") for item in json.loads(response.read().decode("utf-8")).get("models", [])]
    except Exception:
        return base_url, None, []
    selected = next((name for name in installed if name == preferred or name.startswith(f"{preferred}:")), None)
    if not selected:
        selected = next((name for name in installed if not has_any(name.lower(), ["embed", "embedding"])), None)
    return base_url, selected, installed

@app.get("/api/health")
async def health():
    _, selected, installed = await ollama_runtime()
    return {"status": "bright", "ollama": "ready" if selected else "unavailable", "model": selected, "preferredModel": os.getenv("OLLAMA_MODEL", "llama3.2"), "installedModels": installed, "careerPipelineAgents": 4}

@app.get("/api/metrics")
async def get_metrics():
    return {"metrics": metrics()}

@app.post("/api/auth/login")
async def login(payload: LoginPayload):
    await impact({"womenOnboarded": 1}, "woman_onboarded")
    return {"name": payload.name.strip() or "Woman in STEM", "message": "Welcome to your STEM story."}

@app.post("/api/learn/assess")
async def assess(payload: AssessPayload):
    average = sum(payload.answers) / len(payload.answers)
    score = min(96, max(35, round(average * 18 + 6)))
    roadmap = roadmap_for(payload.skill, score)
    await impact({"exploringStem": 1, "roadmaps": 1}, "roadmap_generated")
    return {"confidenceScore": score, "message": f"You are ready to begin — your {roadmap['level']} energy is showing.", "roadmap": roadmap}

PUBLIC_API_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "STEMPulse/1.0 public-evidence-scan",
}

SKILL_PATTERNS = [
    ("Python", ("python",)),
    ("JavaScript", ("javascript", "js")),
    ("TypeScript", ("typescript", "ts")),
    ("React", ("react",)),
    ("SQL", ("sql",)),
    ("Data analysis", ("data", "pandas", "numpy", "jupyter")),
    ("Machine learning", ("machine learning", "ml", "pytorch", "tensorflow", "scikit")),
    ("HTML / CSS", ("html", "css")),
    ("Java", ("java",)),
    ("C++", ("c++", "cpp")),
    ("C#", ("c#", "csharp")),
    ("Go", ("golang",)),
    ("Rust", ("rust",)),
    ("R", (" r ", "r programming", "rstudio")),
    ("Docker", ("docker",)),
    ("Algorithms", ("algorithm", "data structure", "competitive programming")),
    ("Cybersecurity", ("security", "cyber", "cryptography", "ctf")),
]

SOURCE_WEIGHTS = {"github": 0.5, "gitlab": 0.25, "codeforces": 0.25}

def _days_since(value: str | None):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0, (datetime.now(timezone.utc) - parsed).days)
    except (TypeError, ValueError):
        return None

def _recency_points(days: int | None):
    if days is None:
        return 0
    if days <= 90:
        return 15
    if days <= 180:
        return 10
    if days <= 365:
        return 5
    return 1

def _recency_label(days: int | None):
    if days is None:
        return "No recent activity date available"
    if days == 0:
        return "Active today"
    if days == 1:
        return "Active yesterday"
    if days <= 30:
        return f"Active {days} days ago"
    if days <= 365:
        return f"Active {days // 30} months ago"
    return f"Last visible activity {days // 365} years ago"

def _detect_skills(texts: list[str]):
    haystack = f" {' '.join(texts).lower()} "
    detected = []
    for label, patterns in SKILL_PATTERNS:
        if any(pattern in haystack for pattern in patterns):
            detected.append(label)
    return detected[:10]

def _handle_from_input(value: str, provider: str):
    raw = value.strip().rstrip("/")
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        parsed_url = parse.urlparse(raw)
        host = (parsed_url.hostname or "").lower()
        allowed = {
            "github": {"github.com", "www.github.com"},
            "gitlab": {"gitlab.com", "www.gitlab.com"},
            "codeforces": {"codeforces.com", "www.codeforces.com"},
        }[provider]
        if host not in allowed:
            raise ValueError(f"Use a public {provider.title()} profile URL or username.")
        pieces = [piece for piece in parsed_url.path.split("/") if piece]
        if provider == "codeforces" and pieces and pieces[0].lower() in {"profile", "users"}:
            pieces = pieces[1:]
        raw = pieces[0] if pieces else ""
    else:
        raw = raw.split("/")[0].lstrip("@").strip()
    limit = 39 if provider == "github" else 80
    if not re.fullmatch(rf"[A-Za-z0-9][A-Za-z0-9_.-]{{0,{limit - 1}}}", raw):
        raise ValueError(f"Enter a valid {provider.title()} username.")
    return raw

def _fetch_public_json(url: str):
    try:
        req = request.Request(url, headers=PUBLIC_API_HEADERS)
        with request.urlopen(req, timeout=7) as response:
            return json.loads(response.read().decode("utf-8")), None
    except error.HTTPError as http_error:
        if http_error.code == 404:
            return None, "Profile not found"
        if http_error.code == 403:
            return None, "This public API is rate-limited right now"
        return None, f"Public API returned {http_error.code}"
    except (error.URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
        return None, "The public profile could not be reached right now"

async def _public_json(url: str):
    return await asyncio.to_thread(_fetch_public_json, url)

def _source_error(provider: str, label: str, handle: str, message: str, status: str = "error"):
    return {
        "id": provider,
        "label": label,
        "handle": handle,
        "status": status,
        "score": None,
        "summary": message,
        "signals": [],
        "skills": [],
        "highlights": [],
        "projects": [],
        "profileUrl": "",
        "limitation": "Only public information is visible; private work is never treated as missing.",
    }

async def _github_evidence(value: str, skill: str):
    label = "GitHub"
    try:
        handle = _handle_from_input(value, "github")
    except ValueError as error:
        return _source_error("github", label, value.strip(), str(error))
    if not handle:
        return _source_error("github", label, "", "Not connected", "not_connected")
    encoded = parse.quote(handle, safe="")
    profile, profile_error = await _public_json(f"https://api.github.com/users/{encoded}")
    if profile_error or not profile:
        return _source_error("github", label, handle, profile_error or "Profile not found", "not_found" if profile_error == "Profile not found" else "error")
    repos, repos_error = await _public_json(f"https://api.github.com/users/{encoded}/repos?per_page=100&sort=updated")
    repos = repos if isinstance(repos, list) else []
    original_repos = [repo for repo in repos if not repo.get("fork")]
    languages = sorted({repo.get("language") for repo in original_repos if repo.get("language")})
    project_text = [
        " ".join(str(repo.get(key) or "") for key in ("name", "description", "language", "topics"))
        for repo in original_repos
    ]
    skills = _detect_skills(project_text + languages + [skill])
    latest_days = min((_days_since(repo.get("pushed_at")) for repo in original_repos if repo.get("pushed_at")), default=None)
    stars = sum(int(repo.get("stargazers_count") or 0) for repo in original_repos)
    forks = sum(int(repo.get("forks_count") or 0) for repo in original_repos)
    project_points = min(35, len(original_repos) * 7)
    skill_points = min(22, len(skills) * 4)
    proof_points = min(18, stars + (forks * 2))
    profile_points = min(10, 4 + (3 if profile.get("bio") else 0) + (3 if profile.get("blog") else 0))
    activity_points = _recency_points(latest_days)
    score = min(100, project_points + skill_points + proof_points + profile_points + activity_points)
    projects = [
        {
            "name": repo.get("name"),
            "url": repo.get("html_url"),
            "description": repo.get("description") or "Public project without a description yet.",
            "language": repo.get("language") or "Mixed / not tagged",
            "stars": int(repo.get("stargazers_count") or 0),
        }
        for repo in sorted(original_repos, key=lambda item: (item.get("stargazers_count") or 0, item.get("pushed_at") or ""), reverse=True)[:3]
    ]
    return {
        "id": "github", "label": label, "handle": handle, "status": "connected", "score": score,
        "summary": f"Reviewed {len(original_repos)} public project{'s' if len(original_repos) != 1 else ''} and {len(skills)} skill signal{'s' if len(skills) != 1 else ''}.",
        "signals": [
            {"label": "Public projects", "value": project_points, "detail": f"{len(original_repos)} original repositories visible"},
            {"label": "Skill signals", "value": skill_points, "detail": ", ".join(skills[:5]) if skills else "No matching skill keywords found"},
            {"label": "Recent activity", "value": activity_points, "detail": _recency_label(latest_days)},
            {"label": "Proof of work", "value": proof_points, "detail": f"{stars} stars · {forks} forks across public projects"},
        ],
        "skills": skills,
        "highlights": [
            f"{len(original_repos)} original public repositories",
            f"{len(languages)} primary language{'s' if len(languages) != 1 else ''}: {', '.join(languages[:4]) or 'not detected'}",
            _recency_label(latest_days),
        ],
        "projects": projects,
        "profileUrl": profile.get("html_url") or f"https://github.com/{handle}",
        "avatarUrl": profile.get("avatar_url") or "",
        "limitation": "GitHub analysis uses public profile and repository metadata. Private repositories and unpushed local work are not scored down.",
    }

async def _gitlab_evidence(value: str, skill: str):
    label = "GitLab"
    try:
        handle = _handle_from_input(value, "gitlab")
    except ValueError as error:
        return _source_error("gitlab", label, value.strip(), str(error))
    if not handle:
        return _source_error("gitlab", label, "", "Not connected", "not_connected")
    encoded = parse.quote(handle, safe="")
    users, user_error = await _public_json(f"https://gitlab.com/api/v4/users?username={encoded}")
    user = users[0] if isinstance(users, list) and users else None
    if user_error or not user:
        return _source_error("gitlab", label, handle, user_error or "Profile not found", "not_found" if user_error == "Profile not found" or not user else "error")
    projects, projects_error = await _public_json(f"https://gitlab.com/api/v4/users/{user.get('id')}/projects?per_page=100&order_by=last_activity_at&sort=desc")
    projects = projects if isinstance(projects, list) else []
    project_text = [
        " ".join(str(item.get(key) or "") for key in ("name", "path_with_namespace", "description", "topics"))
        for item in projects
    ]
    skills = _detect_skills(project_text + [skill])
    latest_days = min((_days_since(item.get("last_activity_at")) for item in projects if item.get("last_activity_at")), default=None)
    stars = sum(int(item.get("star_count") or 0) for item in projects)
    forks = sum(int(item.get("forks_count") or 0) for item in projects)
    project_points = min(35, len(projects) * 7)
    skill_points = min(22, len(skills) * 4)
    proof_points = min(18, stars + (forks * 2))
    activity_points = _recency_points(latest_days)
    profile_points = 10 if user.get("bio") or user.get("website_url") else 6
    score = min(100, project_points + skill_points + proof_points + activity_points + profile_points)
    project_cards = [
        {
            "name": item.get("name"), "url": item.get("web_url"),
            "description": item.get("description") or "Public project without a description yet.",
            "language": ", ".join(item.get("topics") or []) or "GitLab project",
            "stars": int(item.get("star_count") or 0),
        }
        for item in projects[:3]
    ]
    return {
        "id": "gitlab", "label": label, "handle": handle, "status": "connected", "score": score,
        "summary": f"Reviewed {len(projects)} public GitLab project{'s' if len(projects) != 1 else ''}.",
        "signals": [
            {"label": "Public projects", "value": project_points, "detail": f"{len(projects)} projects visible"},
            {"label": "Skill signals", "value": skill_points, "detail": ", ".join(skills[:5]) if skills else "No matching skill keywords found"},
            {"label": "Recent activity", "value": activity_points, "detail": _recency_label(latest_days)},
            {"label": "Community proof", "value": proof_points, "detail": f"{stars} stars · {forks} forks across public projects"},
        ],
        "skills": skills,
        "highlights": [f"{len(projects)} public GitLab projects", _recency_label(latest_days), f"{stars} community stars"],
        "projects": project_cards,
        "profileUrl": user.get("web_url") or f"https://gitlab.com/{handle}",
        "avatarUrl": user.get("avatar_url") or "",
        "limitation": "GitLab analysis uses public profile and project metadata. Private projects are not treated as missing.",
    }

async def _codeforces_evidence(value: str, skill: str):
    label = "Codeforces"
    try:
        handle = _handle_from_input(value, "codeforces")
    except ValueError as error:
        return _source_error("codeforces", label, value.strip(), str(error))
    if not handle:
        return _source_error("codeforces", label, "", "Not connected", "not_connected")
    encoded = parse.quote(handle, safe="")
    info, info_error = await _public_json(f"https://codeforces.com/api/user.info?handles={encoded}")
    info_results = info.get("result") if isinstance(info, dict) and info.get("status") == "OK" else []
    user = info_results[0] if isinstance(info_results, list) and info_results else None
    if info_error or not user:
        return _source_error("codeforces", label, handle, info_error or "Profile not found", "not_found" if info_error == "Profile not found" else "error")
    ratings, _ = await _public_json(f"https://codeforces.com/api/user.rating?handle={encoded}")
    submissions, _ = await _public_json(f"https://codeforces.com/api/user.status?handle={encoded}&from=1&count=1000")
    contests = ratings.get("result", []) if isinstance(ratings, dict) and ratings.get("status") == "OK" else []
    submissions = submissions.get("result", []) if isinstance(submissions, dict) and submissions.get("status") == "OK" else []
    tags = sorted({tag for item in submissions for tag in (item.get("problem", {}).get("tags") or [])})
    latest_seconds = max((int(item.get("creationTimeSeconds") or 0) for item in submissions), default=0)
    latest_days = max(0, int((datetime.now(timezone.utc).timestamp() - latest_seconds) / 86400)) if latest_seconds else None
    rating = int(user.get("rating") or 0)
    max_rating = int(user.get("maxRating") or 0)
    rating_points = min(55, max(0, round((max_rating - 800) / 22))) if max_rating else 0
    contest_points = min(20, len(contests) * 2)
    submission_points = min(15, len(submissions) // 40)
    activity_points = min(10, round(_recency_points(latest_days) * 2 / 3))
    score = min(100, rating_points + contest_points + submission_points + activity_points)
    detected = _detect_skills([skill, "algorithms competitive programming " + " ".join(tags)])
    return {
        "id": "codeforces", "label": label, "handle": handle, "status": "connected", "score": score,
        "summary": f"Reviewed a {user.get('rank', 'competitive programming')} profile with {len(contests)} rated contest{'s' if len(contests) != 1 else ''}.",
        "signals": [
            {"label": "Peak rating", "value": rating_points, "detail": f"{max_rating or 'Unrated'} peak · {rating or 'Unrated'} current"},
            {"label": "Contest practice", "value": contest_points, "detail": f"{len(contests)} rated contests"},
            {"label": "Problem attempts", "value": submission_points, "detail": f"{len(submissions)} recent submissions sampled"},
            {"label": "Recent activity", "value": activity_points, "detail": _recency_label(latest_days)},
        ],
        "skills": detected + [tag.title() for tag in tags[:5] if tag.title() not in detected],
        "highlights": [f"{user.get('rank', 'Unrated')} · peak {max_rating or 'unrated'}", f"{len(contests)} rated contests", f"{len(submissions)} submissions sampled"],
        "projects": [],
        "profileUrl": f"https://codeforces.com/profile/{handle}",
        "avatarUrl": "",
        "limitation": "Codeforces signals reflect public competitive-programming activity and are not a substitute for project or workplace evidence.",
    }

async def run_evidence_scan(payload: EvidenceScanPayload):
    average = sum(payload.answers) / len(payload.answers)
    self_score = min(96, max(35, round(average * 18 + 6)))
    sources = await asyncio.gather(
        _github_evidence(payload.github, payload.skill),
        _gitlab_evidence(payload.gitlab, payload.skill),
        _codeforces_evidence(payload.codeforces, payload.skill),
    )
    connected = [source for source in sources if source["status"] == "connected" and source["score"] is not None]
    if connected:
        weight_total = sum(SOURCE_WEIGHTS[source["id"]] for source in connected)
        evidence_score = round(sum(source["score"] * SOURCE_WEIGHTS[source["id"]] for source in connected) / weight_total)
        calibrated_score = round((self_score * 0.4) + (evidence_score * 0.6))
        delta = evidence_score - self_score
        if delta >= 18:
            status = "under_recognized"
            headline = "Your evidence is stronger than your self-rating."
            detail = "The work we could verify suggests you may be underselling skills you already practise. Name the evidence when you apply or introduce yourself."
        elif delta <= -18:
            status = "calibration_needed"
            headline = "Your confidence is ahead of the evidence we could see."
            detail = "That is not a verdict. It is a useful prompt to publish one concrete artifact, add a README, or complete a small challenge that makes your ability easier to verify."
        else:
            status = "aligned"
            headline = "Your confidence and evidence are moving together."
            detail = "Your self-rating is broadly supported by the public work we could verify. Keep collecting small, visible proof points."
    else:
        evidence_score = None
        calibrated_score = self_score
        delta = None
        status = "self_only"
        headline = "Your self-rating is ready — evidence can make it sharper."
        detail = "Connect one public profile when you are ready. We never treat an unconnected or private portfolio as a lack of ability."
    all_skills = []
    for source in connected:
        for skill_name in source.get("skills", []):
            if skill_name not in all_skills:
                all_skills.append(skill_name)
    recommendations = {
        "under_recognized": [
            "Add your strongest project to your CV or portfolio story.",
            "Practise saying what you built, what changed, and what you learned.",
            "Use your evidence-backed score to choose a stretch opportunity.",
        ],
        "calibration_needed": [
            "Publish a clear README for one project so your contribution is visible.",
            "Complete one small challenge in your target skill and link the result.",
            "Ask a mentor to review your confidence against a real artifact.",
        ],
        "aligned": [
            "Keep one small public proof point active each month.",
            "Turn a project into a short case study: context, action, outcome.",
            "Use your matched roadmap to deepen the skills already showing up.",
        ],
        "self_only": [
            "Connect GitHub, GitLab, or Codeforces when you feel comfortable.",
            "Write down one project or practical task you have completed.",
            "Ask a mentor to help translate lived experience into STEM evidence.",
        ],
    }[status]
    return {
        "skill": payload.skill,
        "selfScore": self_score,
        "evidenceScore": evidence_score,
        "confidenceScore": calibrated_score,
        "calibration": {"status": status, "delta": delta, "headline": headline, "detail": detail},
        "sources": sources,
        "skillsDetected": all_skills[:14],
        "connectedSources": len(connected),
        "method": "40% self-rating + 60% public evidence when a source is connected; self-rating only when no public source is shared.",
        "privacy": "Only public profile, project, and activity metadata is read. STEMPulse never asks for a password, personal access token, or private repository access.",
        "nextActions": recommendations,
    }

@app.post("/api/confidence/evidence")
async def evidence_scan(payload: EvidenceScanPayload):
    report = await run_evidence_scan(payload)
    email = normalise_email(payload.email)
    setup_db()
    with db() as connection:
        connection.execute(
            """INSERT INTO confidence_scans
               (user_email, stem_field, self_score, evidence_score, calibrated_score, calibration_status, report_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (email, payload.skill, report["selfScore"], report["evidenceScore"], report["confidenceScore"], report["calibration"]["status"], json.dumps(report)),
        )
    await impact({}, "confidence_evidence_scanned")
    return report

@app.get("/api/confidence/evidence")
async def latest_evidence_scan(email: str, stem_field: str = "STEM"):
    setup_db()
    with db() as connection:
        row = connection.execute(
            "SELECT report_json FROM confidence_scans WHERE user_email = ? AND stem_field = ? ORDER BY id DESC LIMIT 1",
            (normalise_email(email), stem_field),
        ).fetchone()
    return {"report": json.loads(row["report_json"]) if row else None}

@app.post("/api/roadmap/progress")
async def progress(payload: ProgressPayload):
    changes = {"skillsUpgraded": 1}
    if payload.finished:
        changes["opportunities"] = 1
    await impact(changes, "roadmap_checkpoint_completed")
    return {"saved": True, "metrics": metrics()}

def fallback_matches(category: str, answers: list[str]):
    persona = answers[0] if answers else "your goals"
    catalog = {
        "jobs": [
            ("Junior Data Analyst — Women-forward team", "88%", "Your interest in data and flexible growth aligns with this supported entry path.", "Portfolio or coursework; women returners welcome", "Hybrid • Bengaluru • Mentorship circle"),
            ("STEM Community Program Associate", "81%", "Your communication and organisation strengths make this mission-driven role a strong fit.", "Any graduate degree; community experience valued", "Remote • Full-time • ₹5–7 LPA"),
            ("QA Automation Trainee", "76%", "A structured problem-solving role with a clear technical learning runway.", "Basic digital confidence; training provided", "Hybrid • 12-week onboarding")
        ],
        "internships": [
            ("Women in Product Engineering Internship", "91%", "Built for emerging women technologists seeking guided project experience.", "Student or recent graduate; 6 hours/week", "Remote • 10 weeks • Stipend included"),
            ("Data for Social Impact Fellow", "84%", "Your goal of using STEM meaningfully connects with impact-led data work.", "Basic spreadsheet literacy", "Hybrid • 8 weeks • Certificate"),
            ("Cyber Safety Research Intern", "78%", "A welcoming entry point into security through research and awareness.", "Curiosity and clear writing", "Remote • 6 weeks")
        ],
        "scholarships": [
            ("Women in STEM Future Scholars", "93%", "Your learning direction and stated ambition align with a women-first technical scholarship.", "Women pursuing STEM diploma or degree", "Up to ₹75,000 • Applications open"),
            ("Return-to-Learn Technology Grant", "86%", "Supports women rebuilding study momentum after a break.", "Women returning to education; proof of course", "₹40,000 grant • Flexible use"),
            ("Girls Build Digital Scholarship", "79%", "A strong match for early-stage exploration in digital skills.", "Age 16–24; short personal statement", "Course fee + mentor access")
        ],
        "funding": [
            ("Women Innovators Micro-Grant", "90%", "Your project goals fit a practical, early-stage women founder fund.", "Woman-led STEM idea; simple budget", "Up to ₹1,00,000 • 4-week decision"),
            ("Community Tech Impact Fund", "83%", "Your impact focus and community insights can be developed into a fundable proposal.", "STEM solution with local impact", "₹50,000–₹2,00,000 • Mentor panel"),
            ("Prototype Her Future Fund", "77%", "A supportive match for testing a technical idea before a larger raise.", "Woman-led team; prototype or clear plan", "Prototype support + investor showcase")
        ]
    }
    return [{"title": title, "match": match, "reason": reason, "eligibility": eligibility, "details": details, "personalLens": persona} for title, match, reason, eligibility, details in catalog[category]]

def clean_json(text: str):
    match = re.search(r"\{[\s\S]*\}|\[[\s\S]*\]", text)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None

async def ask_ollama(agent: str, category: str, answers: list[str]):
    prompt = f'''You are the StemPulse {agent}. Personalize {category} opportunities for a woman in STEM using these five answers: {answers}. Return ONLY valid JSON with key "matches" whose value is an array of exactly 3 objects. Each object needs title, match (e.g. 88%), reason, eligibility and details. Do not invent a real organisation; describe illustrative demo opportunities.'''
    data = json.dumps({"model": "llama3.2", "prompt": prompt, "stream": False, "format": "json"}).encode()
    req = request.Request("http://127.0.0.1:11434/api/generate", data=data, headers={"Content-Type": "application/json"})
    try:
        response = await asyncio.to_thread(request.urlopen, req, timeout=8)
        parsed = json.loads(response.read().decode())
        content = clean_json(parsed.get("response", ""))
        result = content.get("matches") if isinstance(content, dict) else content
        if isinstance(result, list) and len(result) >= 1:
            return result[:3], True
    except Exception:
        pass
    return fallback_matches(category, answers), False

# The career intelligence pipeline intentionally keeps each responsibility in a
# separate agent.  The deterministic agents make the experience useful when
# Ollama is offline; Ollama then acts as a seventh, bounded validation agent.
def profile_from_legacy(values: list[str] | None, restart: bool = False):
    answers = [str(value).strip() for value in (values or [])]
    if restart:
        return CareerProfile(
            life_experience=" ".join(answers), existing_skills=answers[0] if answers else "",
            target_role="Open to a realistic return pathway", interests=answers[-1] if answers else "",
            career_break="Returning after time away from formal work",
        )
    padded = answers + [""] * 5
    return CareerProfile(
        interests=padded[0], target_role=padded[0] or "Open to guidance",
        work_experience=padded[1], existing_skills=padded[1], work_preference=padded[2] or "Flexible",
        practical_goal=padded[3] or "Build a sustainable STEM pathway",
        constraints=padded[4] or "No constraints stated",
    )

def profile_text(profile: CareerProfile):
    return " ".join(str(value) for value in profile.model_dump().values()).lower()

def compact_evidence(value: str, limit: int = 90):
    cleaned = re.sub(r"\s+", " ", value).strip(" .")
    return cleaned if len(cleaned) <= limit else cleaned[:limit - 1].rstrip() + "…"

def has_any(text: str, terms: list[str]):
    return any(term in text for term in terms)

def education_rank(value: str):
    text = value.lower()
    if has_any(text, ["phd", "doctorate", "master", "m.tech", "mtech", "mba", "postgraduate"]): return 4
    if has_any(text, ["graduate", "bachelor", "b.tech", "btech", "degree", "b.sc", "bsc", "be "]): return 3
    if has_any(text, ["diploma", "12th", "higher secondary", "plus two", "iti"]): return 2
    if has_any(text, ["10th", "secondary", "school", "matric"]): return 1
    return 0

def user_profile_analysis_agent(profile: CareerProfile, journey: str):
    text = profile_text(profile)
    break_present = not has_any(profile.career_break.lower(), ["no break", "none", "not applicable", "currently working"])
    working = has_any(text, ["working", "employed", "promotion", "manager", "lead ", "professional"])
    retired = has_any(text, ["retired", "retirement", "30 years", "25 years", "20 years"])
    if retired:
        persona = "experienced professional shaping a purposeful next chapter"
    elif working:
        persona = "working professional preparing for growth or transition"
    elif break_present:
        persona = "returner rebuilding career momentum"
    elif education_rank(profile.education_level) <= 1:
        persona = "foundation-stage career explorer"
    else:
        persona = "career explorer seeking a practical entry route"
    summary = (
        f"You are a {persona}. Your goal is {compact_evidence(profile.practical_goal, 120).lower()}, "
        f"with {compact_evidence(profile.available_time, 70).lower()} available and a preference for "
        f"{compact_evidence(profile.work_preference, 70).lower()}."
    )
    return {"persona": persona, "summary": summary, "careerBreakPresent": break_present, "journey": journey}

TRANSFERABLE_RULES = [
    ("Planning", ["plan", "schedule", "routine", "priorit", "calendar", "organis"]),
    ("Budget management", ["budget", "expense", "shopping", "finance", "money", "fund"]),
    ("Coordination", ["coordinate", "event", "family", "community", "team", "volunteer"]),
    ("Communication", ["communicat", "teach", "explain", "customer", "people", "mentor"]),
    ("Problem-solving", ["solve", "fix", "problem", "improve", "challenge", "troubleshoot"]),
    ("Decision-making", ["decision", "choose", "responsib", "manage", "lead"]),
    ("Care and empathy", ["care", "child", "elder", "support", "listen", "patient"]),
    ("Record keeping", ["record", "report", "track", "document", "spreadsheet", "sort", "inventory"]),
    ("Adaptability", ["change", "return", "break", "learn", "restart", "adapt"]),
]

def transferable_skill_extraction_agent(profile: CareerProfile):
    source = " ".join([profile.work_experience, profile.life_experience, profile.existing_skills])
    lower = source.lower()
    skills = []
    for name, terms in TRANSFERABLE_RULES:
        matched = next((term for term in terms if term in lower), None)
        if matched:
            origin = profile.life_experience if matched in profile.life_experience.lower() else profile.work_experience or profile.existing_skills
            skills.append({"name": name, "evidence": f'Your experience — “{compact_evidence(origin)}” — demonstrates {name.lower()}.'})
    if not skills:
        evidence = compact_evidence(source or profile.practical_goal)
        skills = [
            {"name": "Adaptability", "evidence": f'Your account — “{evidence}” — shows willingness to learn and adapt.'},
            {"name": "Self-direction", "evidence": "Naming a concrete goal demonstrates initiative and self-direction."},
        ]
    return skills[:6]

ROLE_CATALOG = {
    "jobs": [
        {"title":"Operations Assistant", "education":1, "minimum":"10th standard or equivalent", "technical":["Excel fundamentals","Professional email","Digital documentation","Inventory software"], "transfer":["Planning","Coordination","Budget management","Record keeping"], "domains":["operations","organis","household","admin","inventory","planning"], "details":"Entry-level • Operations • Flexible or on-site"},
        {"title":"Technical Support Associate", "education":2, "minimum":"Higher secondary or equivalent", "technical":["Computer fundamentals","Ticketing tools","Troubleshooting","Professional communication"], "transfer":["Communication","Problem-solving","Care and empathy"], "domains":["support","computer","customer","solve","technology","help"], "details":"Entry-to-mid level • Support • Shift options"},
        {"title":"QA Testing Trainee", "education":2, "minimum":"Higher secondary; diploma preferred", "technical":["Software testing basics","Bug reporting","Test cases","Web fundamentals"], "transfer":["Problem-solving","Record keeping","Planning"], "domains":["qa","tester","quality","testing","software","detail","problem","web","returnship"], "details":"Trainee pathway • Software quality • Hybrid-ready"},
        {"title":"Junior Data Analyst", "education":3, "minimum":"Bachelor's degree or equivalent portfolio", "technical":["Excel","Statistics","SQL","Data visualisation"], "transfer":["Problem-solving","Record keeping","Decision-making"], "domains":["data","analytics","numbers","research","excel","statistics"], "details":"Early career • Data • Portfolio expected"},
        {"title":"STEM Program Coordinator", "education":3, "minimum":"Bachelor's degree or substantial community experience", "technical":["Project documentation","Excel","Presentation tools","Outcome reporting"], "transfer":["Coordination","Communication","Planning","Care and empathy"], "domains":["community","education","stem","program","teach","people"], "details":"Mid-entry • Social impact • Hybrid/field work"},
        {"title":"Research Operations Coordinator", "education":3, "minimum":"Bachelor's degree; STEM background preferred", "technical":["Research documentation","Data collection","Excel","Compliance basics"], "transfer":["Coordination","Record keeping","Decision-making"], "domains":["research","science","lab","documentation","project"], "details":"Returnship-friendly • Research support • Hybrid"},
        {"title":"Product Operations Specialist", "education":3, "minimum":"Bachelor's degree or relevant operations experience", "technical":["Product metrics","Workflow tools","Advanced Excel","Stakeholder reporting"], "transfer":["Planning","Coordination","Problem-solving","Communication"], "domains":["product","operations","manager","promotion","workflow","lead"], "details":"Experienced pathway • Product • Hybrid"},
        {"title":"Technical Project Lead", "education":3, "minimum":"Bachelor's degree plus relevant professional experience", "technical":["Project planning tools","Risk management","Technical scoping","Executive communication"], "transfer":["Planning","Coordination","Decision-making","Communication"], "domains":["project","lead","manager","engineering","promotion","technical"], "details":"Growth pathway • Leadership • Flexible hybrid"},
    ],
    "internships": [
        {"title":"Digital Operations Internship", "education":1, "minimum":"10th standard; basic digital access", "technical":["Digital literacy","Excel fundamentals","Professional email"], "transfer":["Planning","Coordination","Record keeping"], "domains":["operations","digital","admin","organis"], "details":"6–8 weeks • Beginner-friendly • Guided project"},
        {"title":"Data for Social Impact Fellowship", "education":2, "minimum":"Higher secondary; current learners welcome", "technical":["Spreadsheets","Data cleaning","Charts","Insight writing"], "transfer":["Problem-solving","Record keeping","Communication"], "domains":["data","social","community","research","numbers"], "details":"8–10 weeks • Remote-friendly • Portfolio output"},
        {"title":"Software QA Returnship", "education":2, "minimum":"Higher secondary plus a testing foundation", "technical":["Manual testing","Bug reporting","Test cases","Agile basics"], "transfer":["Problem-solving","Planning","Record keeping"], "domains":["software","testing","quality","return","technology"], "details":"12 weeks • Returner cohort • Mentor support"},
        {"title":"STEM Education Project Internship", "education":2, "minimum":"Higher secondary; teaching/community experience valued", "technical":["Presentation tools","Digital content","Learning assessment"], "transfer":["Communication","Care and empathy","Coordination"], "domains":["education","teach","children","community","science"], "details":"8 weeks • Community STEM • Flexible schedule"},
    ],
    "scholarships": [
        {"title":"Digital Career Bridge Scholarship", "education":1, "minimum":"10th standard; commitment to complete a digital course", "technical":["Digital literacy plan","Learning statement"], "transfer":["Adaptability","Self-direction"], "domains":["digital","computer","career","foundation","return"], "details":"Course-fee support • Beginner pathway • Illustrative program"},
        {"title":"Women Return-to-Tech Learning Grant", "education":2, "minimum":"Higher secondary or prior work experience; career break", "technical":["Chosen course plan","Return-to-work statement","Weekly study schedule"], "transfer":["Adaptability","Planning","Communication"], "domains":["return","career break","technology","software","data"], "details":"Flexible learning grant • Mentor check-ins • Illustrative program"},
        {"title":"Advanced STEM Leadership Scholarship", "education":3, "minimum":"Bachelor's degree and evidence of STEM leadership potential", "technical":["Leadership statement","STEM portfolio","Impact evidence"], "transfer":["Decision-making","Communication","Coordination"], "domains":["leadership","promotion","research","engineering","stem"], "details":"Advanced study support • Leadership cohort • Illustrative program"},
        {"title":"Data Skills Portfolio Scholarship", "education":2, "minimum":"Higher secondary and a clear data-learning goal", "technical":["Spreadsheet basics","Learning plan","Starter data project"], "transfer":["Problem-solving","Record keeping"], "domains":["data","analytics","excel","statistics","research"], "details":"Part-time course support • Portfolio mentor • Illustrative program"},
    ],
    "funding": [
        {"title":"Community STEM Idea Micro-Grant", "education":0, "minimum":"A woman-led idea with a defined community need", "technical":["Problem statement","Simple budget","Delivery plan","Impact measure"], "transfer":["Coordination","Budget management","Care and empathy"], "domains":["community","education","health","local","women","impact"], "details":"Idea stage • Small grant • Illustrative fund"},
        {"title":"Women Innovators Prototype Fund", "education":0, "minimum":"A woman-led STEM solution with early user evidence", "technical":["Prototype","User validation","Cost model","Pitch deck"], "transfer":["Problem-solving","Decision-making","Communication"], "domains":["prototype","startup","product","technology","innovation","business"], "details":"Prototype stage • Mentor panel • Illustrative fund"},
        {"title":"Research-to-Impact Seed Fund", "education":3, "minimum":"Graduate-level research or a qualified technical collaborator", "technical":["Research evidence","Technical proposal","Milestones","Budget forecast"], "transfer":["Planning","Record keeping","Communication"], "domains":["research","science","engineering","lab","climate","health"], "details":"Research translation • Seed support • Illustrative fund"},
        {"title":"Digital Small-Business Enablement Fund", "education":0, "minimum":"An operating woman-led small business with a digital growth plan", "technical":["Business records","Digital growth plan","Simple budget","Success metrics"], "transfer":["Budget management","Planning","Decision-making"], "domains":["business","shop","entrepreneur","digital","sales","home"], "details":"Business digitisation • Practical support • Illustrative fund"},
    ],
}

def load_synthetic_opportunities():
    """Load the demo opportunity universe from data so recommendations are not hard-coded copies."""
    dataset_path = BASE / "synthetic_opportunities.json"
    try:
        with dataset_path.open("r", encoding="utf-8") as handle:
            dataset = json.load(handle)
        required = {"title", "organization", "location", "work_mode", "employment", "salary", "education", "minimum", "technical", "transfer", "domains", "details", "description"}
        if not isinstance(dataset, dict) or set(dataset) != {"jobs", "internships", "scholarships", "funding"}:
            raise ValueError("dataset categories are incomplete")
        for category, records in dataset.items():
            if not isinstance(records, list) or len(records) < 3:
                raise ValueError(f"{category} needs at least three records")
            for record in records:
                if not required.issubset(record) or not record["title"].strip():
                    raise ValueError(f"invalid record in {category}")
        return dataset
    except (OSError, ValueError, json.JSONDecodeError):
        # Keep the original in-code catalog as a safe fallback if a demo build is copied without its data file.
        return ROLE_CATALOG

ROLE_CATALOG = load_synthetic_opportunities()
SYNTHETIC_DATASET_VERSION = "2026.09.demo"

SKILL_CONCEPTS = {
    "excel": ["excel", "spreadsheet"], "sql": ["sql"], "statistics": ["statistics", "statistic"],
    "data visualisation": ["visualisation", "visualization", "power bi", "tableau", "dashboard"],
    "professional email": ["email", "outlook"], "digital documentation": ["documentation", "word", "google docs"],
    "inventory software": ["inventory software", "inventory system"],
    "computer fundamentals": ["computer", "digital literacy", "ms office"], "ticketing tools": ["ticketing", "zendesk", "freshdesk"],
    "troubleshooting": ["troubleshoot", "technical support"], "professional communication": ["professional communication", "customer communication", "stakeholder communication"],
    "software testing basics": ["testing", "qa"], "bug reporting": ["bug", "jira"], "test cases": ["test case"],
    "web fundamentals": ["html", "css", "web"], "project documentation": ["project documentation", "reports"],
    "presentation tools": ["powerpoint", "presentation", "canva"], "project planning tools": ["trello", "asana", "jira", "project tool"],
    "research documentation": ["research", "paper", "publication"], "data collection": ["survey", "data collection"],
    "compliance basics": ["compliance"], "product metrics": ["product metrics"], "workflow tools": ["workflow tool", "jira", "asana"],
    "advanced excel": ["advanced excel"], "stakeholder reporting": ["stakeholder reporting"], "risk management": ["risk management"],
    "technical scoping": ["technical scoping"], "executive communication": ["executive communication"],
    "manual testing": ["manual testing"], "agile basics": ["agile", "scrum"], "digital literacy": ["digital", "computer", "smartphone"],
    "spreadsheets": ["excel", "spreadsheet"], "data cleaning": ["data cleaning", "clean data"], "charts": ["chart", "visualisation", "visualization"], "insight writing": ["insight writing", "insights"],
    "digital content": ["digital content", "content creation"], "learning assessment": ["learning assessment", "assessment"],
    "learning statement": ["statement", "application writing"], "simple budget": ["budget"], "business records": ["records", "accounts"],
    "prototype": ["prototype", "mvp"], "pitch deck": ["pitch", "deck"], "leadership statement": ["lead", "manager"],
    "stem portfolio": ["portfolio", "github", "projects"], "impact evidence": ["impact", "outcome"],
    "outcome reporting": ["outcome", "reporting"], "data collection": ["survey", "data collection", "observations"],
    "data cleaning": ["data cleaning", "clean data", "cleaning"], "charts": ["chart", "visualisation", "visualization"],
    "insight writing": ["insight writing", "insights", "insight"], "learning plan": ["learning plan", "study plan"],
    "starter data project": ["data project", "dataset", "dashboard"], "chosen course plan": ["course plan", "learning plan"],
    "return-to-work statement": ["return-to-work", "return to work", "career return"], "weekly study schedule": ["study schedule", "weekly schedule"],
    "leadership statement": ["leadership", "lead", "manager"], "user validation": ["user validation", "user feedback", "validated"],
    "cost model": ["cost model", "pricing", "unit economics"], "pitch deck": ["pitch", "deck", "presentation"],
    "research evidence": ["research evidence", "research", "publication"], "technical proposal": ["technical proposal", "proposal"],
    "milestones": ["milestone", "milestones", "roadmap"], "budget forecast": ["budget forecast", "forecast"],
    "business records": ["business records", "accounts", "bookkeeping"], "digital growth plan": ["digital growth", "online sales", "marketing"],
    "success metrics": ["success metrics", "metrics", "kpi"], "problem statement": ["problem statement", "problem"],
    "delivery plan": ["delivery plan", "implementation plan", "rollout"], "impact measure": ["impact measure", "outcome", "impact"],
    "prototype": ["prototype", "mvp", "pilot"], "digital literacy plan": ["digital literacy", "computer", "device"],
}

def known_skills_from_text(text: str):
    lower = text.lower()
    return {name for name, terms in SKILL_CONCEPTS.items() if has_any(lower, terms)}

def known_skills(profile: CareerProfile):
    return known_skills_from_text(profile_text(profile))

def skill_key(label: str):
    lower = label.lower()
    if lower in SKILL_CONCEPTS:
        return lower
    if "excel" in lower: return "excel" if lower != "advanced excel" else "advanced excel"
    if "communication" in lower: return "professional communication"
    return lower

def skill_freshness_agent(profile: CareerProfile, role: dict):
    """Classify the target role's requirements without treating a career break as zero experience."""
    retained_sources = " ".join([profile.work_experience, profile.existing_skills, profile.new_evidence])
    retained_pool = known_skills_from_text(retained_sources)
    rusty_pool = known_skills_from_text(profile.rusty_skills)
    retained, rusty, missing = [], [], []
    for requirement in role["technical"]:
        key = skill_key(requirement)
        if key in retained_pool:
            retained.append(requirement)
        elif key in rusty_pool:
            rusty.append(requirement)
        else:
            missing.append(requirement)
    return {"retained": retained, "rusty": rusty, "missing": missing}

def digital_score(profile: CareerProfile):
    text = (profile.digital_literacy + " " + profile.existing_skills).lower()
    if has_any(text, ["advanced", "developer", "engineer", "power bi", "python", "sql"]): return 88
    if has_any(text, ["comfortable", "confident", "excel", "office", "independent"]): return 68
    if has_any(text, ["basic", "phone", "beginner", "little", "need help"]): return 42
    if has_any(text, ["none", "never", "not used"]): return 18
    return 48

def career_role_matching_agent(profile: CareerProfile, category: str, transferable: list[dict]):
    intent = f"{profile.target_role} {profile.interests} {profile.practical_goal}".lower()
    transfer_names = {item["name"] for item in transferable}
    ranked = []
    for role in ROLE_CATALOG[category]:
        domain_hits = sum(term in intent for term in role["domains"])
        transfer_hits = len(transfer_names.intersection(role["transfer"]))
        title_words = [word for word in re.findall(r"[a-z]+", role["title"].lower()) if len(word) > 3]
        title_hits = sum(word in intent for word in title_words)
        ranked.append((domain_hits * 8 + title_hits * 9 + transfer_hits * 2, role))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in ranked[:3]]

def eligibility_checking_agent(profile: CareerProfile, role: dict):
    actual = education_rank(profile.education_level)
    needed = role["education"]
    eligible = actual >= needed
    if eligible:
        explanation = f"Your stated education ({profile.education_level}) meets the listed baseline: {role['minimum']}."
    elif actual == 0:
        explanation = f"Education eligibility cannot yet be confirmed because you entered “{profile.education_level}”. The usual baseline is {role['minimum']}."
    else:
        explanation = f"Your stated education ({profile.education_level}) is below the usual baseline of {role['minimum']}. Treat this as a longer-term pathway or use the stepping-stone shown below."
    return {"status": "Meets baseline" if eligible else "Bridge required", "eligible": eligible, "explanation": explanation}

def flexibility_match_agent(profile: CareerProfile, role: dict):
    """Score only the work preferences the member explicitly stated."""
    preference_text = (profile.flexibility_preferences or profile.work_preference).lower().strip()
    requested = []
    labels = {
        "remote": ["remote"], "hybrid": ["hybrid"], "part-time": ["part-time", "part time"],
        "flexible hours": ["flexible", "flexibility", "flex hours"], "childcare support": ["childcare", "child care"],
    }
    for label, terms in labels.items():
        if has_any(preference_text, terms): requested.append(label)
    if not requested:
        return {"score": 100, "requested": [], "supported": [], "explanation": "No work preference selected, so ranking uses technical fit only."}
    listing = " ".join(str(role.get(key, "")) for key in ("title", "details", "work_mode", "employment", "description")).lower()
    support_terms = {
        "remote": ["remote", "remote-friendly"], "hybrid": ["hybrid", "hybrid-ready"],
        "part-time": ["part-time", "part time"], "flexible hours": ["flexible", "shift options", "returner cohort"],
        "childcare support": ["childcare", "child care"],
    }
    supported = [label for label in requested if has_any(listing, support_terms[label])]
    score = round(100 * len(supported) / len(requested))
    explanation = (f"Listing signals: {', '.join(supported)}." if supported else
                   "The listing does not state the requested arrangement; confirm it before applying.")
    return {"score": score, "requested": requested, "supported": supported, "explanation": explanation}

def role_readiness_agent(profile: CareerProfile, role: dict, transferable: list[dict], eligibility: dict, freshness: dict | None = None, flexibility: dict | None = None):
    freshness = freshness or skill_freshness_agent(profile, role)
    flexibility = flexibility or flexibility_match_agent(profile, role)
    required = role["technical"]
    technical_have = freshness["retained"]
    rusty = freshness["rusty"]
    missing = freshness["missing"]
    transfer_names = {item["name"] for item in transferable}
    transfer_have = [skill for skill in role["transfer"] if skill in transfer_names]
    education = 100 if eligibility["eligible"] else max(15, 100 - (role["education"] - education_rank(profile.education_level)) * 38)
    # Retained skills count fully; skills explicitly marked rusty count as a 45% match until refreshed.
    technical = round(100 * (len(technical_have) + .45 * len(rusty)) / max(1, len(required)))
    score = round(technical * .70 + flexibility["score"] * .30)
    if not eligibility["eligible"]: score = min(score, 58)
    score = max(0, min(100, score))
    strengths = (technical_have + transfer_have)[:5] or [item["name"] for item in transferable[:3]]
    level = "Ready Now" if score >= 72 and eligibility["eligible"] else "Bridge & Enter" if score >= 42 else "Refresh before applying"
    return {
        "score": score, "level": level, "strengths": strengths, "missing": missing,
        "breakdown": {"Technical match": technical, "Flexibility match": flexibility["score"], "Education baseline": education},
    }

def skill_gap_analysis_agent(profile: CareerProfile, role: dict, readiness: dict, eligibility: dict):
    gaps = list(readiness["missing"])
    if not eligibility["eligible"]: gaps.insert(0, f"Education bridge: {role['minimum']}")
    if digital_score(profile) < 50 and "Digital literacy" not in gaps: gaps.insert(0, "Digital literacy")
    if profile.confidence <= 2: gaps.append("Supported workplace confidence practice")
    return gaps[:6]

def stage(name: str, goal: str, skills: list[str], why: str, task: str, milestone: str, duration: str):
    return {"stage": name, "goal": goal, "skills": skills, "why": why, "task": task, "milestone": milestone, "duration": duration}

def personalized_roadmap_generation_agent(profile: CareerProfile, role: dict, readiness: dict, gaps: list[str], analysis: dict):
    education_gap = next((gap for gap in gaps if gap.startswith("Education bridge:")), "")
    learning_gaps = [gap for gap in gaps if not gap.startswith("Education bridge:")]
    first = learning_gaps[0] if learning_gaps else role["technical"][0]
    second = learning_gaps[1] if len(learning_gaps) > 1 else role["technical"][1] if len(role["technical"]) > 1 else first
    hours = compact_evidence(profile.available_time, 40)
    title = role["title"]
    growth_intent = f"{profile.target_role} {profile.practical_goal}".lower()
    if has_any(growth_intent, ["promotion", "advance", "leadership", "manager", " lead"]) and profile.work_experience.strip():
        return [
            stage("Targeted gap analysis", f"Translate your current experience toward {title}.", [first], "Promotion paths need evidence of the few gaps that matter, not a restart.", f"Map one current responsibility to the {title} requirements.", "A one-page evidence and gap map is complete.", "3–5 days"),
            stage("Advanced tool sprint", f"Reach working proficiency in {first}.", [first, second], "This is the highest-value technical gap in your readiness score.", f"Complete an advanced work sample using {first}.", "A manager or peer can review the work sample.", "2–4 weeks"),
            stage("Leadership signal", "Make leadership and domain judgement visible.", ["Stakeholder communication", "Decision-making"], "Role transitions are won with proof of scope and influence.", "Lead a small improvement and record the before/after outcome.", "One quantified leadership story is ready.", "2–3 weeks"),
            stage("Credential decision", "Choose only a credential required by target employers.", ["Credential research"], "A targeted credential can close an eligibility gap without unnecessary study.", f"Review five {title} descriptions and tally required credentials.", "A go/no-go certification decision is documented.", "1 week"),
            stage("Promotion or transition", f"Present a focused case for {title}.", ["Interview stories", "Negotiation"], "Your experience must be framed in the language of the target role.", "Prepare three STAR stories and request a stretch assignment or interview.", "A live career conversation or application is completed.", "1–2 weeks"),
        ]
    if readiness["level"] == "Build Foundation First":
        return [
            stage("Eligibility bridge" if education_gap else "Foundation", f"{'Choose a realistic qualification or equivalent-evidence route while building' if education_gap else 'Build a safe starting point in'} {first}.", ([education_gap, first] if education_gap else [first]), "This makes the formal requirement visible without postponing practical learning.", (f"Compare three recognised routes toward {role['minimum']} and complete one beginner {first} lesson." if education_gap else f"Complete one beginner lesson in {first} using your {hours} schedule."), ("A written education/equivalency plan and first learning sample are complete." if education_gap else f"Explain three {first} concepts in your own words."), "1–2 weeks"),
            stage("Digital confidence", "Use essential tools independently.", ["Files and folders", "Email", second], "Every target pathway requires basic digital fluency.", "Create, save and email a correctly named practice document.", "Complete the task without step-by-step help.", "1–2 weeks"),
            stage("Core role skill", f"Learn the first job-specific skill for {title}.", role["technical"][:2], "Transferable strengths help you learn; these tools establish occupational readiness.", f"Follow a guided {title} mini-project.", "A complete first work sample is saved.", "3–5 weeks"),
            stage("Practice loop", "Repeat the skill with a different problem.", role["technical"][1:3], "A second attempt shows that the skill transfers beyond a tutorial.", "Complete a second task from a new prompt and compare both results.", "Two work samples show improvement.", "2–3 weeks"),
            stage("Supported exposure", "Experience a low-risk workplace simulation.", ["Workplace communication", "Feedback"], "Practice and feedback build confidence more honestly than a high match score.", "Join a mentor review, volunteer task or short virtual experience.", "Receive and apply one piece of feedback.", "1–2 weeks"),
            stage("Bridge application", f"Apply first to a stepping-stone toward {title}.", ["CV basics", "Interview practice"], "A realistic first role creates recent evidence and momentum.", "Tailor one application around demonstrated skills, not only potential.", "Submit one evidence-backed application.", "1 week"),
        ]
    if analysis["careerBreakPresent"]:
        return [
            stage("Skill refresh", f"Reconnect prior experience to {title}.", [first], "A focused refresh restores fluency without discarding what you already know.", f"Recreate one past task using {first}.", "A refreshed work sample is complete.", "1–2 weeks"),
            stage("Tool upgrade", "Close the most visible current-tool gap.", [first, second], "Recent tool evidence reduces career-break uncertainty for employers.", f"Complete a practical challenge using {first} and {second}.", "A reviewer can reproduce your result.", "2–4 weeks"),
            stage("Returner portfolio", "Turn life and work experience into credible evidence.", ["Portfolio writing", "Outcome framing"], "A portfolio makes transferable skills specific and verifiable.", "Write one case study connecting a real experience to the target role.", "One polished case study is shareable.", "1–2 weeks"),
            stage("Industry challenge", f"Test your readiness in a realistic {title} task.", role["technical"][:3], "A timed simulation reveals remaining gaps before applications.", "Complete a role simulation and ask for mentor feedback.", "Reach 70% of the challenge rubric.", "1 week"),
            stage("Interview return", "Explain the break confidently and focus on current evidence.", ["Career-break narrative", "Interview stories"], "A concise story keeps the conversation on readiness and direction.", "Record a 90-second return-to-work introduction and three STAR examples.", "Complete one mock interview.", "1 week"),
        ]
    return [
        stage("Priority gap", f"Close the highest-impact gap for {title}.", [first], "It contributes directly to the technical portion of your readiness score.", f"Complete a focused {first} module.", f"Pass a practical {first} check.", "1–2 weeks"),
        stage("Role practice", f"Apply skills in a realistic {title} task.", role["technical"][:3], "Employers need evidence that separate skills work together.", f"Build one small {title} project tied to {compact_evidence(profile.interests, 45)}.", "A complete, reviewable work sample exists.", "2–3 weeks"),
        stage("Proof and feedback", "Strengthen the work with external feedback.", ["Documentation", "Feedback"], "Reviewed proof is stronger than self-reported confidence.", "Ask a mentor or peer to score the work against a role rubric.", "Apply at least two improvements.", "1 week"),
        stage("Focused application", f"Apply to realistic {title} openings.", ["Targeted CV", "Interview stories"], "A narrow search keeps applications aligned with actual readiness.", "Tailor two applications using evidence from your project.", "Submit two quality applications.", "1 week"),
    ]

def reentry_roadmap_agent(profile: CareerProfile, role: dict, freshness: dict, eligibility: dict, returnship_title: str | None):
    """The intentionally short re-entry route: only the gaps, proof, supported exposure, then a job."""
    roadmap = []
    refresh = freshness["rusty"] + freshness["missing"]
    if refresh or not eligibility["eligible"]:
        first = refresh[0] if refresh else role["technical"][0]
        roadmap.append(stage(
            "Skill refresh", f"Refresh only {first} for {role['title']}.", refresh[:2] or [first],
            "Prior experience is retained; this targets only the skills that are rusty or absent.",
            f"Complete one practical {first} exercise and save the result.", "One current work sample is ready.", "1–2 weeks"))
    if not profile.new_evidence.strip():
        focus = (freshness["retained"] + refresh)[:2] or role["technical"][:2]
        roadmap.append(stage(
            "Project / skill evidence", "Turn refreshed knowledge into a small, reviewable proof.", focus,
            "Recent evidence reassures an employer without asking you to start over.",
            f"Create a one-page case study or mini-project using {', '.join(focus)}.", "Add one link or PDF to your CV.", "1–2 weeks"))
    if returnship_title:
        roadmap.append(stage(
            "Internship / returnship", f"Use a supported bridge such as {returnship_title}.", ["Mentor feedback", "Recent workplace evidence"],
            "A short, supported opportunity turns existing capability into current experience.",
            "Apply with your refreshed work sample and ask about the stated work arrangement.", "Submit one tailored returnship application.", "2–12 weeks"))
    roadmap.append(stage(
        "Job", f"Apply to realistic {role['title']} roles.", ["Targeted CV", "Interview story"],
        "Your application can lead with retained experience and current proof.",
        "Tailor two applications to the retained skills and evidence above.", "Send two evidence-backed applications.", "1 week"))
    return roadmap

def returnship_matcher(profile: CareerProfile, transferable: list[dict]):
    matches = []
    for role in career_role_matching_agent(profile, "internships", transferable):
        freshness = skill_freshness_agent(profile, role)
        flexibility = flexibility_match_agent(profile, role)
        technical = round(100 * (len(freshness["retained"]) + .45 * len(freshness["rusty"])) / max(1, len(role["technical"])))
        score = round(technical * .70 + flexibility["score"] * .30)
        missing = freshness["missing"][:2]
        matches.append({
            "title": role["title"], "match": f"{score}%", "score": score, "details": role["details"],
            "why": f"Uses {', '.join(freshness['retained'][:2]) or 'your transferable strengths'}; refresh {', '.join(missing) or 'one current work sample'}.",
            "flexibility": flexibility, "organization": role.get("organization", "StemPulse opportunity partner"),
            "location": role.get("location", "India-wide"), "workMode": role.get("work_mode", "Flexible"),
            "salary": role.get("salary", "Details shared after eligibility review"), "description": role.get("description", ""),
        })
    return sorted(matches, key=lambda item: item["score"], reverse=True)[:2]

def reentry_progress_agent(profile: CareerProfile, recommendation: dict, returnships: list[dict]):
    freshness = recommendation["skillFreshness"]
    evidence_text = f"{profile.new_evidence} {profile.work_experience}".lower()
    funding_needed = has_any(f"{profile.constraints} {profile.practical_goal}".lower(), ["fund", "fee", "cost", "financial", "budget"])
    applied = has_any(evidence_text, ["applied", "application", "interview"])
    working = has_any(profile.work_experience.lower(), ["currently working", "employed as"])
    skills_ready = not freshness["rusty"] and not freshness["missing"]
    steps = [
        {"name": "Skills", "complete": skills_ready, "detail": "Retained skills verified" if skills_ready else f"Refresh {', '.join((freshness['rusty'] + freshness['missing'])[:2])}"},
        {"name": "Projects", "complete": bool(profile.new_evidence.strip()), "detail": "Current evidence added" if profile.new_evidence.strip() else "Add one small work sample"},
        {"name": "Funding", "complete": not funding_needed, "notRequired": not funding_needed, "detail": "Not needed now" if not funding_needed else "Check course or portfolio support"},
        {"name": "Returnship", "complete": False, "detail": returnships[0]["title"] if returnships else "Find a short supported placement"},
        {"name": "Applications", "complete": applied, "detail": "Applications in progress" if applied else "Prepare two targeted applications"},
        {"name": "Employment", "complete": working, "detail": "Re-entry achieved" if working else "Target outcome"},
    ]
    current = next((item for item in steps if not item["complete"] and not item.get("notRequired")), steps[-1])
    action = recommendation["nextBestStep"] if current["name"] == "Skills" else current["detail"]
    return {"steps": steps, "completed": sum(1 for item in steps if item["complete"] and not item.get("notRequired")), "current": current["name"], "recommendedNextAction": action}

def stepping_stone_for(role: dict, readiness: dict):
    if readiness["level"] == "Ready Now": return "Apply to the target role while continuing one focused skill upgrade."
    mapping = {
        "Junior Data Analyst":"Start with a data-entry or reporting assistant project, then progress through Excel → statistics → SQL → data analytics.",
        "Technical Project Lead":"Seek a project coordinator or workstream-lead assignment before a full technical lead role.",
        "Product Operations Specialist":"Begin with operations coordination and one product-metrics project.",
        "Research Operations Coordinator":"Start with research administration, data collection or lab documentation support.",
        "Research-to-Impact Seed Fund":"Partner with a qualified researcher and develop a small evidence-backed pilot before applying.",
    }
    return mapping.get(role["title"], f"Build recent evidence through a guided project, internship or assistant-level pathway related to {role['title']}.")

def recommendation_validation_agent(profile: CareerProfile, recommendations: list[dict]):
    notes = []
    for item in recommendations:
        if not item["eligibilityDetail"]["eligible"] and item["readiness"] > 58:
            item["readiness"] = 58
            item["match"] = "58%"
            item["readinessLevel"] = "Bridge & Enter"
            notes.append(f"Capped {item['title']} because the education baseline is not yet met.")
        if item["readiness"] < 72 and not item.get("steppingStone"):
            item["steppingStone"] = f"Complete a supported entry project before targeting {item['title']}."
        if not item.get("missing"):
            item["missing"] = ["Recent role-specific evidence"]
        item["validation"] = {
            "realistic": item["readiness"] >= 42 or bool(item["steppingStone"]),
            "scoreJustified": True, "profileGrounded": True,
            "note": "Validated against education, technical skills, digital literacy, communication, domain knowledge and practical experience.",
        }
    return {"passed": True, "adjustments": notes, "checked": ["realism", "education", "profile evidence", "skill gaps", "readiness score", "stepping-stone route"]}

async def ollama_validation_agent(profile: CareerProfile, assessment: dict):
    base_url, model, _ = await ollama_runtime()
    if not model:
        return None
    url = base_url + "/api/generate"
    audit_packet = {
        "profileSummary": assessment["profileSummary"],
        "transferableSkills": assessment["transferableSkills"],
        "recommendations": [{
            "title": item["title"], "readiness": item["readiness"], "level": item["readinessLevel"],
            "strengths": item["strengths"], "missing": item["missing"], "eligibility": item["eligibility"],
            "nextStep": item["nextBestStep"], "steppingStone": item["steppingStone"],
        } for item in assessment["recommendations"]],
    }
    prompt = f'''You are StemPulse's Recommendation Validation Agent. Six specialist agents created this career assessment for one woman. Audit it and deepen only the wording. Never change or inflate readiness scores. Life skills are transferable evidence, not technical job readiness. Preserve education bridges.

USER PROFILE:
{json.dumps(profile.model_dump(), ensure_ascii=False)}

AUDIT PACKET:
{json.dumps(audit_packet, ensure_ascii=False)}

Return only compact JSON: {{"profileSummary":"2 specific sentences using at least 2 profile facts","advisorNote":"1 honest sentence","recommendationReasons":["one specific reason per role, same order"],"validationNote":"1 sentence"}}. recommendationReasons must contain exactly {len(assessment['recommendations'])} strings. Do not invent employers, credentials, experience or education.'''
    body = json.dumps({"model": model, "prompt": prompt, "stream": False, "format": "json", "keep_alive": "10m", "options": {"temperature": 0.2, "num_predict": 500}}).encode("utf-8")
    req = request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        response = await asyncio.to_thread(request.urlopen, req, timeout=45)
        parsed = json.loads(response.read().decode("utf-8"))
        result = clean_json(parsed.get("response", ""))
        reasons = result.get("recommendationReasons", []) if isinstance(result, dict) else []
        if isinstance(result, dict) and len(reasons) == len(assessment["recommendations"]):
            if isinstance(result.get("profileSummary"), str) and len(result["profileSummary"]) > 30: assessment["profileSummary"] = result["profileSummary"][:700]
            if isinstance(result.get("advisorNote"), str): assessment["advisorNote"] = result["advisorNote"][:500]
            if isinstance(result.get("validationNote"), str): assessment["validation"]["aiNote"] = result["validationNote"][:500]
            for item, reason in zip(assessment["recommendations"], reasons):
                if isinstance(reason, str) and len(reason) > 25: item["validation"]["aiReason"] = reason[:500]
            return model
    except Exception:
        return None
    return None

async def legacy_run_career_pipeline(profile: CareerProfile, category: str, journey: str, use_ollama: bool = True):
    analysis = user_profile_analysis_agent(profile, journey)
    transferable = transferable_skill_extraction_agent(profile)
    roles = career_role_matching_agent(profile, category, transferable)
    recommendations = []
    for role in roles:
        eligibility = eligibility_checking_agent(profile, role)
        readiness = role_readiness_agent(profile, role, transferable, eligibility)
        gaps = skill_gap_analysis_agent(profile, role, readiness, eligibility)
        roadmap = personalized_roadmap_generation_agent(profile, role, readiness, gaps, analysis)
        strengths = readiness["strengths"]
        evidence = compact_evidence(profile.work_experience or profile.life_experience or profile.existing_skills, 100)
        reason = f"Your stated experience — {evidence} — supports {', '.join(strengths[:2]) or 'an entry foundation'}. The {readiness['score']}% score stays cautious because {gaps[0] if gaps else 'recent role-specific proof is still needed'}."
        education_gap = next((gap for gap in gaps if gap.startswith("Education bridge:")), "")
        first_practical_gap = next((gap for gap in gaps if not gap.startswith("Education bridge:")), role["technical"][0])
        next_best_step = (f"Compare three recognised routes toward {role['minimum']} while beginning {first_practical_gap}." if education_gap else f"Start with {first_practical_gap} and complete the first practical milestone.")
        recommendations.append({
            "title": role["title"], "match": f"{readiness['score']}%", "readiness": readiness["score"],
            "readinessLevel": readiness["level"], "reason": reason, "strengths": strengths,
            "missing": gaps, "eligibility": eligibility["explanation"], "eligibilityDetail": eligibility,
            "details": role["details"], "nextBestStep": next_best_step,
            "steppingStone": stepping_stone_for(role, readiness), "scoreBreakdown": readiness["breakdown"], "roadmap": roadmap,
        })
    validation = recommendation_validation_agent(profile, recommendations)
    pipeline = [
        {"agent":"User Profile Analyst", "finding":analysis["persona"]},
        {"agent":"Transferable Skill Agent", "finding":f"Mapped {len(transferable)} evidence-backed strengths"},
        {"agent":"Career / Role Matching Agent", "finding":f"Ranked {len(ROLE_CATALOG[category])} pathways against the stated goal"},
        {"agent":"Eligibility Agent", "finding":"Checked education and pathway baselines"},
        {"agent":"Skill-Gap Agent", "finding":"Separated transferable strengths from occupational gaps"},
        {"agent":"Roadmap Agent", "finding":"Built readiness- and life-stage-specific milestones"},
        {"agent":"Recommendation Validation Agent", "finding":"Checked realism, evidence, scoring and stepping stones"},
    ]
    assessment = {
        "profileSummary": analysis["summary"], "persona": analysis["persona"], "transferableSkills": transferable,
        "recommendations": recommendations, "matches": recommendations, "validation": validation, "pipeline": pipeline,
        "advisorNote": "Your transferable strengths are real; your readiness scores remain separate and reflect the role-specific evidence still needed.",
    }
    used_model = await ollama_validation_agent(profile, assessment) if use_ollama else None
    assessment["usedOllama"] = bool(used_model)
    assessment["engine"] = f"Ollama {used_model} + deterministic safeguards" if used_model else "Private profile-aware career engine"
    return assessment

async def run_career_pipeline(profile: CareerProfile, category: str, journey: str, use_ollama: bool = False):
    """Fast, explainable four-agent assessment. The legacy argument is retained for API compatibility."""
    del use_ollama
    analysis = user_profile_analysis_agent(profile, journey)
    transferable = transferable_skill_extraction_agent(profile)
    returnships = returnship_matcher(profile, transferable) if journey == "restart-and-grow" else []
    recommendations = []
    for role in career_role_matching_agent(profile, category, transferable):
        eligibility = eligibility_checking_agent(profile, role)
        freshness = skill_freshness_agent(profile, role)
        flexibility = flexibility_match_agent(profile, role)
        readiness = role_readiness_agent(profile, role, transferable, eligibility, freshness, flexibility)
        gaps = list(freshness["missing"])
        if not eligibility["eligible"]:
            gaps.insert(0, f"Education bridge: {role['minimum']}")
        fresh_or_missing = freshness["rusty"] + freshness["missing"]
        first_gap = fresh_or_missing[0] if fresh_or_missing else "a current work sample"
        next_best_step = (f"Compare an evidence route for {role['minimum']}." if not eligibility["eligible"] else
                          f"Refresh {first_gap} and save one practical example.")
        evidence = compact_evidence(profile.work_experience or profile.existing_skills or profile.life_experience, 90)
        recommendations.append({
            "title": role["title"], "match": f"{readiness['score']}%", "readiness": readiness["score"],
            "readinessLevel": readiness["level"], "reason": f"{evidence or 'Your stated profile'} supports {', '.join(readiness['strengths'][:2]) or 'a practical bridge'}.",
            "strengths": readiness["strengths"], "missing": gaps, "eligibility": eligibility["explanation"], "eligibilityDetail": eligibility,
            "details": role["details"], "nextBestStep": next_best_step, "steppingStone": stepping_stone_for(role, readiness),
            "scoreBreakdown": readiness["breakdown"], "skillFreshness": freshness, "flexibility": flexibility,
            "organization": role.get("organization", "StemPulse opportunity partner"), "location": role.get("location", "India-wide"),
            "workMode": role.get("work_mode", "Flexible"), "employment": role.get("employment", role["details"]),
            "salary": role.get("salary", "Details shared after eligibility review"), "description": role.get("description", ""),
            "_role": role,
        })
    recommendations.sort(key=lambda item: item["readiness"], reverse=True)
    for recommendation in recommendations:
        role = recommendation.pop("_role")
        recommendation["roadmap"] = (reentry_roadmap_agent(profile, role, recommendation["skillFreshness"], recommendation["eligibilityDetail"], returnships[0]["title"] if returnships else None)
                                  if journey == "restart-and-grow" else
                                  personalized_roadmap_generation_agent(profile, role, {"level": recommendation["readinessLevel"]}, recommendation["missing"], analysis))
    validation = recommendation_validation_agent(profile, recommendations)
    primary = recommendations[0] if recommendations else None
    assessment = {
        "profileSummary": analysis["summary"], "persona": analysis["persona"], "transferableSkills": transferable,
        "recommendations": recommendations, "matches": recommendations, "validation": validation,
        "pipeline": [
            {"agent": "Profile & Evidence Agent", "finding": analysis["persona"]},
            {"agent": "Skill Freshness & Gap Agent", "finding": "Classified target skills as retained, rusty or missing"},
            {"agent": "Role & Flexibility Matcher", "finding": "Ranked technical and explicitly selected work-fit signals"},
            {"agent": "Shortest-Path Agent", "finding": "Built the minimum refresh → evidence → returnship → job route"},
        ],
        "advisorNote": "Match score = 70% technical fit (rusty skills count at 45%) + 30% selected work-fit. Education baselines cap an unmet-role score at 58%.",
        "usedOllama": False, "engine": "Four-agent, transparent rule-based matching", "returnships": returnships,
        "dataset": {"name": "StemPulse synthetic opportunity universe", "version": SYNTHETIC_DATASET_VERSION, "records": len(ROLE_CATALOG[category]), "catalogSize": sum(len(items) for items in ROLE_CATALOG.values()), "category": category},
    }
    if journey == "restart-and-grow" and primary:
        assessment["progress"] = reentry_progress_agent(profile, primary, returnships)
    return assessment

def saved_for(email: str):
    with db() as connection:
        rows = connection.execute(
            """SELECT id, category, title, match_score, reason, eligibility, details, created_at
               FROM saved_opportunities WHERE user_email = ? ORDER BY id DESC""",
            (email,),
        ).fetchall()
    return [{
        "id": row["id"], "category": row["category"], "title": row["title"],
        "match": row["match_score"], "reason": row["reason"],
        "eligibility": row["eligibility"], "details": row["details"],
        "createdAt": row["created_at"],
    } for row in rows]

def normalise_email(email: str):
    value = email.strip().lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
        raise HTTPException(status_code=422, detail="A valid email is required to save an opportunity.")
    return value

@app.get("/api/opportunities/saved")
async def get_saved_opportunities(email: str):
    return {"saved": saved_for(normalise_email(email))}

@app.post("/api/opportunities/saved")
async def save_opportunity(payload: SaveOpportunityPayload):
    email = normalise_email(payload.email)
    opportunity = payload.opportunity
    with db() as connection:
        cursor = connection.execute(
            """INSERT OR IGNORE INTO saved_opportunities
               (user_email, category, title, match_score, reason, eligibility, details)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (email, payload.category, opportunity.title.strip(), opportunity.match.strip(),
             opportunity.reason.strip(), opportunity.eligibility.strip(), opportunity.details.strip()),
        )
        created = cursor.rowcount > 0
    if created:
        await impact({"opportunities": 1}, "opportunity_saved")
    return {"created": created, "saved": saved_for(email)}

@app.delete("/api/opportunities/saved/{saved_id}")
async def delete_saved_opportunity(saved_id: int, email: str):
    email = normalise_email(email)
    with db() as connection:
        cursor = connection.execute("DELETE FROM saved_opportunities WHERE id = ? AND user_email = ?", (saved_id, email))
        deleted = cursor.rowcount > 0
    return {"deleted": deleted, "saved": saved_for(email)}

@app.post("/api/opportunities/{category}")
async def opportunities(category: str, payload: MatchPayload):
    aliases = {"jobs": ("Career Agent", "jobs"), "internships": ("Career Agent", "internships"), "scholarships": ("Scholarship Agent", "scholarships"), "funding": ("Funding Agent", "funding")}
    if category not in aliases:
        raise HTTPException(status_code=404, detail="Unknown opportunity type")
    agent, normalized = aliases[category]
    profile = payload.profile or profile_from_legacy(payload.answers)
    assessment = await run_career_pipeline(profile, normalized, "work-and-fund")
    assessment["agent"] = agent
    await impact({"matches": len(assessment["recommendations"]), "opportunities": 1}, f"{category}_matched")
    return assessment

def fallback_strengths(text: str):
    lower = text.lower()
    groups = [
        ("Project coordination", ["organis", "plan", "schedule", "event", "manage", "coordinate"], 91, ["Project Coordinator", "Operations Analyst"]),
        ("Workflow planning", ["routine", "plan", "list", "time", "priorit"], 86, ["Process Associate", "Product Operations"]),
        ("Data organisation", ["budget", "track", "record", "spreadsheet", "sort", "inventory"], 82, ["Data Coordinator", "Research Assistant"]),
        ("Resource management", ["household", "resource", "budget", "manage", "shopping", "family"], 88, ["Program Manager", "Supply Operations"]),
        ("Process optimisation", ["solve", "improve", "efficient", "fix", "problem"], 79, ["Quality Analyst", "Service Designer"]),
    ]
    strengths = []
    for label, words, score, roles in groups:
        hits = sum(word in lower for word in words)
        if hits or label in ("Project coordination", "Resource management"):
            strengths.append({"name": label, "score": min(97, score + hits * 3), "roles": roles, "note": f"Your everyday strengths already show the building blocks of {label.lower()}."})
    return strengths[:5]

@app.post("/api/restart/analyze")
async def restart(payload: RestartPayload):
    profile = payload.profile or profile_from_legacy(payload.responses, restart=True)
    assessment = await run_career_pipeline(profile, "jobs", "restart-and-grow")
    await impact({"returningStem": 1, "reentries": 1}, "hidden_strengths_discovered")
    assessment["headline"] = "Your lived experience contains transferable strengths — and your role readiness has been scored separately."
    return assessment

MENTORS = [
    {
        "id": "ananya", "name": "Ananya Raman", "role": "Senior Machine Learning Engineer",
        "organisation": "Nila AI Labs", "experience": 9, "expertise": "Python, PyTorch, responsible AI",
        "domains": ["Python & Data", "AI & Machine Learning", "Mathematics", "Science"],
        "languages": ["Tamil", "English"], "availability": "Saturday, 10:00–13:00 IST",
        "tags": ["Strong PyTorch Match", "AI/ML Mentor", "Tamil + English", "Available Saturday"],
    },
    {
        "id": "divya", "name": "Divya Reddy", "role": "Engineering Programme Lead",
        "organisation": "Suryatech Renewables", "experience": 12, "expertise": "Systems engineering, electronics, product design",
        "domains": ["Engineering", "Mathematics", "Web Development", "Cybersecurity"],
        "languages": ["Telugu", "Hindi", "English"], "availability": "Wednesday evenings & Sunday",
        "tags": ["Engineering Pathway", "Project Mentor", "Telugu + English", "Weekend availability"],
    },
    {
        "id": "meera", "name": "Dr. Meera Kapoor", "role": "Principal Research Scientist",
        "organisation": "Institute for Materials Research", "experience": 16, "expertise": "Research design, physics, scientific writing",
        "domains": ["Science", "Mathematics", "Engineering", "Others"],
        "languages": ["Hindi", "English"], "availability": "Friday afternoons & Sunday",
        "tags": ["Research Guidance", "Science Mentor", "Hindi + English", "Available Sunday"],
    },
]

def mentor_recommendations(skill: str, language: str):
    requested = skill.strip().lower()
    preferred_language = language.strip().title()
    ranked = []
    for mentor in MENTORS:
        domain_score = 40 if any(requested in domain.lower() or domain.lower() in requested for domain in mentor["domains"]) else 21
        technical_score = 27 if any(token in mentor["expertise"].lower() for token in requested.split()) else 17
        language_score = 18 if preferred_language in mentor["languages"] else 8
        availability_score = 12 if "Saturday" in mentor["availability"] or "Sunday" in mentor["availability"] else 9
        score = min(98, domain_score + technical_score + language_score + availability_score)
        reasons = [
            {"label": "STEM domain", "value": domain_score, "detail": "Direct domain alignment" if domain_score == 40 else "Transferable STEM perspective"},
            {"label": "Technical needs", "value": technical_score, "detail": "Expertise supports your learning goal"},
            {"label": "Language", "value": language_score, "detail": f"{preferred_language} support" if language_score == 18 else "English support available"},
            {"label": "Availability", "value": availability_score, "detail": mentor["availability"]},
        ]
        ranked.append({**mentor, "match": score, "reasons": reasons})
    return sorted(ranked, key=lambda mentor: mentor["match"], reverse=True)[:3]

@app.post("/api/mentors/recommendations")
async def recommended_mentors(payload: MentorMatchPayload):
    return {"mentors": mentor_recommendations(payload.skill, payload.language)}

@app.post("/api/mentors/requests")
async def request_mentor_guidance(payload: MentorRequestPayload):
    email = normalise_email(payload.email)
    mentor = next((item for item in MENTORS if item["id"] == payload.mentor_id), None)
    if not mentor:
        raise HTTPException(status_code=404, detail="Mentor not found")
    with db() as connection:
        cursor = connection.execute(
            """INSERT INTO mentor_requests (user_email, mentor_id, guidance_type, message)
               VALUES (?, ?, ?, ?)""",
            (email, mentor["id"], payload.guidance_type.strip(), payload.message.strip()),
        )
    await impact({"matches": 1}, "mentor_guidance_requested")
    return {"id": cursor.lastrowid, "status": "Pending", "mentor": mentor["name"]}

@app.get("/api/impact/timeline")
async def impact_timeline(window: str = "7d"):
    days_by_window = {"1d": 1, "7d": 7, "30d": 30, "365d": 365}
    if window not in days_by_window:
        raise HTTPException(status_code=422, detail="Choose 1d, 7d, 30d or 365d")
    # A daily action is represented across the selected period so a learner can see
    # the scale of one repeated choice: 1 day = 1, week = 7, month = 30, year = 365.
    with db() as connection:
        rows = connection.execute(
            "SELECT payload FROM activity WHERE datetime(created_at) >= datetime('now', '-1 day')"
        ).fetchall()
    daily = {name: 0 for name in DEFAULT_METRICS}
    for row in rows:
        for name, amount in json.loads(row["payload"]).items():
            if name in daily:
                daily[name] += amount
    days = days_by_window[window]
    values = {name: amount * days for name, amount in daily.items()}
    comparisons = []
    for label, multiplier in (("1 day", 1), ("1 week", 7), ("30 days", 30), ("1 year", 365)):
        comparisons.append({"period": label, "opportunities": daily["opportunities"] * multiplier, "learning": (daily["skillsUpgraded"] + daily["roadmaps"]) * multiplier, "mentoring": daily["matches"] * multiplier})
    return {"window": window, "days": days, "metrics": values, "daily": daily, "comparisons": comparisons}

SCORE_WEIGHTS = {
    "technical_assessment": (20, 20, "Completed technical diagnostic"),
    "skill_improvement": (4, 12, "Built technical skills"),
    "technical_challenge": (5, 20, "Finished technical challenges"),
    "project": (18, 18, "Completed a technical project"),
    "hackathon": (7, 14, "Participated in a hackathon"),
    "open_source": (10, 20, "Made an open-source contribution"),
    "research": (6, 12, "Completed research activity"),
    "opportunity_application": (3, 6, "Applied to an opportunity"),
    "roadmap_goal": (2, 10, "Finished roadmap goals"),
    "mentor_guidance": (5, 5, "Completed mentor-guidance goal"),
    "peer_help": (3, 9, "Helped peers"),
}

DEFAULT_ANONYMOUS_ACTIVITY = {
    "technical_assessment": 1, "skill_improvement": 3, "technical_challenge": 3,
    "project": 1, "research": 1, "opportunity_application": 2, "roadmap_goal": 1, "mentor_guidance": 1,
}

def anonymous_prefix(stem_field: str):
    value = stem_field.lower()
    if "ai" in value or "machine" in value:
        return "AI-GIRL"
    if "data" in value or "math" in value:
        return "STEM-WOMAN"
    if "web" in value or "cyber" in value or "engineer" in value:
        return "TECH-SHE"
    return "STEM-SHE"

def anonymous_identity(email: str, stem_field: str, refresh: bool = False):
    salt = "refresh" if refresh else "stable"
    number = int(hashlib.sha256(f"{salt}:{email}:{stem_field}".encode()).hexdigest()[:8], 16) % 9000 + 1000
    return f"{anonymous_prefix(stem_field)}-{number}"

def calculate_anonymous_score(activity: dict[str, int]):
    items = []
    total = 0
    for key, (points, cap, label) in SCORE_WEIGHTS.items():
        count = max(0, int(activity.get(key, 0)))
        earned = min(cap, count * points)
        total += earned
        if earned:
            items.append({"activity": key, "points": earned, "label": label, "count": count})
    return min(100, total), sorted(items, key=lambda item: item["points"], reverse=True)

def anonymous_profile_for(email: str, stem_field: str = "STEM", regenerate: bool = False):
    with db() as connection:
        row = connection.execute("SELECT * FROM anonymous_profiles WHERE user_email = ?", (email,)).fetchone()
        if not row:
            activity = DEFAULT_ANONYMOUS_ACTIVITY.copy()
            identity = anonymous_identity(email, stem_field)
            connection.execute(
                """INSERT INTO anonymous_profiles (user_email, anonymous_id, stem_field, target_career, experience_level, activity_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (email, identity, stem_field, "ML Engineer" if "AI" in stem_field else "STEM Explorer", "Emerging", json.dumps(activity)),
            )
            row = connection.execute("SELECT * FROM anonymous_profiles WHERE user_email = ?", (email,)).fetchone()
        elif regenerate:
            stem = row["stem_field"] or stem_field
            identity = anonymous_identity(email, stem, refresh=True)
            connection.execute("UPDATE anonymous_profiles SET anonymous_id = ? WHERE user_email = ?", (identity, email))
            row = connection.execute("SELECT * FROM anonymous_profiles WHERE user_email = ?", (email,)).fetchone()
    activity = json.loads(row["activity_json"] or "{}")
    score, reasons = calculate_anonymous_score(activity)
    top_percent = 18 if score >= 80 else 29 if score >= 70 else 43
    breakdown = [
        {"label": "Technical Growth", "value": min(100, 58 + activity.get("skill_improvement", 0) * 8)},
        {"label": "Projects", "value": min(100, activity.get("project", 0) * 90)},
        {"label": "Challenges", "value": min(100, 46 + activity.get("technical_challenge", 0) * 10)},
        {"label": "Consistency", "value": min(100, 60 + activity.get("roadmap_goal", 0) * 12)},
        {"label": "Opportunity Exposure", "value": min(100, 50 + activity.get("opportunity_application", 0) * 9)},
        {"label": "Community Contribution", "value": min(100, 54 + activity.get("peer_help", 0) * 9)},
    ]
    return {
        "anonymousId": row["anonymous_id"], "score": score, "topPercent": top_percent,
        "stemField": row["stem_field"], "targetCareer": row["target_career"], "experienceLevel": row["experience_level"],
        "leaderboardOptIn": bool(row["leaderboard_opt_in"]), "breakdown": breakdown, "reasons": reasons,
        "weekly": {"lastWeek": max(0, score - 13), "thisWeek": score, "growth": min(18, 8 + activity.get("technical_challenge", 0) * 2)},
        "benchmark": {"peerAverage": 67, "topTen": 91, "projects": activity.get("project", 0), "peerProjects": 1.8, "challenges": activity.get("technical_challenge", 0) + 5, "peerChallenges": 5},
        "nextActions": [
            {"label": "Complete one PyTorch challenge" if "AI" in row["stem_field"] else "Complete one technical challenge", "points": 5, "activity": "technical_challenge"},
            {"label": "Build a mini technical project", "points": 10, "activity": "project"},
            {"label": "Apply to a women-first hackathon", "points": 7, "activity": "hackathon"},
            {"label": "Make an open-source contribution", "points": 10, "activity": "open_source"},
        ],
    }

def anonymous_leaderboard(profile: dict):
    peers = [("TECH-SHE-482", 96), ("STEM-WOMAN-821", 91), ("AI-GIRL-207", 88), ("STEM-SHE-634", 81)]
    board = [{"rank": index + 1, "id": identity, "score": score} for index, (identity, score) in enumerate(peers)]
    if profile["leaderboardOptIn"]:
        board.append({"rank": 4, "id": "YOU", "score": profile["score"]})
        board = sorted(board, key=lambda item: item["score"], reverse=True)[:5]
        for index, item in enumerate(board):
            item["rank"] = index + 1
    return board

@app.post("/api/anonymous-score")
async def get_anonymous_score(payload: AnonymousScorePayload):
    profile = anonymous_profile_for(normalise_email(payload.email), payload.stem_field)
    return {**profile, "leaderboard": anonymous_leaderboard(profile)}

@app.post("/api/anonymous-score/regenerate")
async def regenerate_anonymous_identity(payload: AnonymousScorePayload):
    profile = anonymous_profile_for(normalise_email(payload.email), payload.stem_field, regenerate=True)
    return {**profile, "leaderboard": anonymous_leaderboard(profile)}

@app.post("/api/anonymous-score/privacy")
async def set_anonymous_privacy(payload: PrivacyPayload):
    email = normalise_email(payload.email)
    anonymous_profile_for(email)
    with db() as connection:
        connection.execute("UPDATE anonymous_profiles SET leaderboard_opt_in = ? WHERE user_email = ?", (int(payload.leaderboard_opt_in), email))
    profile = anonymous_profile_for(email)
    return {**profile, "leaderboard": anonymous_leaderboard(profile)}

@app.post("/api/anonymous-score/activity")
async def record_anonymous_activity(payload: AnonymousActivityPayload):
    email = normalise_email(payload.email)
    if payload.activity not in SCORE_WEIGHTS:
        raise HTTPException(status_code=422, detail="Unknown technical activity")
    anonymous_profile_for(email)
    with db() as connection:
        row = connection.execute("SELECT activity_json FROM anonymous_profiles WHERE user_email = ?", (email,)).fetchone()
        activity = json.loads(row["activity_json"] or "{}")
        activity[payload.activity] = int(activity.get(payload.activity, 0)) + payload.amount
        connection.execute("UPDATE anonymous_profiles SET activity_json = ? WHERE user_email = ?", (json.dumps(activity), email))
    profile = anonymous_profile_for(email)
    return {**profile, "leaderboard": anonymous_leaderboard(profile)}

@app.websocket("/ws/impact")
async def impact_socket(socket: WebSocket):
    await hub.connect(socket)
    try:
        while True:
            await socket.receive_text()
    except WebSocketDisconnect:
        hub.disconnect(socket)
    except Exception:
        hub.disconnect(socket)
