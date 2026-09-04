import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from backend.main import CareerProfile, EvidenceScanPayload, ROLE_CATALOG, run_career_pipeline, run_evidence_scan


def analyse(profile: CareerProfile, category: str = "jobs"):
    return asyncio.run(run_career_pipeline(profile, category, "test", use_ollama=False))


class CareerPipelineTests(unittest.TestCase):
    def test_synthetic_catalog_has_distinct_records_for_every_opportunity_type(self):
        expected_counts = {"jobs": 12, "internships": 8, "scholarships": 7, "funding": 7}
        self.assertEqual({category: len(records) for category, records in ROLE_CATALOG.items()}, expected_counts)
        for category, records in ROLE_CATALOG.items():
            self.assertEqual(len({record["title"] for record in records}), len(records))
            self.assertEqual(len({record["organization"] for record in records}), len(records))
            self.assertTrue(all(record["location"] and record["work_mode"] and record["salary"] for record in records))

    def test_recommendations_return_dataset_metadata_and_three_different_records(self):
        profile = CareerProfile(
            education_level="B.Tech Computer Science", target_role="data analyst",
            interests="data and research", existing_skills="Excel and SQL",
            work_preference="hybrid", digital_literacy="comfortable", confidence=4,
        )
        result = analyse(profile)
        self.assertEqual(result["dataset"]["records"], 12)
        self.assertEqual(result["dataset"]["catalogSize"], 34)
        self.assertEqual(result["dataset"]["category"], "jobs")
        self.assertEqual(len(result["recommendations"]), 3)
        self.assertEqual(len({item["organization"] for item in result["recommendations"]}), 3)
        self.assertTrue(all(item["description"] and item["location"] and item["salary"] for item in result["recommendations"]))

    def test_evidence_scan_lifts_low_self_rating_when_public_work_is_strong(self):
        connected = {
            "id": "github", "label": "GitHub", "handle": "asha", "status": "connected", "score": 88,
            "summary": "Reviewed 4 public projects.", "signals": [], "skills": ["Python", "Data analysis"],
            "highlights": [], "projects": [], "profileUrl": "https://github.com/asha", "limitation": "Public only",
        }
        with patch("backend.main._github_evidence", new=AsyncMock(return_value=connected)), \
             patch("backend.main._gitlab_evidence", new=AsyncMock(return_value={"id": "gitlab", "label": "GitLab", "handle": "", "status": "not_connected", "score": None, "summary": "Not connected", "signals": [], "skills": [], "highlights": [], "projects": [], "profileUrl": "", "limitation": "Public only"})), \
             patch("backend.main._codeforces_evidence", new=AsyncMock(return_value={"id": "codeforces", "label": "Codeforces", "handle": "", "status": "not_connected", "score": None, "summary": "Not connected", "signals": [], "skills": [], "highlights": [], "projects": [], "profileUrl": "", "limitation": "Public only"})):
            report = asyncio.run(run_evidence_scan(EvidenceScanPayload(email="asha@example.com", skill="Python & Data", answers=[1, 1, 2, 1, 2], github="asha")))
        self.assertEqual(report["selfScore"], 35)
        self.assertEqual(report["evidenceScore"], 88)
        self.assertEqual(report["confidenceScore"], 67)
        self.assertEqual(report["calibration"]["status"], "under_recognized")
        self.assertIn("Python", report["skillsDetected"])

    def test_evidence_scan_keeps_self_only_baseline_without_connected_sources(self):
        report = asyncio.run(run_evidence_scan(EvidenceScanPayload(email="quiet@example.com", skill="Science", answers=[3, 3, 3, 3, 3])))
        self.assertEqual(report["selfScore"], 60)
        self.assertIsNone(report["evidenceScore"])
        self.assertEqual(report["confidenceScore"], 60)
        self.assertEqual(report["calibration"]["status"], "self_only")

    def test_personas_receive_distinct_grounded_outputs(self):
        homemaker = CareerProfile(
            name="Asha", education_level="10th standard", work_experience="No formal work experience yet",
            life_experience="I manage household budgets, plan school schedules and coordinate community events",
            career_break="Homemaker for 12 years", interests="organising inventory and helping local shops",
            confidence=2, existing_skills="smartphone and WhatsApp", target_role="Data Analyst",
            constraints="care duties and no laptop yet", available_time="45 minutes each evening",
            digital_literacy="basic smartphone only", work_preference="part-time nearby",
            practical_goal="earn a first independent income",
        )
        returner = CareerProfile(
            name="Meera", education_level="B.Tech Computer Science", work_experience="5 years software QA and bug reporting",
            life_experience="caregiving and school volunteering", career_break="6 year career break",
            interests="software quality and reliable products", confidence=3,
            existing_skills="manual testing, Jira, Excel and test cases", target_role="QA tester returnship",
            constraints="school pickup at 3pm", available_time="8 hours per week",
            digital_literacy="comfortable with computers and office tools", work_preference="hybrid flexible",
            practical_goal="return to paid technology work",
        )
        professional = CareerProfile(
            name="Priya", education_level="MBA and B.Tech",
            work_experience="10 years as product operations manager leading cross-functional teams",
            life_experience="I mentor women at work", career_break="currently working, no break",
            interests="product strategy and leadership", confidence=5,
            existing_skills="advanced Excel, Jira, product metrics, presentations and stakeholder reporting",
            target_role="Technical Project Lead promotion", constraints="can study weekends",
            available_time="5 hours on weekends", digital_literacy="advanced digital tools",
            work_preference="hybrid", practical_goal="win a promotion to technical project lead",
        )

        results = [analyse(profile) for profile in (homemaker, returner, professional)]
        primary = [result["recommendations"][0] for result in results]

        self.assertEqual(len({item["title"] for item in primary}), 3)
        self.assertEqual(len({item["readiness"] for item in primary}), 3)
        self.assertTrue(all(len(result["pipeline"]) == 4 for result in results))
        self.assertTrue(all({"Technical match", "Flexibility match", "Education baseline"}.issubset(item["scoreBreakdown"]) for item in primary))
        self.assertEqual(primary[0]["readinessLevel"], "Refresh before applying")
        self.assertTrue(any("Education bridge" in " ".join(item["missing"]) for item in results[0]["recommendations"]))
        self.assertEqual(primary[1]["title"], "QA Testing Trainee")
        self.assertIn("promotion", results[2]["profileSummary"])

    def test_transferable_skills_never_carry_readiness_scores(self):
        profile = CareerProfile(
            education_level="10th standard", life_experience="I plan meals, manage budgets and coordinate family schedules",
            target_role="Operations Assistant", digital_literacy="beginner", confidence=2,
        )
        result = analyse(profile)
        self.assertTrue(result["transferableSkills"])
        self.assertTrue(all("score" not in skill for skill in result["transferableSkills"]))
        self.assertTrue(all("readiness" in role for role in result["recommendations"]))

    def test_retired_stem_professional_gets_experience_led_path(self):
        profile = CareerProfile(
            name="Dr Leela", education_level="PhD in Physics",
            work_experience="Retired research scientist with 28 years in materials laboratories, publications and team leadership",
            life_experience="I mentor students and manage a science club", career_break="Retired for 3 years",
            interests="research mentoring and science communication", confidence=4,
            existing_skills="research documentation, data collection, Excel, presentations and reports",
            target_role="Research Operations Coordinator or STEM mentor", constraints="no daily commute",
            available_time="12 hours per week", digital_literacy="comfortable with computers and office tools",
            work_preference="part-time remote consulting", practical_goal="contribute to research and mentor younger women",
        )
        result = analyse(profile)
        primary = result["recommendations"][0]
        self.assertIn("experienced professional", result["persona"])
        self.assertEqual(primary["title"], "Research Operations Coordinator")
        self.assertIn("Research documentation", primary["skillFreshness"]["retained"])
        self.assertEqual(primary["roadmap"][0]["stage"], "Skill refresh")

    def test_education_gate_caps_unmet_role(self):
        profile = CareerProfile(
            education_level="10th standard", life_experience="I track expenses and organise records",
            target_role="Junior Data Analyst", existing_skills="Excel", digital_literacy="comfortable with Excel",
        )
        result = analyse(profile)
        analyst = next(item for item in result["recommendations"] if item["title"] == "Junior Data Analyst")
        self.assertLessEqual(analyst["readiness"], 58)
        self.assertFalse(analyst["eligibilityDetail"]["eligible"])
        self.assertIn("Excel", analyst["steppingStone"])

    def test_each_roadmap_stage_is_actionable(self):
        profile = CareerProfile(
            education_level="Bachelor's degree", work_experience="3 years community teaching",
            career_break="2 year break", interests="STEM education", existing_skills="presentations and reports",
            target_role="STEM Program Coordinator", available_time="6 hours per week",
        )
        result = analyse(profile)
        required = {"stage", "goal", "skills", "why", "task", "milestone", "duration"}
        for stage in result["recommendations"][0]["roadmap"]:
            self.assertTrue(required.issubset(stage))

    def test_reentry_profile_keeps_experience_and_shows_the_short_path(self):
        profile = CareerProfile(
            education_level="B.Tech Computer Science", work_experience="5 years in software QA and bug reporting",
            career_break="6 year career break", target_role="QA testing returnship",
            existing_skills="manual testing, Jira, test cases and Excel", rusty_skills="web fundamentals, Agile basics",
            new_evidence="", flexibility_preferences="hybrid, flexible hours", available_time="8 hours per week",
            practical_goal="return to paid technology work",
        )
        result = asyncio.run(run_career_pipeline(profile, "jobs", "restart-and-grow", use_ollama=False))
        primary = result["recommendations"][0]
        self.assertEqual(result["engine"], "Four-agent, transparent rule-based matching")
        self.assertFalse(result["usedOllama"])
        self.assertEqual(len(result["pipeline"]), 4)
        self.assertIn("Software testing basics", primary["skillFreshness"]["retained"])
        self.assertIn("Web fundamentals", primary["skillFreshness"]["rusty"])
        self.assertEqual([stage["stage"] for stage in primary["roadmap"]], ["Skill refresh", "Project / skill evidence", "Internship / returnship", "Job"])
        self.assertEqual([step["name"] for step in result["progress"]["steps"]], ["Skills", "Projects", "Funding", "Returnship", "Applications", "Employment"])
        self.assertEqual(result["returnships"][0]["title"], "Software QA Returnship")


if __name__ == "__main__":
    unittest.main()
