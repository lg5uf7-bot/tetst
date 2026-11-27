"""v2.5 Hybrid ML + Rules Classifier for startups & jobs (with full description + short_summary)."""

from __future__ import annotations

import hashlib
import re
from flashtext import KeywordProcessor
from sklearn.calibration import CalibratedClassifierCV
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import FeatureUnion
from sklearn.svm import LinearSVC

INDUSTRY_MAP = {
    "b2b": "EnterpriseSaaS", "enterprise": "EnterpriseSaaS", "saas": "EnterpriseSaaS",
    "productivity": "EnterpriseSaaS", "collaboration": "EnterpriseSaaS",
    "developer tools": "DevTools_Infra", "devtools": "DevTools_Infra", "infrastructure": "DevTools_Infra",
    "cloud": "DevTools_Infra", "open source": "DevTools_Infra", "security": "Cybersecurity",
    "cybersecurity": "Cybersecurity", "privacy": "Cybersecurity",
    "fintech": "Fintech", "payments": "Fintech", "banking": "Fintech", "insurance": "Fintech",
    "crypto": "Crypto_Web3", "web3": "Crypto_Web3", "blockchain": "Crypto_Web3",
    "healthcare": "Healthcare_Bio", "biotech": "Healthcare_Bio", "biotechnology": "Healthcare_Bio",
    "medical": "Healthcare_Bio", "health": "Healthcare_Bio", "therapeutics": "Healthcare_Bio",
    "consumer": "Consumer_Social", "social": "Consumer_Social", "gaming": "Consumer_Social",
    "ecommerce": "Ecommerce_Retail", "e-commerce": "Ecommerce_Retail", "retail": "Ecommerce_Retail", "marketplace": "Ecommerce_Retail",
    "climate": "Climate_Energy", "energy": "Climate_Energy", "cleantech": "Climate_Energy", "sustainability": "Climate_Energy",
    "robotics": "Robotics_Hardware", "hardware": "Robotics_Hardware", "semiconductors": "Robotics_Hardware", "aerospace": "Robotics_Hardware", "manufacturing": "Robotics_Hardware", "drones": "Robotics_Hardware",
    "education": "EdTech_HR", "edtech": "EdTech_HR", "recruiting": "EdTech_HR", "hr": "EdTech_HR",
    "real estate": "PropTech_Logistics", "proptech": "PropTech_Logistics", "construction": "PropTech_Logistics", "logistics": "PropTech_Logistics", "supply chain": "PropTech_Logistics",
    "artificial intelligence": "AI_ML", "machine learning": "AI_ML", "generative ai": "AI_ML", "llm": "AI_ML", "ai": "AI_ML",
}

INDUSTRY_RULES = {
    "AI_ML": re.compile(r"\b(llm|large language model|foundation model|generative ai|ai agent|copilot|inference|neural|computer vision|fine-tun|rag|vector database)\b", re.I),
    "DevTools_Infra": re.compile(r"\b(sdk|api platform|developer tool|ci/cd|kubernetes|observability|postgres|compiler|serverless|terraform|git|cloud infra)\b", re.I),
    "Cybersecurity": re.compile(r"\b(zero trust|soc 2|siem|vulnerability|threat detection|iam|pentest|encryption|infosec|cybersecurity|devsecops)\b", re.I),
    "Fintech": re.compile(r"\b(payment|neobank|treasury|underwriting|ledger|credit card|payroll|accounting|invoicing|stablecoin|fintech|lending|insurtech)\b", re.I),
    "Healthcare_Bio": re.compile(r"\b(patient|clinical|fda|biotech|therapeutics|genomic|ehr|diagnostic|telehealth|pharma|protein|drug discovery)\b", re.I),
    "Robotics_Hardware": re.compile(r"\b(robot|autonomous|lidar|drone|pcb|asic|chip|semiconductor|firmware|actuator|manufacturing|humanoid)\b", re.I),
    "Climate_Energy": re.compile(r"\b(solar|battery|grid|carbon|ev charging|geothermal|hydrogen|fusion|renewable|climate|emissions)\b", re.I),
    "Ecommerce_Retail": re.compile(r"\b(shopify|storefront|checkout|merchant|dtc|e-commerce|retail|inventory|fulfillment)\b", re.I),
    "PropTech_Logistics": re.compile(r"\b(freight|trucking|warehouse|shipment|real estate|mortgage|tenant|property|construction|fleet)\b", re.I),
    "EdTech_HR": re.compile(r"\b(tutoring|curriculum|classroom|student|lms|hiring|recruiting|ats|onboarding|workforce|upskilling)\b", re.I),
    "Crypto_Web3": re.compile(r"\b(ethereum|solana|defi|smart contract|on-chain|zk-proof|wallet|blockchain|web3|protocol)\b", re.I),
    "Consumer_Social": re.compile(r"\b(creator|podcast|music|dating|messaging|social network|mobile game|consumer app|short video)\b", re.I),
    "EnterpriseSaaS": re.compile(r"\b(crm|erp|workflow automation|b2b saas|sales enablement|legaltech|contract management|compliance|procurement|revops)\b", re.I),
}

BM_RULES = [
    ("DeveloperPlatform", re.compile(r"\b(api|sdk|developer platform|open[- ]source|cloud infra|self-hosted|cli)\b", re.I)),
    ("Marketplace", re.compile(r"\b(marketplace|two-sided|buyers and sellers|connects .* with|matching platform)\b", re.I)),
    ("Fintech_Financial", re.compile(r"\b(neobank|payments|lending|credit|card issuing|treasury|insurance|underwriting)\b", re.I)),
    ("Hardware_DeepTech", re.compile(r"\b(hardware|robot|chip|silicon|device|satellite|drone|sensor|manufacturing)\b", re.I)),
    ("Bio_Pharma", re.compile(r"\b(therapeutics|clinical trial|drug discovery|molecule|biotech|assay)\b", re.I)),
    ("Ecommerce_DTC", re.compile(r"\b(direct-to-consumer|dtc|shop|apparel|skincare|consumer brand)\b", re.I)),
    ("SaaS", re.compile(r"\b(saas|software|platform|copilot|agent|dashboard|automation|workflow)\b", re.I)),
]

ROLE_RULES = [
    ("ml_ai", re.compile(r"\b(machine learning|deep learning|llm|nlp|computer vision|ai engineer|research scientist|applied scientist|ml engineer|ai researcher|generative ai|forward deployed.*ai|data scientist)\b", re.I)),
    ("security", re.compile(r"\b(security|appsec|infosec|soc analyst|penetration|threat|devsecops|cloud security|product security|trust & safety)\b", re.I)),
    ("data", re.compile(r"\b(data engineer|analytics engineer|business intelligence|\bbi\b|etl|data warehouse|data analyst|data platform)\b", re.I)),
    ("infrastructure", re.compile(r"\b(devops|sre|site reliability|platform engineer|cloud engineer|kubernetes|infrastructure engineer|systems engineer|distributed systems|release engineer|build engineer|network engineer|database engineer|reliability)\b", re.I)),
    ("design", re.compile(r"\b(product designer|ux|ui/ux|design system|brand designer|visual designer|graphic designer|creative director|motion designer|interaction designer|ux researcher)\b", re.I)),
    ("product", re.compile(r"\b(product manager|product lead|head of product|vp of product|technical product|group pm|product owner|product operations)\b", re.I)),
    ("sales", re.compile(r"\b(account executive|\bae\b|\bsdr\b|\bbdr\b|sales development|business development|revenue operations|revops|solutions engineer|sales engineer|solutions architect|enterprise sales|head of sales|vp sales|account manager|customer success|partnerships|alliances|field sales|strategic accounts)\b", re.I)),
    ("marketing", re.compile(r"\b(marketing|growth lead|growth manager|content lead|developer relations|devrel|developer advocate|seo|demand gen|product marketing|pmm|communications|public relations|brand manager|social media|community manager)\b", re.I)),
    ("engineering", re.compile(r"\b(software engineer|full[- ]stack|frontend|front[- ]end|backend|back[- ]end|mobile engineer|ios engineer|android engineer|firmware|embedded|robotics engineer|hardware engineer|electrical engineer|mechanical engineer|qa engineer|test engineer|sdet|developer|founding engineer|staff engineer|principal engineer|engineering manager|vp engineering|cto|solutions developer|web developer|forward deployed engineer)\b", re.I)),
    ("operations", re.compile(r"\b(operations|chief of staff|recruiter|talent acquisition|people partner|hr |human resources|finance|controller|accountant|fp&a|legal|counsel|paralegal|compliance|supply chain|logistics|procurement|office manager|executive assistant|program manager|project manager|support specialist|customer support)\b", re.I)),
]

SKILLS_CASE_INSENSITIVE = [
    "Python", "TypeScript", "JavaScript", "Java", "Kotlin", "Swift", "Scala", "Ruby", "PHP", "SQL",
    "React", "Next.js", "Vue", "Angular", "Node.js", "FastAPI", "Django", "Flask", "Spring Boot",
    "PostgreSQL", "MySQL", "MongoDB", "Redis", "ClickHouse", "Snowflake", "BigQuery", "DuckDB", "Databricks", "dbt",
    "Kafka", "Airflow", "Kubernetes", "Docker", "Terraform", "AWS", "GCP", "Azure", "Cloudflare", "Linux",
    "PyTorch", "TensorFlow", "JAX", "CUDA", "vLLM", "LangChain", "Hugging Face", "RAG", "LLM", "NLP", "Computer Vision",
    "GraphQL", "gRPC", "REST API", "Microservices", "CI/CD", "GitHub Actions", "Datadog", "Prometheus", "Grafana",
    "Figma", "Salesforce", "HubSpot", "Stripe", "ROS2", "SolidWorks", "Verilog", "Embedded Linux",
]
SKILLS_CASE_SENSITIVE = [
    "Go", "R", "C#", "C++", "Rust", "Clay", "Spark", "Gong", "Tableau", "Looker",
]


def _build_skill_extractors():
    kp_ci = KeywordProcessor(case_sensitive=False)
    for sk in SKILLS_CASE_INSENSITIVE:
        kp_ci.add_keyword(sk, sk)
    for alias, canon in [
        ("Golang", "Go"), ("Postgres", "PostgreSQL"), ("K8s", "Kubernetes"),
        ("TS", "TypeScript"), ("NextJS", "Next.js"), ("ReactJS", "React"),
        ("NodeJS", "Node.js"), ("Amazon Web Services", "AWS"), ("Google Cloud", "GCP"),
        ("Large Language Models", "LLM"),
    ]:
        kp_ci.add_keyword(alias, canon)
    kp_cs = KeywordProcessor(case_sensitive=True)
    for sk in SKILLS_CASE_SENSITIVE:
        kp_cs.add_keyword(sk, sk)
    return kp_ci, kp_cs


KP_CI, KP_CS = _build_skill_extractors()

SALARY_TEXT_RE = re.compile(
    r"(\$|USD|EUR|GBP|CAD)\s?(\d{2,3}(?:,\d{3})|\d{2,3}k)\s*(?:[-–—to]{1,3}\s*(?:\$|USD|EUR|GBP|CAD)?\s?(\d{2,3}(?:,\d{3})|\d{2,3}k))?\s*(?:/\s*(?:yr|year|annual|hr|hour)|per\s+(?:year|annum|hour))?",
    re.I,
)
EQUITY_RE = re.compile(r"\b(equity|stock options?|rsu|ownership stake|meaningful equity|competitive equity)\b", re.I)


def _parse_money(tok: str | None) -> int | None:
    if not tok:
        return None
    t = tok.lower().replace(",", "").strip()
    mult = 1000 if t.endswith("k") else 1
    try:
        v = int(float(t.rstrip("k")) * mult)
        return v if v > 0 else None
    except Exception:
        return None


def extract_compensation(raw_comp, desc: str) -> dict:
    smin = smax = None
    cur = interval = None
    if isinstance(raw_comp, dict):
        if "min" in raw_comp or "max" in raw_comp:
            smin = raw_comp.get("min")
            smax = raw_comp.get("max")
            cur = raw_comp.get("currency") or "USD"
            interval = raw_comp.get("interval") or "year"
        for tier in (raw_comp.get("compensationTiers") or raw_comp.get("summaryComponents") or []):
            if isinstance(tier, dict) and str(tier.get("compensationType", "")).lower() in ("salary", "base", ""):
                lo = tier.get("minValue")
                hi = tier.get("maxValue")
                if lo or hi:
                    smin, smax = lo, hi
                    cur = tier.get("currencyCode") or "USD"
                    interval = (tier.get("interval") or "YEAR").lower()
                    break
    elif isinstance(raw_comp, list):
        for item in raw_comp:
            if isinstance(item, dict) and (item.get("min_cents") or item.get("max_cents")):
                smin = int(item["min_cents"] // 100) if item.get("min_cents") else None
                smax = int(item["max_cents"] // 100) if item.get("max_cents") else None
                cur = item.get("currency_type") or "USD"
                interval = "year"
                break
    if smin is None and desc:
        for m in SALARY_TEXT_RE.finditer(desc):
            lo = _parse_money(m.group(2))
            hi = _parse_money(m.group(3)) or lo
            if lo and ((20 <= lo <= 350 and "hour" in m.group(0).lower()) or (30000 <= lo <= 900000)):
                smin, smax = lo, hi
                sym = m.group(1).upper()
                cur = {"$" : "USD", "USD": "USD", "EUR": "EUR", "GBP": "GBP", "CAD": "CAD"}.get(sym, "USD")
                interval = "hour" if lo < 1000 else "year"
                break
    if isinstance(smin, (int, float)): smin = int(smin)
    if isinstance(smax, (int, float)): smax = int(smax)
    if smin and smin < 1000 and interval != "hour":
        smin = smax = None
    return {
        "salary_min": smin, "salary_max": smax,
        "salary_currency": cur, "salary_interval": interval,
        "offers_equity": bool(EQUITY_RE.search(desc or "")),
    }


def extract_skills(title: str, desc: str, company_name: str = "") -> list[str]:
    text = f"{title} {desc}"
    seen, out = set(), []
    cname_low = (company_name or "").strip().lower()
    for sk in KP_CI.extract_keywords(text) + KP_CS.extract_keywords(text):
        if cname_low and sk.lower() == cname_low:
            continue
        if sk not in seen:
            seen.add(sk)
            out.append(sk)
    return out[:18]


def classify_workplace_strict(ats_wp: str, loc: str, title: str, desc: str) -> str:
    if ats_wp in ("remote", "hybrid", "onsite"):
        return ats_wp
    tl = f"{title} | {loc}".lower()
    if re.search(r"\b(hybrid|days?\s+(?:a|per)\s+week\s+in[- ]office)\b", tl):
        return "hybrid"
    if re.search(r"\b(remote|work from anywhere|distributed)\b", tl):
        return "remote"
    if re.search(r"\b(on-?site|in-?person|in-?office)\b", tl):
        return "onsite"
    d_low = (desc or "")[:2000].lower()
    if re.search(r"\b(hybrid work|hybrid schedule|\d\s+days\s+(?:per|a)\s+week\s+in\s+(?:the\s+)?office)\b", d_low):
        return "hybrid"
    if re.search(r"\b(fully remote|100% remote|remote-first|remote position|remote role)\b", d_low):
        return "remote"
    if re.search(r"\b(on-?site in|in-?office role|this role is based in our .* office)\b", d_low):
        return "onsite"
    return "unknown"


def classify_exp_code(title: str, desc: str) -> str:
    tl = (title or "").lower()
    if re.search(r"\b(intern|internship|co-op|apprentice)\b", tl): return "intern"
    if re.search(r"\b(vp|vice president|chief |cto|cpo|cfo|cro|ciso|head of|director)\b", tl): return "executive"
    if re.search(r"\b(principal|staff|distinguished|fellow)\b", tl): return "staff"
    if re.search(r"\b(lead|manager|team lead|tech lead)\b", tl): return "lead"
    if re.search(r"\b(senior|sr\.?|iii|iv)\b", tl): return "senior"
    if re.search(r"\b(junior|jr\.?|entry[- ]level|new grad|associate)\b", tl): return "entry"
    d = (desc or "")[:2500].lower()
    m = re.search(r"\b(\d{1,2})\+?\s*(?:-\s*\d{1,2}\s*)?years?\s+of\s+(?:relevant\s+|industry\s+|software\s+|professional\s+)?experience\b", d)
    if m:
        yrs = int(m.group(1))
        if yrs >= 8: return "staff"
        if yrs >= 5: return "senior"
        if yrs >= 2: return "mid"
        return "entry"
    if re.search(r"\b(mid[- ]level|ii)\b", tl): return "mid"
    return "unknown"


def make_short_summary(title: str, dept: str, wp: str, loc: str, skills: list[str], comp: dict) -> str:
    parts = [title]
    if dept: parts.append(dept)
    if wp != "unknown": parts.append(wp.title())
    if loc: parts.append(loc)
    if comp.get("salary_min"):
        s_hi = comp.get("salary_max") or comp["salary_min"]
        parts.append(f"${comp['salary_min']:,}-${s_hi:,}/{comp.get('salary_interval') or 'yr'}")
    if skills:
        parts.append("Skills: " + ", ".join(skills[:6]))
    return " · ".join(parts)[:320]


class HybridClassifier:
    def __init__(self):
        self.ind_vec = FeatureUnion([
            ("word", TfidfVectorizer(ngram_range=(1, 2), max_features=12000, sublinear_tf=True)),
            ("char", TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), max_features=8000, sublinear_tf=True)),
        ])
        self.ind_clf = CalibratedClassifierCV(LinearSVC(C=0.6, class_weight="balanced"), cv=3)
        self.ct_vec = TfidfVectorizer(ngram_range=(1, 2), max_features=6000, sublinear_tf=True)
        self.ct_clf = CalibratedClassifierCV(LinearSVC(C=0.5, class_weight="balanced"), cv=3)
        self.role_vec = TfidfVectorizer(ngram_range=(1, 2), max_features=6000, sublinear_tf=True)
        self.role_clf = LogisticRegression(max_iter=1000, class_weight="balanced")
        self.ind_ready = self.ct_ready = self.role_ready = False

    def _leak_free_text(self, c: dict) -> str:
        return f"{c.get('one_liner', '')} {c.get('description', '')}"[:2000]

    def fit_companies(self, companies: list[dict]):
        x_ind, y_ind, x_ct, y_ct = [], [], [], []
        for c in companies:
            txt = self._leak_free_text(c).strip()
            if len(txt) < 12:
                continue
            raw_tokens = [c.get("industry_raw", ""), c.get("subindustry_raw", "")] + list(c.get("tags") or [])
            ind_label = None
            for tok in raw_tokens:
                tl = (tok or "").lower().strip()
                if tl in INDUSTRY_MAP:
                    ind_label = INDUSTRY_MAP[tl]
                    break
            if ind_label:
                x_ind.append(txt); y_ind.append(ind_label)
            ct_raw = (c.get("customer_type_raw") or "").upper()
            if ct_raw in ("B2B", "B2C", "B2B2C"):
                x_ct.append(txt); y_ct.append(ct_raw)
        if len(set(y_ind)) >= 3 and len(x_ind) >= 60:
            self.ind_clf.fit(self.ind_vec.fit_transform(x_ind), y_ind)
            self.ind_ready = True
        if len(set(y_ct)) >= 2 and len(x_ct) >= 40:
            self.ct_clf.fit(self.ct_vec.fit_transform(x_ct), y_ct)
            self.ct_ready = True

    def fit_jobs(self, jobs: list[dict]):
        xr, yr = [], []
        for j in jobs:
            rf = self.classify_role_rule_only(j.get("title", ""), j.get("department", ""), j.get("team", ""))
            if rf != "other":
                xr.append(f"{j.get('title', '')} {j.get('department', '')} {(j.get('description_text', ''))[:400]}")
                yr.append(rf)
        if len(set(yr)) >= 3 and len(xr) >= 50:
            self.role_clf.fit(self.role_vec.fit_transform(xr), yr)
            self.role_ready = True

    def classify_company(self, c: dict) -> dict:
        txt = f"{c.get('name', '')} {c.get('one_liner', '')} {c.get('description', '')}"
        leak_txt = self._leak_free_text(c)
        tags_raw = " ".join([c.get("industry_raw", ""), c.get("subindustry_raw", "")] + list(c.get("tags") or []))
        ind_tags = []
        for tok in [c.get("industry_raw", ""), c.get("subindustry_raw", "")] + list(c.get("tags") or []):
            mapped = INDUSTRY_MAP.get((tok or "").lower().strip())
            if mapped and mapped not in ind_tags:
                ind_tags.append(mapped)
        for ind, pat in INDUSTRY_RULES.items():
            if pat.search(txt) and ind not in ind_tags:
                ind_tags.append(ind)

        ind_primary, ind_conf = "EnterpriseSaaS", 0.55
        if self.ind_ready and len(leak_txt.strip()) >= 12:
            probs = self.ind_clf.predict_proba(self.ind_vec.transform([leak_txt]))[0]
            classes = list(self.ind_clf.classes_)
            best_i = int(probs.argmax())
            ind_primary, ind_conf = classes[best_i], float(probs[best_i])
            if ind_tags and ind_conf < 0.50:
                ind_primary, ind_conf = ind_tags[0], 0.72
        elif ind_tags:
            ind_primary, ind_conf = ind_tags[0], 0.80
        if ind_primary not in ind_tags:
            ind_tags.insert(0, ind_primary)

        ct_raw = (c.get("customer_type_raw") or "").upper()
        if ct_raw in ("B2B", "B2C", "B2B2C"):
            ct, ct_conf = ct_raw, 0.96
        elif self.ct_ready and len(leak_txt.strip()) >= 12:
            probs = self.ct_clf.predict_proba(self.ct_vec.transform([leak_txt]))[0]
            best_i = int(probs.argmax())
            ct, ct_conf = str(self.ct_clf.classes_[best_i]), float(probs[best_i])
        else:
            ct = "B2C" if re.search(r"\b(consumer|patient|student|shopper|creator|family)\b", txt, re.I) else "B2B"
            ct_conf = 0.65

        bm = "SaaS"
        for label, pat in BM_RULES:
            if pat.search(f"{txt} {tags_raw}"):
                bm = label
                break
        ai_native = bool(re.search(r"\b(ai|llm|machine learning|generative|neural|autonomous agent|copilot)\b", f"{txt} {tags_raw}", re.I))
        return {
            "industry_primary": ind_primary,
            "industry_tags": ind_tags[:4],
            "industry_confidence": round(ind_conf, 3),
            "customer_type": ct,
            "customer_confidence": round(ct_conf, 3),
            "business_model": bm,
            "ai_native": ai_native,
            "classification_source": "code_ml_v2",
        }

    def classify_role_rule_only(self, title: str, dept: str = "", team: str = "") -> str:
        t_only = (title or "").strip()
        for role, pat in ROLE_RULES:
            if pat.search(t_only):
                return role
        combo = f"{title} {dept} {team}".strip()
        for role, pat in ROLE_RULES:
            if pat.search(combo):
                return role
        return "other"

    def enrich_job(self, j: dict, company_name: str = "") -> dict:
        title = j.get("title") or ""
        dept = j.get("department") or ""
        team = j.get("team") or ""
        desc = j.get("description_text") or ""
        loc = j.get("location") or ""

        role = self.classify_role_rule_only(title, dept, team)
        if role == "other" and self.role_ready:
            probs = self.role_clf.predict_proba(self.role_vec.transform([f"{title} {dept} {desc[:400]}"]))[0]
            if float(probs.max()) >= 0.40:
                role = str(self.role_clf.classes_[int(probs.argmax())])

        wp = classify_workplace_strict(j.get("workplace_type", "unknown"), loc, title, desc)
        exp = classify_exp_code(title, desc)
        skills = extract_skills(title, desc, company_name=company_name)
        comp = extract_compensation(j.get("raw_comp"), desc)
        short_summary = make_short_summary(title, dept, wp, loc, skills, comp)
        content_hash = hashlib.sha256(
            f"{title}|{loc}|{wp}|{comp.get('salary_min')}|{comp.get('salary_max')}|{desc[:6000]}".encode("utf-8")
        ).hexdigest()[:24]

        return {
            "role_family": role,
            "workplace_type": wp,
            "experience_level": exp,
            "skills": skills,
            **comp,
            "short_summary": short_summary,
            "description_text": desc,
            "content_hash": content_hash,
        }
