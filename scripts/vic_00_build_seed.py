import json, re
from pathlib import Path

BASE_URL = "https://www.legislation.vic.gov.au"
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
SEED_PATH = DATA_DIR / "vic_seed_urls.json"

def title_to_slug(t):
    s = t.lower()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    return re.sub(r"-+", "-", s)

ACTS = [
    "Victoria Police Act 2013","Bail Act 1977","Crimes Act 1958",
    "Criminal Procedure Act 2009","Sentencing Act 1991",
    "Planning and Environment Act 1987","Environment Protection Act 2017",
    "Road Safety Act 1986","Residential Tenancies Act 1997",
    "Equal Opportunity Act 2010","Charter of Human Rights and Responsibilities Act 2006",
    "Family Violence Protection Act 2008","Children Youth and Families Act 2005",
    "Public Health and Wellbeing Act 2008","Education and Training Reform Act 2006",
    "Water Act 1989","Local Government Act 2020","Evidence Act 2008",
    "Occupational Health and Safety Act 2004","Land Tax Act 2005",
    "Mental Health and Wellbeing Act 2022","Working with Children Act 2005",
    "Disability Act 2006","Privacy and Data Protection Act 2014",
    "Health Records Act 2001","Building Act 1993","Electoral Act 2002",
    "Interpretation of Legislation Act 1984","Supreme Court Act 1986",
    "Magistrates Court Act 1989","County Court Act 1958",
    "Victorian Civil and Administrative Tribunal Act 1998",
    "Emergency Management Act 2013","Gambling Regulation Act 2003",
    "Liquor Control Reform Act 1998","Transport Integration Act 2010",
    "Public Administration Act 2004","Corrections Act 1986",
    "Sex Offenders Registration Act 2004","Serious Offenders Act 2018",
    "Confiscation Act 1997","Control of Weapons Act 1990",
    "Drugs Poisons and Controlled Substances Act 1981",
    "Summary Offences Act 1966","Infringements Act 2006",
    "Independent Broad-based Anti-corruption Commission Act 2011",
    "Audit Act 1994","Aboriginal Heritage Act 2006",
    "Owners Corporations Act 2006","Sale of Land Act 1962",
    "Australian Consumer Law and Fair Trading Act 2012",
    "Wildlife Act 1975","Country Fire Authority Act 1958",
    "Workplace Injury Rehabilitation and Compensation Act 2013",
]

REGS = [
    "Bail Regulations 2022","Building Regulations 2018",
    "Occupational Health and Safety Regulations 2017",
    "Road Safety (Vehicles) Regulations 2009",
    "Residential Tenancies Regulations 2021",
    "Environment Protection Regulations 2021",
    "Planning and Environment Regulations 2015",
    "Gambling Regulations 2023","Liquor Control Reform Regulations 2009",
    "Criminal Procedure Regulations 2017",
    "Victorian Civil and Administrative Tribunal Rules 2018",
    "Supreme Court (General Civil Procedure) Rules 2015",
    "Magistrates Court (Fees) Regulations 2022",
    "Children Youth and Families Regulations 2017",
    "Public Health and Wellbeing Regulations 2019",
    "Corrections Regulations 2019","Control of Weapons Regulations 2021",
    "Drugs Poisons and Controlled Substances Regulations 2017",
    "Electoral Regulations 2021","Local Government (Electoral) Regulations 2020",
]

items = []
seen = set()
for t in ACTS:
    slug = title_to_slug(t)
    url = f"{BASE_URL}/in-force/acts/{slug}"
    if url not in seen:
        seen.add(url)
        items.append({"url": url, "slug": slug, "doc_type": "act", "title": t})
for t in REGS:
    slug = title_to_slug(t)
    url = f"{BASE_URL}/in-force/statutory-rules/{slug}"
    if url not in seen:
        seen.add(url)
        items.append({"url": url, "slug": slug, "doc_type": "statutory_rule", "title": t})

SEED_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2))
print(f"seed 생성 완료: {len(items)}개 → {SEED_PATH}")
