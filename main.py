from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
import json
from datetime import datetime
from pathlib import Path
import re

app = FastAPI(title="Infervia API", version="3.1 - Multi-Scraper")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================================================================
# PYDANTIC MODELS
# ==============================================================================

class WhoThisAffects(BaseModel):
    healthcare_roles: List[str]
    facility_types: List[str]
    departments: List[str]

class NextStep(BaseModel):
    action: str
    timeline: str
    owner: str
    priority: Optional[str] = "medium"

class FinancialImpact(BaseModel):
    estimated_cost: Optional[str] = None
    revenue_impact: Optional[str] = None
    areas_affected: List[str] = []

class SimplifiedAnalysis(BaseModel):
    detailed_summary: str
    who_this_affects: WhoThisAffects
    next_steps: List[NextStep]
    key_requirements: Optional[List[str]] = []
    financial_impact: Optional[FinancialImpact] = None
    compliance_deadline: Optional[str] = None
    source_link: str

class QualityControl(BaseModel):
    quality_score: float
    quality_grade: str
    safe_to_use: bool
    factual_accuracy: Optional[float] = None

class RegulatoryChange(BaseModel):
    document_id: int
    title: str
    url: str
    source: str
    date: Optional[str]
    scraper_type: Optional[str] = None
    
    facility_types: List[str]
    affected_departments: List[str]
    
    simplified_analysis: SimplifiedAnalysis
    quality_control: Optional[QualityControl] = None
    
    discovered_date: str
    analysis_timestamp: Optional[str] = None

class DashboardStats(BaseModel):
    total_changes: int
    by_facility_type: Dict[str, int]
    by_department: Dict[str, int]
    by_scraper_type: Dict[str, int]
    new_this_week: int
    average_quality_score: Optional[float] = None

# ==============================================================================
# DATA LOADING - Works with Multi-Scraper Pipeline Output
# ==============================================================================

def load_latest_analysis() -> Optional[Dict]:
    """Load the most recent analysis file"""
    
    # Search multiple locations
    search_dirs = [
        Path("."),
        Path("analysis_data/analysis_data"),
        Path("/Users/dharshinikannan/HealthcarePlatform/Coding Web scraper/healthcare_api"),
        Path("/Users/dharshinikannan/HealthcarePlatform/Coding Web scraper"),
    ]
    
    json_files = []
    for search_dir in search_dirs:
        if search_dir.exists():
            files = list(search_dir.glob("enhanced_healthcare_analysis_*.json"))
            if files:
                json_files.extend(files)
                print(f"📂 Found {len(files)} files in {search_dir}")
    
    if not json_files:
        print("⚠️ No analysis files found")
        return None
    
    # Get most recent by date in filename
    def extract_date(filepath):
        match = re.search(r'(\d{8})_(\d{6})', filepath.name)
        if match:
            return int(match.group(1) + match.group(2))
        return 0
    
    latest_file = max(json_files, key=extract_date)
    
    print(f"📂 Loading: {latest_file.name}")
    
    try:
        with open(latest_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def parse_document(doc_data: Dict, idx: int) -> RegulatoryChange:
    """Parse document from multi-scraper pipeline format"""
    
    # Get document info
    doc_info = doc_data.get("document_info", {})
    title = doc_info.get("title", "Untitled")
    url = doc_info.get("url", "")
    date = doc_info.get("date", "")
    source = doc_info.get("source", "CMS")
    scraper_type = doc_info.get("scraper_type", "Unknown")
    
    # Get primary analysis
    primary = doc_data.get("primary_analysis", {})
    
    # Handle error case
    if "error" in primary:
        return RegulatoryChange(
            document_id=idx,
            title=title,
            url=url,
            source=source,
            date=date,
            scraper_type=scraper_type,
            facility_types=["hospitals"],
            affected_departments=["compliance"],
            simplified_analysis=SimplifiedAnalysis(
                detailed_summary="Error analyzing this document",
                who_this_affects=WhoThisAffects(
                    healthcare_roles=["Compliance Officers"],
                    facility_types=["hospitals"],
                    departments=["compliance"]
                ),
                next_steps=[],
                source_link=url
            ),
            discovered_date=datetime.now().strftime("%Y-%m-%d"),
            analysis_timestamp=datetime.now().isoformat()
        )
    
    # Parse analysis
    detailed_summary = primary.get("detailed_summary", "Analysis in progress")
    
    who_affects = primary.get("who_this_affects", {})
    healthcare_roles = who_affects.get("healthcare_roles", ["Compliance Officers"])
    facility_types = who_affects.get("facility_types", ["hospitals"])
    departments = who_affects.get("departments", ["compliance"])
    
    # Next steps
    next_steps_raw = primary.get("next_steps", [])
    next_steps = []
    for step in next_steps_raw[:5]:
        next_steps.append(NextStep(
            action=step.get("action", ""),
            timeline=step.get("timeline", ""),
            owner=step.get("owner", ""),
            priority=step.get("priority", "medium")
        ))
    
    # Financial impact
    fin_impact_raw = primary.get("financial_impact", {})
    financial_impact = None
    if fin_impact_raw:
        financial_impact = FinancialImpact(
            estimated_cost=fin_impact_raw.get("estimated_cost"),
            revenue_impact=fin_impact_raw.get("revenue_impact"),
            areas_affected=fin_impact_raw.get("areas_affected", [])
        )
    
    # Build simplified analysis
    simplified = SimplifiedAnalysis(
        detailed_summary=detailed_summary,
        who_this_affects=WhoThisAffects(
            healthcare_roles=healthcare_roles,
            facility_types=facility_types,
            departments=departments
        ),
        next_steps=next_steps,
        key_requirements=primary.get("key_requirements", []),
        financial_impact=financial_impact,
        compliance_deadline=primary.get("compliance_deadline"),
        source_link=url
    )
    
    # Quality control
    qc_data = doc_data.get("quality_control", {})
    quality_control = None
    if qc_data:
        quality_control = QualityControl(
            quality_score=qc_data.get("quality_score", 0),
            quality_grade=qc_data.get("quality_grade", "Unknown"),
            safe_to_use=qc_data.get("safe_to_use", False),
            factual_accuracy=qc_data.get("factual_accuracy")
        )
    
    metadata = doc_data.get("metadata", {})
    analysis_timestamp = metadata.get("analyzed_at", datetime.now().isoformat())
    
    return RegulatoryChange(
        document_id=idx,
        title=title,
        url=url,
        source=source,
        date=date,
        scraper_type=scraper_type,
        facility_types=facility_types,
        affected_departments=departments,
        simplified_analysis=simplified,
        quality_control=quality_control,
        discovered_date=datetime.now().strftime("%Y-%m-%d"),
        analysis_timestamp=analysis_timestamp
    )

# ==============================================================================
# API ENDPOINTS
# ==============================================================================

@app.get("/")
def root():
    return {
        "status": "healthy",
        "service": "Infervia API v3.1 - Multi-Scraper",
        "version": "3.1.0",
        "features": [
            "Multi-scraper support (newsroom, latest_updates, regulations, transmittals, federal_register)",
            "Comprehensive detailed summaries",
            "3-model AI verification",
            "Quality control scoring",
            "Financial impact analysis"
        ]
    }

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.get("/api/changes/recent", response_model=List[RegulatoryChange])
async def get_recent_changes(limit: int = 20):
    """Get recent regulatory changes from all scraper types"""
    
    data = load_latest_analysis()
    
    if not data:
        raise HTTPException(status_code=404, detail="No analysis data found")
    
    # NEW FORMAT: documents is at top level
    documents = data.get("documents", [])
    
    if not documents:
        print("⚠️ No documents found in analysis file")
        print(f"   Available keys: {list(data.keys())}")
        raise HTTPException(status_code=404, detail="No documents in analysis file")
    
    print(f"✅ Found {len(documents)} documents")
    
    # Parse each document
    changes = []
    for idx, doc in enumerate(documents[:limit]):
        try:
            change = parse_document(doc, idx)
            changes.append(change)
        except Exception as e:
            print(f"⚠️ Error parsing document {idx}: {e}")
            continue
    
    return changes

@app.get("/api/changes/by-facility/{facility_type}")
async def get_changes_by_facility(facility_type: str, limit: int = 10):
    """Filter by facility type"""
    all_changes = await get_recent_changes(limit=100)
    filtered = [
        c for c in all_changes 
        if facility_type.lower() in [ft.lower() for ft in c.facility_types]
    ]
    return filtered[:limit]

@app.get("/api/changes/by-department/{department}")
async def get_changes_by_department(department: str, limit: int = 10):
    """Filter by department"""
    all_changes = await get_recent_changes(limit=100)
    filtered = [
        c for c in all_changes 
        if department.lower() in [d.lower() for d in c.affected_departments]
    ]
    return filtered[:limit]

@app.get("/api/changes/by-scraper/{scraper_type}")
async def get_changes_by_scraper(scraper_type: str, limit: int = 20):
    """Filter by scraper type (newsroom, latest_updates, etc)"""
    all_changes = await get_recent_changes(limit=100)
    filtered = [
        c for c in all_changes 
        if c.scraper_type and scraper_type.lower() in c.scraper_type.lower()
    ]
    return filtered[:limit]

@app.get("/api/stats/dashboard", response_model=DashboardStats)
async def get_dashboard_stats():
    """Dashboard statistics"""
    
    changes = await get_recent_changes(limit=100)
    
    # Count by facility type
    facility_counts = {}
    for change in changes:
        for ft in change.facility_types:
            facility_counts[ft] = facility_counts.get(ft, 0) + 1
    
    # Count by department
    dept_counts = {}
    for change in changes:
        for dept in change.affected_departments:
            dept_counts[dept] = dept_counts.get(dept, 0) + 1
    
    # Count by scraper type
    scraper_counts = {}
    for change in changes:
        if change.scraper_type:
            scraper_counts[change.scraper_type] = scraper_counts.get(change.scraper_type, 0) + 1
    
    # Count new this week
    week_ago = datetime.now().timestamp() - (7 * 24 * 60 * 60)
    new_this_week = sum(
        1 for c in changes 
        if datetime.fromisoformat(c.analysis_timestamp).timestamp() > week_ago
    )
    
    # Average quality
    quality_scores = [
        c.quality_control.quality_score 
        for c in changes 
        if c.quality_control
    ]
    avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else None
    
    return DashboardStats(
        total_changes=len(changes),
        by_facility_type=facility_counts,
        by_department=dept_counts,
        by_scraper_type=scraper_counts,
        new_this_week=new_this_week,
        average_quality_score=round(avg_quality, 2) if avg_quality else None
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)