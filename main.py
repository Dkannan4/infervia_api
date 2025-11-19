from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict
import json
from datetime import datetime
from pathlib import Path
import re

app = FastAPI(title="Infervia API with Citations", version="3.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================================================================
# PYDANTIC MODELS WITH CITATIONS
# ==============================================================================

class Citation(BaseModel):
    """Source evidence supporting a claim"""
    claim: str  # What the AI said
    claim_type: str  # summary, requirement, deadline, etc.
    supporting_text: str  # Exact quote from source
    context: Optional[str] = None  # Surrounding text
    page_number: Optional[int] = None
    section: Optional[str] = None
    character_offset: Optional[int] = None
    confidence_score: float
    document_title: str
    document_url: str

class WhoThisAffects(BaseModel):
    healthcare_roles: List[str]
    facility_types: List[str]
    departments: List[str]

class NextStep(BaseModel):
    action: str
    timeline: str
    owner: str
    priority: Optional[str] = "medium"
    # NEW: Citation for this next step
    citation: Optional[Citation] = None

class FinancialImpact(BaseModel):
    estimated_cost: Optional[str] = None
    revenue_impact: Optional[str] = None
    areas_affected: List[str] = []
    # NEW: Citation for financial claims
    citation: Optional[Citation] = None

class SimplifiedAnalysis(BaseModel):
    detailed_summary: str
    who_this_affects: WhoThisAffects
    next_steps: List[NextStep]
    key_requirements: Optional[List[str]] = []
    financial_impact: Optional[FinancialImpact] = None
    compliance_deadline: Optional[str] = None
    source_link: str
    
    # NEW: All citations for this analysis
    source_citations: Optional[List[Citation]] = []
    citation_count: Optional[int] = 0
    grounding_score: Optional[float] = None  # How well claims are supported

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
    average_grounding_score: Optional[float] = None  # NEW

# ==============================================================================
# DATA LOADING
# ==============================================================================

def load_latest_analysis() -> Optional[Dict]:
    """Load most recent analysis file"""
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
    
    if not json_files:
        return None
    
    def extract_date(filepath):
        match = re.search(r'(\d{8})_(\d{6})', filepath.name)
        return int(match.group(1) + match.group(2)) if match else 0
    
    latest_file = max(json_files, key=extract_date)
    print(f"📂 Loading: {latest_file.name}")
    
    with open(latest_file, 'r') as f:
        return json.load(f)

def parse_citation(citation_data: Dict) -> Citation:
    """Parse citation from data"""
    return Citation(
        claim=citation_data.get("claim", ""),
        claim_type=citation_data.get("claim_type", ""),
        supporting_text=citation_data.get("supporting_text", ""),
        context=citation_data.get("context"),
        page_number=citation_data.get("page_number"),
        section=citation_data.get("section"),
        character_offset=citation_data.get("character_offset"),
        confidence_score=citation_data.get("confidence_score", 0),
        document_title=citation_data.get("document_title", ""),
        document_url=citation_data.get("document_url", "")
    )

def parse_document(doc_data: Dict, idx: int) -> RegulatoryChange:
    """Parse document with citations"""
    
    doc_info = doc_data.get("document_info", {})
    primary = doc_data.get("primary_analysis", {})
    
    # Parse basic info
    title = doc_info.get("title", "Untitled")
    url = doc_info.get("url", "")
    
    # Parse citations
    citations_raw = primary.get("source_citations", [])
    citations = [parse_citation(c) for c in citations_raw]
    
    # Parse analysis components
    who_affects = primary.get("who_this_affects", {})
    
    # Next steps with citations
    next_steps = []
    for step in primary.get("next_steps", [])[:5]:
        # Find citation for this step
        step_citation = None
        step_action = step.get("action", "")
        for cit in citations:
            if cit.claim_type == "next_step" and step_action in cit.claim:
                step_citation = cit
                break
        
        next_steps.append(NextStep(
            action=step.get("action", ""),
            timeline=step.get("timeline", ""),
            owner=step.get("owner", ""),
            priority=step.get("priority", "medium"),
            citation=step_citation
        ))
    
    # Financial impact with citation
    fin_raw = primary.get("financial_impact", {})
    financial_impact = None
    if fin_raw:
        fin_citation = None
        for cit in citations:
            if cit.claim_type == "financial_impact":
                fin_citation = cit
                break
        
        financial_impact = FinancialImpact(
            estimated_cost=fin_raw.get("estimated_cost"),
            revenue_impact=fin_raw.get("revenue_impact"),
            areas_affected=fin_raw.get("areas_affected", []),
            citation=fin_citation
        )
    
    # Build analysis
    simplified = SimplifiedAnalysis(
        detailed_summary=primary.get("detailed_summary", ""),
        who_this_affects=WhoThisAffects(
            healthcare_roles=who_affects.get("healthcare_roles", []),
            facility_types=who_affects.get("facility_types", []),
            departments=who_affects.get("departments", [])
        ),
        next_steps=next_steps,
        key_requirements=primary.get("key_requirements", []),
        financial_impact=financial_impact,
        compliance_deadline=primary.get("compliance_deadline"),
        source_link=url,
        source_citations=citations,
        citation_count=len(citations),
        grounding_score=primary.get("grounding_score")
    )
    
    # Quality control
    qc_data = doc_data.get("quality_control", {})
    quality_control = QualityControl(
        quality_score=qc_data.get("quality_score", 0),
        quality_grade=qc_data.get("quality_grade", "Unknown"),
        safe_to_use=qc_data.get("safe_to_use", False),
        factual_accuracy=qc_data.get("factual_accuracy")
    ) if qc_data else None
    
    return RegulatoryChange(
        document_id=idx,
        title=title,
        url=url,
        source=doc_info.get("source", "CMS"),
        date=doc_info.get("date", ""),
        scraper_type=doc_info.get("scraper_type"),
        facility_types=who_affects.get("facility_types", []),
        affected_departments=who_affects.get("departments", []),
        simplified_analysis=simplified,
        quality_control=quality_control,
        discovered_date=datetime.now().strftime("%Y-%m-%d"),
        analysis_timestamp=doc_data.get("metadata", {}).get("analyzed_at", datetime.now().isoformat())
    )

# ==============================================================================
# API ENDPOINTS
# ==============================================================================

@app.get("/")
def root():
    return {
        "status": "healthy",
        "service": "Infervia API with Source Citations",
        "version": "3.2.0",
        "features": [
            "Source evidence extraction",
            "Citation tracking with confidence scores",
            "Page numbers and section references",
            "Grounding score calculation"
        ]
    }

@app.get("/api/changes/recent", response_model=List[RegulatoryChange])
async def get_recent_changes(limit: int = 50):
    """Get recent changes with full citation data"""
    
    data = load_latest_analysis()
    if not data:
        raise HTTPException(status_code=404, detail="No analysis data found")
    
    documents = data.get("documents", [])
    if not documents:
        raise HTTPException(status_code=404, detail="No documents in analysis")
    
    changes = []
    for idx, doc in enumerate(documents[:limit]):
        try:
            change = parse_document(doc, idx)
            changes.append(change)
        except Exception as e:
            print(f"⚠️ Error parsing document {idx}: {e}")
    
    return changes

@app.get("/api/changes/{doc_id}/citations", response_model=List[Citation])
async def get_document_citations(doc_id: int):
    """Get all citations for a specific document"""
    
    changes = await get_recent_changes(limit=100)
    
    if doc_id >= len(changes):
        raise HTTPException(status_code=404, detail="Document not found")
    
    return changes[doc_id].simplified_analysis.source_citations

@app.get("/api/changes/{doc_id}/citations/{claim_type}")
async def get_citations_by_type(doc_id: int, claim_type: str):
    """Get citations filtered by type (summary, requirement, deadline, etc)"""
    
    all_citations = await get_document_citations(doc_id)
    
    filtered = [c for c in all_citations if c.claim_type == claim_type]
    
    return filtered

@app.get("/api/stats/dashboard", response_model=DashboardStats)
async def get_dashboard_stats():
    """Dashboard with grounding scores"""
    
    changes = await get_recent_changes(limit=50)
    
    facility_counts = {}
    dept_counts = {}
    scraper_counts = {}
    
    for change in changes:
        for ft in change.facility_types:
            facility_counts[ft] = facility_counts.get(ft, 0) + 1
        for dept in change.affected_departments:
            dept_counts[dept] = dept_counts.get(dept, 0) + 1
        if change.scraper_type:
            scraper_counts[change.scraper_type] = scraper_counts.get(change.scraper_type, 0) + 1
    
    week_ago = datetime.now().timestamp() - (7 * 24 * 60 * 60)
    new_this_week = sum(
        1 for c in changes 
        if datetime.fromisoformat(c.analysis_timestamp).timestamp() > week_ago
    )
    
    # Average quality and grounding scores
    quality_scores = [c.quality_control.quality_score for c in changes if c.quality_control]
    grounding_scores = [
        c.simplified_analysis.grounding_score 
        for c in changes 
        if c.simplified_analysis.grounding_score
    ]
    
    avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else None
    avg_grounding = sum(grounding_scores) / len(grounding_scores) if grounding_scores else None
    
    return DashboardStats(
        total_changes=len(changes),
        by_facility_type=facility_counts,
        by_department=dept_counts,
        by_scraper_type=scraper_counts,
        new_this_week=new_this_week,
        average_quality_score=round(avg_quality, 2) if avg_quality else None,
        average_grounding_score=round(avg_grounding, 2) if avg_grounding else None
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
