from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import httpx
import os
import json
from datetime import datetime

app = FastAPI(title="Infervia API", version="2.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Databricks config
DATABRICKS_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

# Pydantic Models
class RegulatoryChange(BaseModel):
    document_id: int
    title: str
    url: str
    source: str
    date: Optional[str]
    change_type: str
    impact_level: str
    facility_types: Optional[str]
    affected_departments: Optional[str]
    requires_action: bool
    safe_for_use: bool
    discovered_date: str
    analysis_timestamp: Optional[str] = None
    change_analysis: Optional[Dict[str, Any]] = None
    clinical_analysis: Optional[Dict[str, Any]] = None
    financial_analysis: Optional[Dict[str, Any]] = None
    compliance_analysis: Optional[Dict[str, Any]] = None
    quality_control: Optional[Dict[str, Any]] = None

class DashboardStats(BaseModel):
    total_changes: int
    high_impact: int
    requires_action: int
    new_this_week: int
    by_source: Dict[str, int]
    by_impact: Dict[str, int]

# Helper function
async def execute_sql(query: str):
    """Execute SQL via Databricks REST API"""
    url = f"https://{DATABRICKS_HOST}/api/2.0/sql/statements"
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "statement": query,
        "warehouse_id": DATABRICKS_WAREHOUSE_ID,
        "wait_timeout": "30s"
    }
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json=payload, headers=headers)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Databricks API error: {response.text}"
            )
        
        result = response.json()
        
        if result.get("status", {}).get("state") == "SUCCEEDED":
            return result.get("result", {}).get("data_array", [])
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Query failed: {result.get('status', {})}"
            )

def safe_json_parse(json_string):
    """Safely parse JSON string, return empty dict if invalid"""
    if not json_string:
        return {}
    try:
        return json.loads(json_string)
    except:
        return {}

# Endpoints
@app.get("/")
def root():
    return {
        "status": "healthy",
        "service": "Infervia API",
        "version": "2.0.0",
        "endpoints": [
            "/api/changes/recent",
            "/api/changes/high-impact",
            "/api/stats/dashboard",
            "/api/changes/{document_id}"
        ]
    }

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.get("/api/changes/recent", response_model=List[RegulatoryChange])
async def get_recent_changes(limit: int = 20, days: int = 30):
    """Get recent regulatory changes with all analysis"""
    query = f"""
        SELECT 
            document_id, title, url, source, date,
            change_type, impact_level, facility_types,
            affected_departments, requires_action, safe_for_use,
            discovered_date, analysis_timestamp,
            change_analysis_json, clinical_analysis_json,
            financial_analysis_json, compliance_analysis_json,
            quality_control_json
        FROM infervia.weekly_changes
        WHERE discovered_date >= date_sub(current_date(), {days})
        ORDER BY discovered_date DESC, document_id DESC
        LIMIT {limit}
    """
    
    try:
        rows = await execute_sql(query)
        
        changes = []
        for row in rows:
            change = {
                "document_id": row[0] or 0,
                "title": row[1] or "Untitled",
                "url": row[2] or "",
                "source": row[3] or "unknown",
                "date": str(row[4]) if row[4] else None,
                "change_type": row[5] or "unknown",
                "impact_level": row[6] or "Unknown",
                "facility_types": row[7],
                "affected_departments": row[8],
                "requires_action": bool(row[9]),
                "safe_for_use": bool(row[10]),
                "discovered_date": str(row[11]) if row[11] else "",
                "analysis_timestamp": str(row[12]) if row[12] else None,
                "change_analysis": safe_json_parse(row[13]),
                "clinical_analysis": safe_json_parse(row[14]),
                "financial_analysis": safe_json_parse(row[15]),
                "compliance_analysis": safe_json_parse(row[16]),
                "quality_control": safe_json_parse(row[17])
            }
            changes.append(change)
        
        return changes
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/changes/high-impact", response_model=List[RegulatoryChange])
async def get_high_impact_changes(limit: int = 10):
    """Get high-impact changes requiring action"""
    query = f"""
        SELECT 
            document_id, title, url, source, date,
            change_type, impact_level, facility_types,
            affected_departments, requires_action, safe_for_use,
            discovered_date, analysis_timestamp,
            change_analysis_json, clinical_analysis_json,
            financial_analysis_json, compliance_analysis_json,
            quality_control_json
        FROM infervia.weekly_changes
        WHERE impact_level = 'High' 
        AND requires_action = true
        ORDER BY discovered_date DESC
        LIMIT {limit}
    """
    
    try:
        rows = await execute_sql(query)
        
        changes = []
        for row in rows:
            change = {
                "document_id": row[0] or 0,
                "title": row[1] or "Untitled",
                "url": row[2] or "",
                "source": row[3] or "unknown",
                "date": str(row[4]) if row[4] else None,
                "change_type": row[5] or "unknown",
                "impact_level": row[6] or "Unknown",
                "facility_types": row[7],
                "affected_departments": row[8],
                "requires_action": bool(row[9]),
                "safe_for_use": bool(row[10]),
                "discovered_date": str(row[11]) if row[11] else "",
                "analysis_timestamp": str(row[12]) if row[12] else None,
                "change_analysis": safe_json_parse(row[13]),
                "clinical_analysis": safe_json_parse(row[14]),
                "financial_analysis": safe_json_parse(row[15]),
                "compliance_analysis": safe_json_parse(row[16]),
                "quality_control": safe_json_parse(row[17])
            }
            changes.append(change)
        
        return changes
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/dashboard", response_model=DashboardStats)
async def get_dashboard_stats():
    """Get dashboard statistics"""
    query = """
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN impact_level = 'High' THEN 1 END) as high_impact,
            COUNT(CASE WHEN requires_action = true THEN 1 END) as requires_action,
            COUNT(CASE WHEN discovered_date >= date_sub(current_date(), 7) THEN 1 END) as new_this_week
        FROM infervia.weekly_changes
    """
    
    by_source_query = """
        SELECT source, COUNT(*) as count
        FROM infervia.weekly_changes
        GROUP BY source
    """
    
    by_impact_query = """
        SELECT impact_level, COUNT(*) as count
        FROM infervia.weekly_changes
        GROUP BY impact_level
    """
    
    try:
        rows = await execute_sql(query)
        main_stats = rows[0] if rows else [0, 0, 0, 0]
        
        source_rows = await execute_sql(by_source_query)
        by_source = {row[0]: row[1] for row in source_rows}
        
        impact_rows = await execute_sql(by_impact_query)
        by_impact = {row[0]: row[1] for row in impact_rows}
        
        return {
            "total_changes": main_stats[0] or 0,
            "high_impact": main_stats[1] or 0,
            "requires_action": main_stats[2] or 0,
            "new_this_week": main_stats[3] or 0,
            "by_source": by_source,
            "by_impact": by_impact
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)