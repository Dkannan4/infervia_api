from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import json
from datetime import datetime
from databricks import sql
import os

app = FastAPI(title="Infervia API", version="1.0.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://infervia-frontend-g935-git-main-prems-projects-bd05301b.vercel.app"],  # Change to your frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Databricks connection config
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def get_db_connection():
    """Create Databricks SQL connection"""
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

# ============================================
# API MODELS
# ============================================

class Change(BaseModel):
    change_type: str
    title: str
    url: str
    impact_level: str
    facility_types: str
    safe_for_use: bool
    discovered_date: str

class ChangeDetail(BaseModel):
    title: str
    url: str
    date: str
    change_type: str
    impact_level: str
    facility_types: str
    affected_departments: str
    change_analysis: dict
    clinical_analysis: dict
    financial_analysis: dict
    compliance_analysis: dict
    safe_for_use: bool

class WeeklySummary(BaseModel):
    total_changes: int
    high_impact_count: int
    new_documents: int
    critical_changes: int
    week_start: str

# ============================================
# ENDPOINTS
# ============================================

@app.get("/")
def root():
    """Health check"""
    return {
        "status": "healthy",
        "service": "Infervia API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/changes/weekly", response_model=List[Change])
def get_weekly_changes(limit: int = 20):
    """Get this week's changes"""
    
    query = f"""
        SELECT 
            change_type,
            title,
            url,
            impact_level,
            facility_types,
            safe_for_use,
            discovered_date
        FROM infervia.weekly_changes
        WHERE discovered_date >= date_sub(current_date(), 7)
        ORDER BY discovered_date DESC
        LIMIT {limit}
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
                changes = []
                for row in results:
                    changes.append({
                        "change_type": row[0],
                        "title": row[1],
                        "url": row[2],
                        "impact_level": row[3],
                        "facility_types": row[4],
                        "safe_for_use": row[5],
                        "discovered_date": row[6]
                    })
                
                return changes
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/changes/{change_id}", response_model=ChangeDetail)
def get_change_detail(change_id: int):
    """Get detailed change analysis"""
    
    query = f"""
        SELECT 
            title, url, date, change_type, impact_level,
            facility_types, affected_departments,
            change_analysis_json,
            clinical_analysis_json,
            financial_analysis_json,
            compliance_analysis_json,
            safe_for_use
        FROM infervia.weekly_changes
        WHERE document_id = {change_id}
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()
                
                if not row:
                    raise HTTPException(status_code=404, detail="Change not found")
                
                return {
                    "title": row[0],
                    "url": row[1],
                    "date": row[2],
                    "change_type": row[3],
                    "impact_level": row[4],
                    "facility_types": row[5],
                    "affected_departments": row[6],
                    "change_analysis": json.loads(row[7]) if row[7] else {},
                    "clinical_analysis": json.loads(row[8]) if row[8] else {},
                    "financial_analysis": json.loads(row[9]) if row[9] else {},
                    "compliance_analysis": json.loads(row[10]) if row[10] else {},
                    "safe_for_use": row[11]
                }
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/summary/weekly", response_model=WeeklySummary)
def get_weekly_summary():
    """Get weekly summary statistics"""
    
    query = """
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN impact_level = 'High' THEN 1 END) as high_impact,
            COUNT(CASE WHEN change_type = 'entirely_new' THEN 1 END) as new_docs,
            COUNT(CASE WHEN change_type IN ('requirements_added', 'deadline_changed') THEN 1 END) as critical
        FROM infervia.weekly_changes
        WHERE discovered_date >= date_sub(current_date(), 7)
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()
                
                return {
                    "total_changes": row[0],
                    "high_impact_count": row[1],
                    "new_documents": row[2],
                    "critical_changes": row[3],
                    "week_start": (datetime.now().date() - datetime.timedelta(days=7)).isoformat()
                }
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/changes/filter")
def filter_changes(
    impact_level: Optional[str] = None,
    facility_type: Optional[str] = None,
    change_type: Optional[str] = None,
    limit: int = 50
):
    """Filter changes by criteria"""
    
    conditions = ["discovered_date >= date_sub(current_date(), 30)"]
    
    if impact_level:
        conditions.append(f"impact_level = '{impact_level}'")
    if facility_type:
        conditions.append(f"facility_types LIKE '%{facility_type}%'")
    if change_type:
        conditions.append(f"change_type = '{change_type}'")
    
    where_clause = " AND ".join(conditions)
    
    query = f"""
        SELECT 
            change_type, title, url, impact_level, 
            facility_types, safe_for_use, discovered_date
        FROM infervia.weekly_changes
        WHERE {where_clause}
        ORDER BY discovered_date DESC
        LIMIT {limit}
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
                changes = []
                for row in results:
                    changes.append({
                        "change_type": row[0],
                        "title": row[1],
                        "url": row[2],
                        "impact_level": row[3],
                        "facility_types": row[4],
                        "safe_for_use": row[5],
                        "discovered_date": row[6]
                    })
                
                return {"changes": changes, "count": len(changes)}
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/alerts/critical")
def get_critical_alerts():
    """Get critical changes requiring immediate attention"""
    
    query = """
        SELECT title, url, change_type, impact_level, discovered_date
        FROM infervia.weekly_changes
        WHERE impact_level = 'High'
        AND change_type IN ('deadline_changed', 'requirements_added', 'entirely_new')
        AND discovered_date >= date_sub(current_date(), 7)
        ORDER BY discovered_date DESC
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
                alerts = []
                for row in results:
                    alerts.append({
                        "title": row[0],
                        "url": row[1],
                        "change_type": row[2],
                        "impact_level": row[3],
                        "discovered_date": row[4],
                        "alert_level": "critical"
                    })
                
                return {"alerts": alerts, "count": len(alerts)}
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
