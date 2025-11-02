from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import httpx
import os
from datetime import datetime

app = FastAPI(title="Infervia API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://infervia-frontend-g935.vercel.app", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Databricks config
DATABRICKS_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

# Models (Pydantic v1 compatible)
class Change(BaseModel):
    change_type: str
    title: str
    url: str
    impact_level: str
    facility_types: str
    safe_for_use: bool
    discovered_date: str
    
    class Config:
        schema_extra = {
            "example": {
                "change_type": "entirely_new",
                "title": "Sample Change",
                "url": "https://example.com",
                "impact_level": "High",
                "facility_types": "hospital",
                "safe_for_use": True,
                "discovered_date": "2025-01-01"
            }
        }

class WeeklySummary(BaseModel):
    total_changes: int
    high_impact_count: int
    new_documents: int
    critical_changes: int

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

# Endpoints
@app.get("/")
def root():
    return {
        "status": "healthy",
        "service": "Infervia API",
        "version": "1.0.0"
    }

@app.get("/api/changes/weekly", response_model=List[Change])
async def get_weekly_changes(limit: int = 20):
    query = f"""
        SELECT 
            change_type, title, url, impact_level,
            facility_types, safe_for_use, discovered_date
        FROM infervia.weekly_changes
        WHERE discovered_date >= date_sub(current_date(), 7)
        ORDER BY discovered_date DESC
        LIMIT {limit}
    """
    
    try:
        rows = await execute_sql(query)
        
        changes = []
        for row in rows:
            changes.append({
                "change_type": row[0] or "unknown",
                "title": row[1] or "Untitled",
                "url": row[2] or "",
                "impact_level": row[3] or "Unknown",
                "facility_types": row[4] or "",
                "safe_for_use": bool(row[5]),
                "discovered_date": str(row[6]) if row[6] else ""
            })
        
        return changes
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/summary/weekly", response_model=WeeklySummary)
async def get_weekly_summary():
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
        rows = await execute_sql(query)
        row = rows[0] if rows else [0, 0, 0, 0]
        
        return {
            "total_changes": row[0] or 0,
            "high_impact_count": row[1] or 0,
            "new_documents": row[2] or 0,
            "critical_changes": row[3] or 0
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/alerts/critical")
async def get_critical_alerts():
    query = """
        SELECT title, url, change_type, impact_level, discovered_date
        FROM infervia.weekly_changes
        WHERE impact_level = 'High'
        AND discovered_date >= date_sub(current_date(), 7)
        ORDER BY discovered_date DESC
        LIMIT 10
    """
    
    try:
        rows = await execute_sql(query)
        
        alerts = []
        for row in rows:
            alerts.append({
                "title": row[0],
                "url": row[1],
                "change_type": row[2],
                "impact_level": row[3],
                "discovered_date": str(row[4])
            })
        
        return {"alerts": alerts, "count": len(alerts)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok"}