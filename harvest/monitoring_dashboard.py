"""
Monitoring dashboard for PixVault.
Provides a web-based dashboard for viewing metrics and statistics.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from .monitoring import (
    get_metrics_collector, get_metrics, get_daily_stats,
    get_metrics_summary, export_metrics_json
)
from .prometheus_metrics import export_prometheus_metrics
from .utils.logger import get_logger

logger = get_logger("harvest.monitoring_dashboard")

# Initialize FastAPI app
app = FastAPI(
    title="PixVault Monitoring Dashboard",
    description="Real-time monitoring and metrics dashboard",
    version="1.0.0"
)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main monitoring dashboard."""
    return templates.TemplateResponse("monitoring_dashboard.html", {"request": request})


@app.get("/api/metrics")
async def get_metrics_api():
    """Get current metrics data."""
    try:
        # Get today's stats
        today = datetime.now().strftime('%Y-%m-%d')
        daily_stats = get_daily_stats(today)
        
        # Get 7-day summary
        summary = get_metrics_summary(days=7)
        
        # Get recent metrics
        recent_metrics = get_metrics(start_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        
        return {
            "timestamp": time.time(),
            "date": today,
            "daily_stats": daily_stats.__dict__,
            "summary": summary,
            "recent_metrics": recent_metrics
        }
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/metrics/{metric_name}")
async def get_metric_data(metric_name: str, days: int = 7):
    """Get specific metric data."""
    try:
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        metrics = get_metrics(
            metric_name=metric_name,
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "metric_name": metric_name,
            "period": f"{start_date} to {end_date}",
            "data": metrics.get(metric_name, [])
        }
        
    except Exception as e:
        logger.error(f"Failed to get metric data: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/daily-stats")
async def get_daily_stats_api(days: int = 7):
    """Get daily statistics for multiple days."""
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Get daily stats for each day
        daily_stats_list = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            stats = get_daily_stats(date_str)
            daily_stats_list.append(stats.__dict__)
            current_date += timedelta(days=1)
        
        return {
            "period": f"{start_date} to {end_date}",
            "daily_stats": daily_stats_list
        }
        
    except Exception as e:
        logger.error(f"Failed to get daily stats: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/export/json")
async def export_json_api():
    """Export metrics as JSON."""
    try:
        json_data = export_metrics_json()
        return JSONResponse(content=json.loads(json_data))
        
    except Exception as e:
        logger.error(f"Failed to export JSON: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/export/prometheus")
async def export_prometheus_api():
    """Export metrics in Prometheus format."""
    try:
        prometheus_data = export_prometheus_metrics()
        return JSONResponse(content={"data": prometheus_data})
        
    except Exception as e:
        logger.error(f"Failed to export Prometheus: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Basic health check
        collector = get_metrics_collector()
        
        return {
            "status": "healthy",
            "timestamp": time.time(),
            "database_path": str(collector.metrics_db_path),
            "storage_path": str(collector.storage_path)
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(status_code=500, content={"status": "unhealthy", "error": str(e)})


def run_dashboard(host: str = "127.0.0.1", port: int = 8001, reload: bool = False):
    """Run the monitoring dashboard server."""
    import uvicorn
    
    uvicorn.run(
        "harvest.monitoring_dashboard:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )


if __name__ == "__main__":
    run_dashboard()
